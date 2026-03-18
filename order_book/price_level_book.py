from collections import OrderedDict
import heapq
import itertools
from typing import List, Set, Tuple, Dict

from .models import RestingOrder, Side, Trade, NewLimit, Cancel, NewMarket, InboundEvent


class OrderBookPriceLevel:
    """
    Price-level order book.

    Data structures:
    - bid_levels / ask_levels:
        Map price -> FIFO queue of resting orders at that price.
    - ordered_bids / ordered_asks:
        Heaps of prices used to retrieve the best live price quickly.
        Empty/stale heap entries are removed lazily.
    - active_bid_prices / active_ask_prices:
        Track which prices currently have non-empty queues.
    - resting_orders:
        Map order_id -> RestingOrder for O(1) cancellation lookup.

    Invariant:
    A price is considered live iff it appears in the relevant active_* set
    and its corresponding level queue is non-empty.
    """
    def __init__(self):
        # Hash mapping price -> FIFO queue of RestingOrders keyed by order_id
        self.bid_levels: dict[float, OrderedDict[int, RestingOrder]] = {}
        self.ask_levels: dict[float, OrderedDict[int, RestingOrder]] = {}

        # Priority queues (heap-based) keeping the prices ordered 
        self.ordered_bids: List[float] = []
        self.ordered_asks: List[float] = []
        
        # Tracks which prices are active. Used for lazy-deletion of heap items
        self.active_bid_prices: Set[float] = set()
        self.active_ask_prices: Set[float] = set()

        # Mapping order_ids to RestingOrder objects
        self.resting_orders: dict[int, RestingOrder] = {}

        # Presumably these are needed for tracking. Not sure yet
        self._trade_seq = itertools.count()
        self._time_seq = itertools.count()

    def _peek_price_level(self, level: OrderedDict[int, RestingOrder]) -> Tuple[int, RestingOrder]:
        """Return the oldest resting order in a **non-empty** price level."""
        return next(iter(level.items()))

    def _best_bid(self):
        while self.ordered_bids:
            top = self.ordered_bids[0]
            if top in self.active_bid_prices:
                return top
            heapq.heappop_max(self.ordered_bids) # stale heap entry
        return None

    def _best_ask(self):
        while self.ordered_asks:
            top = self.ordered_asks[0]
            if top in self.active_ask_prices:
                return top
            heapq.heappop(self.ordered_asks) # stale heap entry
        return None

    def _fill(self, *, aggressor_side: Side, aggressor_order_id: int, resting: RestingOrder, desired_qty: int) -> Tuple[int, Trade]:
        """
        Fill up to `desired_qty` against a single resting order.

        This method mutates `resting.qty` in place and returns:
        - the filled quantity
        - the resulting Trade record

        It does not remove depleted orders from the book; callers are responsible
        for any cleanup if `resting.qty` reaches zero.
        """
        if desired_qty <= 0:
            raise ValueError("desired_qty must be positive")
        
        filled = min(desired_qty, resting.qty)
        resting.qty -= filled
        
        trade = Trade(
            price=resting.price,
            seq=next(self._trade_seq),
            aggressor_side=aggressor_side,
            qty=filled,
        )
        return filled, trade
    
    def _rest(self, *, side: Side, price: float, qty: int, order_id: int) -> None:
        """
        Add a new resting order to the book.

        If the price level does not yet exist, create it, register the price in the
        appropriate heap, and mark the price as active.
        """
        t = next(self._time_seq)
        ro = RestingOrder(
            price=price,
            time=t,
            side=side,
            qty=qty,
            order_id=order_id,
        )

        self.resting_orders[order_id] = ro

        if side is Side.BUY:
            if price not in self.bid_levels:
                self.bid_levels[price] = OrderedDict()
                heapq.heappush_max(self.ordered_bids, price)   # add live price to bid heap
                self.active_bid_prices.add(price)              # mark price as live
            self.bid_levels[price][order_id] = ro              # append to FIFO

        elif side is Side.SELL:
            if price not in self.ask_levels:
                self.ask_levels[price] = OrderedDict()
                heapq.heappush(self.ordered_asks, price)
                self.active_ask_prices.add(price)
            self.ask_levels[price][order_id] = ro

        else:
            raise ValueError("Invalid side")

    def _remove_filled_resting_order(self, resting: RestingOrder) -> None:
        """
        Remove a resting order that has been fully filled.

        Preconditions:
        - `resting` is currently live in the book
        - `resting` is present in `resting_orders`
        - `resting` is present in its corresponding price level
        """
        del self.resting_orders[resting.order_id]

        if resting.side is Side.BUY:
            level = self.bid_levels[resting.price]
            del level[resting.order_id]
            if len(level) == 0:
                del self.bid_levels[resting.price]
                self.active_bid_prices.discard(resting.price)

        elif resting.side is Side.SELL:
            level = self.ask_levels[resting.price]
            del level[resting.order_id]
            if len(level) == 0:
                del self.ask_levels[resting.price]
                self.active_ask_prices.discard(resting.price)

        else:
            raise ValueError("Invalid side")
        
    def _cancel_order(self, ev: Cancel) -> None:
        """
        Cancel a resting order by order_id.

        Steps:
        1. Look up the resting order in O(1) via `resting_orders`.
        2. Remove it from the corresponding price-level FIFO queue.
        3. If that queue becomes empty, remove the level from the level map
        and mark the price inactive. The heap entry is cleaned up lazily.
        """
        order = self.resting_orders.pop(ev.order_id, None)
        if order is None:
            # Unknown / already-filled / already-cancelled order: no-op.
            return
        
        if order.side is Side.BUY:
            order_queue = self.bid_levels.get(order.price)
            if order_queue is None:
                raise KeyError("Active BUY order exists, but corresponding bid level is missing.")

            del order_queue[order.order_id]

            if len(order_queue) == 0:
                del self.bid_levels[order.price]
                self.active_bid_prices.discard(order.price)

        elif order.side is Side.SELL:
            order_queue = self.ask_levels.get(order.price)
            if order_queue is None:
                raise KeyError("Active SELL order exists, but corresponding ask level is missing.")

            del order_queue[order.order_id]

            if len(order_queue) == 0:
                del self.ask_levels[order.price]
                self.active_ask_prices.discard(order.price)

        else:
            raise ValueError("Invalid side")

    def _process_limit_order(self, limit_order: NewLimit) -> List[Trade]:
        trades: List[Trade] = []
        order_qty = limit_order.qty

        if limit_order.side is Side.BUY:
            while order_qty > 0:
                best_ask_price = self._best_ask()
                if (best_ask_price is None) or (limit_order.price < best_ask_price):
                    break

                # Match against the oldest resting order at the best ask price
                best_ask_orders = self.ask_levels[best_ask_price]
                matched_order_id, matched_order = self._peek_price_level(best_ask_orders)

                filled, trade = self._fill(
                    aggressor_side=limit_order.side,
                    aggressor_order_id=limit_order.order_id,
                    resting=matched_order,
                    desired_qty=order_qty,
                )
                order_qty -= filled
                trades.append(trade)

                if matched_order.qty == 0:
                    self._remove_filled_resting_order(matched_order)

        elif limit_order.side is Side.SELL:
            while order_qty > 0:
                best_bid_price = self._best_bid()
                if (best_bid_price is None) or (limit_order.price > best_bid_price):
                    break

                # Match against the oldest resting order at the best bid price
                best_bid_orders = self.bid_levels[best_bid_price]
                _, matched_order = self._peek_price_level(best_bid_orders)

                filled, trade = self._fill(
                    aggressor_side=limit_order.side,
                    aggressor_order_id=limit_order.order_id,
                    resting=matched_order,
                    desired_qty=order_qty,
                )
                order_qty -= filled
                trades.append(trade)

                if matched_order.qty == 0:
                    self._remove_filled_resting_order(matched_order)

        else:
            raise ValueError("Invalid side")

        if order_qty > 0:
            self._rest(
                side=limit_order.side,
                price=limit_order.price,
                qty=order_qty,
                order_id=limit_order.order_id,
            )

        return trades

    def _process_market_order(self, market_order: NewMarket) -> List[Trade]:
        """
        Execute an incoming market order against the best available resting liquidity.

        Market orders do not rest on the book. If available liquidity is exhausted
        before the order is fully filled, the remainder is discarded.
        """
        trades: List[Trade] = []
        order_qty = market_order.qty

        if market_order.side is Side.BUY:
            while order_qty > 0:
                best_ask_price = self._best_ask()
                if best_ask_price is None:
                    break

                # Match against the oldest resting order at the best ask price
                best_ask_orders = self.ask_levels[best_ask_price]
                matched_order_id, matched_order = self._peek_price_level(best_ask_orders)

                filled, trade = self._fill(
                    aggressor_side=market_order.side,
                    aggressor_order_id=market_order.order_id,
                    resting=matched_order,
                    desired_qty=order_qty,
                )
                order_qty -= filled
                trades.append(trade)

                if matched_order.qty == 0:
                    self._remove_filled_resting_order(matched_order)

        elif market_order.side is Side.SELL:
            while order_qty > 0:
                best_bid_price = self._best_bid()
                if best_bid_price is None:
                    break

                # Match against the oldest resting order at the best bid price
                best_bid_orders = self.bid_levels[best_bid_price]
                matched_order_id, matched_order = self._peek_price_level(best_bid_orders)

                filled, trade = self._fill(
                    aggressor_side=market_order.side,
                    aggressor_order_id=market_order.order_id,
                    resting=matched_order,
                    desired_qty=order_qty,
                )
                order_qty -= filled
                trades.append(trade)

                if matched_order.qty == 0:
                    self._remove_filled_resting_order(matched_order)

        else:
            raise ValueError("Invalid side")

        return trades

    def process_event(self, ev: InboundEvent) -> List[Trade]:
        if isinstance(ev, NewLimit):
            return self._process_limit_order(ev)
        elif isinstance(ev, NewMarket):
            return self._process_market_order(ev)
        elif isinstance(ev, Cancel):
            self._cancel_order(ev)
            return []
        else:
            raise TypeError(f"Unknown event: {type(ev)!r}") 

    def get_l2(self, depth: int | None = None) -> Dict[str, List[Tuple[float, int]]]:
        """
        Return a level-2 snapshot of the order book. 

        Output format:
        {
            "bids": [(price, total_qty), ...]
            "asks": [(price, total_qty), ...]
        }

        Prices are ordered best-to-worst on each side.
        If `depth` is provided, truncate to that many levels per side.
        """
        bid_prices = sorted(self.bid_levels.keys(), reverse=True)
        ask_prices = sorted(self.ask_levels.keys())

        if depth is not None:
            bid_prices = bid_prices[:depth]
            ask_prices = ask_prices[:depth]

        bids = [
            (price, sum(order.qty for order in self.bid_levels[price].values()))
            for price in bid_prices
        ]
        asks = [
            (price, sum(order.qty for order in self.ask_levels[price].values()))
            for price in ask_prices
        ]

        return {"bids": bids, "asks": asks}