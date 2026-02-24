from dataclasses import dataclass, field
import heapq
import itertools
from typing import List, Set, Tuple, Union
from enum import Enum

class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class RestingOrder:
    price: float 
    time: int = field(compare=False)
    side: Side = field(compare=False)
    qty: int = field(compare=False)
    order_id: int = field(compare=False)


@dataclass(frozen=True)
class NewLimit:
    price: float 
    side: Side
    qty: int
    order_id: int


@dataclass(frozen=True)
class NewMarket: 
    order_id: int
    side: Side
    qty: int


@dataclass(frozen=True)
class Cancel:
    order_id: int

    
InboundEvent = Union[NewLimit, NewMarket, Cancel]


@dataclass(frozen=True)
class Trade:
    """
    Represents an executed transaction between two counterparties.

    Unlike an Order (which expresses unilateral intent to buy or sell),
    a Trade is a bilateral event produced by the matching engine when
    an incoming order interacts with resting liquidity.

    Trades are immutable and represent historical fact.
    """
    price: float 
    seq: int
    aggressor_side: Side
    qty: int


class OrderBook:
    def __init__(self):
        # Resting bids and asks are stored as (priority_key, order_object) items in min-heaps
        self.bids: List[Tuple[Tuple[float, int], RestingOrder]] = []
        self.asks: List[Tuple[Tuple[float, int], RestingOrder]] = []

        self.active_orders: Set[int] = set()    # Tracks which orders are active. Used for lazy-cancellation of heap items
        self._trade_seq = itertools.count()
        self._time_seq = itertools.count()

    def _best_bid(self):
        while self.bids:
            _, top = self.bids[0]
            if top.order_id in self.active_orders:
                return top
            # cancelled
            heapq.heappop(self.bids)
        return None

    def _best_ask(self):
        while self.asks:
            _, top = self.asks[0]
            if top.order_id in self.active_orders:
                return top
            # cancelled
            heapq.heappop(self.asks)
        return None
    
    def _fill(self, *, aggressor_side: Side, aggressor_order_id: int, resting: RestingOrder, desired_qty: int) -> Tuple[int, Trade]:
        """
        Fill up to `desired_qty` against a single resting order.
        Mutates `resting.qty`. Returns `(filled_qty, Trade)`.
        """
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
        t = next(self._time_seq)
        ro = RestingOrder(
            price=price,
            time=t,
            side=side,
            qty=qty,
            order_id=order_id,
        )

        # Bid priority is reverse ordered (i.e., larger bids = better)
        if side is Side.BUY:
            key = (-price, t)
            heapq.heappush(self.bids, (key, ro))
        else:
            key = (price, t)
            heapq.heappush(self.asks, (key, ro))

        self.active_orders.add(order_id)


    def _cancel_order(self, ev: Cancel) -> None:
        self.active_orders.discard(ev.order_id)

    def _process_limit_order(self, limit_order: NewLimit) -> List[Trade]:
        trades: List[Trade] = []
        order_qty = limit_order.qty
        if limit_order.side is Side.BUY:
            while True:
                best_ask = self._best_ask()
                if (best_ask is None) or (limit_order.price < best_ask.price) or (order_qty == 0):
                    break

                # Execute the trade at best market prices
                filled, trade = self._fill(
                    aggressor_side=limit_order.side,
                    aggressor_order_id=limit_order.order_id,
                    resting=best_ask,
                    desired_qty=order_qty
                )
                order_qty -= filled
                trades.append(trade)

                # Ask is filled, remove from order book
                if best_ask.qty == 0:
                    heapq.heappop(self.asks)
                    self.active_orders.discard(best_ask.order_id)
            
        elif limit_order.side is Side.SELL:
            while True:
                best_bid = self._best_bid()
                if (best_bid is None) or (limit_order.price > best_bid.price) or (order_qty == 0):
                    break

                # Execute the trade at best market prices
                filled, trade = self._fill(
                    aggressor_side=limit_order.side,
                    aggressor_order_id=limit_order.order_id,
                    resting=best_bid,
                    desired_qty=order_qty
                )
                order_qty -= filled
                trades.append(trade)

                # Bid is filled, remove from order book
                if best_bid.qty == 0:
                    heapq.heappop(self.bids)
                    self.active_orders.discard(best_bid.order_id)

        else:
            raise ValueError("Invalid side")
        
        # If the order is partially filled, it rests
        if order_qty > 0:
            self._rest(
                side=limit_order.side,
                price=limit_order.price,
                qty=order_qty,
                order_id=limit_order.order_id
            )

        return trades

    def _process_market_order(self, market_order: NewMarket) -> List[Trade]:
        """
        Carries out an incoming market order. The order is filled immediately at the best available price. \
            If supply runs out, the remainder of the order is cancelled.

        The idea is that market orders demand liquidity, they don't provide it. 
        """
        trades: List[Trade] = []
        order_qty = market_order.qty

        if market_order.side is Side.BUY:
            while True:
                best_ask = self._best_ask()
                # Stopping condition: No available supply OR order is filled
                if (best_ask is None) or (order_qty == 0):
                    break
                
                # Execute the trade at best market prices
                units_filled, trade = self._fill(
                    aggressor_side=market_order.side,
                    aggressor_order_id=market_order.order_id,
                    resting=best_ask,
                    desired_qty=order_qty,
                )
                order_qty -= units_filled
                trades.append(trade)

                # Ask is filled, remove it from the order book
                if best_ask.qty == 0:
                    heapq.heappop(self.asks)
                    self.active_orders.discard(best_ask.order_id)

        elif market_order.side is Side.SELL:
            while True:
                # Stopping condition: No available supply OR order is filled
                best_bid = self._best_bid()
                if (best_bid is None) or (order_qty == 0):
                    break
                
                # Execute the trade at best market prices
                units_filled, trade = self._fill(
                    aggressor_side=market_order.side,
                    aggressor_order_id=market_order.order_id,
                    resting=best_bid,
                    desired_qty=order_qty,
                )
                order_qty -= units_filled
                trades.append(trade)

                # Bid is filled, remove it from the order book
                if best_bid.qty == 0:
                    heapq.heappop(self.bids)
                    self.active_orders.discard(best_bid.order_id)
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
