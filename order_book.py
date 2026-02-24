from dataclasses import dataclass, field
import heapq
import itertools
from typing import List, Set
from enum import Enum

class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"

_id_counter = itertools.count()

@dataclass(order=True)
class Order:
    """
    Represents a limit order submitted to the exchange.

    An Order expresses unilateral intent to transact at a specified
    price and quantity. Orders may rest in the book, be partially filled,
    fully executed, or cancelled.
    """
    # heap uses the first fields for ordering
    price: float 
    time: int
    side: Side = field(compare=False)
    qty: int = field(compare=False)
    order_id: int = field(compare=False)

    @staticmethod
    def limit(side: Side, price: float, qty: int, order_id: int) -> "Order":
        oid = next(_id_counter)
        return Order(time=oid, side=side, price=price, qty=qty, order_id=order_id)


@dataclass(frozen=True)
class Cancel:
    order_id: int

    
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
        self.bids: List[Order] = []  # max-heap sorted by price, then time (requires Python 3.14+)
        self.asks: List[Order] = []  # min-heap sorted by price, then time
        self.active_orders: Set[int] = set()    # Tracks which orders are active. Used for lazy-cancellation of heap items
        self._trade_seq = itertools.count()
        
    def add_limit_order(self, order: Order) -> List[Trade]:
        trades: List[Trade] = []
        if order.side is Side.BUY:
            while True:
                best_ask = self._best_ask()
                if (best_ask is None) or (order.price < best_ask.price) or (order.qty == 0):
                    break

                units_exchanged = min(order.qty, best_ask.qty)

                order.qty -= units_exchanged
                best_ask.qty -= units_exchanged
                trades.append(
                    Trade(
                        price=best_ask.price, 
                        seq=next(self._trade_seq),
                        aggressor_side=order.side,
                        qty=units_exchanged
                    )
                )
                # Ask is filled, remove it from the order book
                if best_ask.qty == 0:
                    heapq.heappop(self.asks)
                    self.active_orders.discard(best_ask.order_id)

            if order.qty > 0:
                heapq.heappush_max(self.bids, order)
            
        elif order.side is Side.SELL:
            while True:
                best_bid = self._best_bid()
                if (best_bid is None) or (order.price > best_bid.price) or (order.qty == 0):
                    break

                units_exchanged = min(order.qty, best_bid.qty)

                order.qty -= units_exchanged
                best_bid.qty -= units_exchanged
                trades.append(
                    Trade(
                        price=best_bid.price, 
                        seq=next(self._trade_seq),
                        aggressor_side=order.side,
                        qty=units_exchanged
                    )
                )
                # Bid is filled, remove it from the order book
                if best_bid.qty == 0:
                    heapq.heappop_max(self.bids)
                    self.active_orders.discard(best_bid.order_id)

            if order.qty > 0:
                heapq.heappush(self.asks, order)
        else:
            raise ValueError("Invalid side")
        
        return trades

    def _cancel_order(self, ev: Cancel) -> None:
        self.active_orders.discard(ev.order_id)

    def _best_bid(self):
        while self.bids:
            top = self.bids[0]
            if top.order_id in self.active_orders:
                return top
            # cancelled
            heapq.heappop_max(self.bids)
        return None

    def _best_ask(self):
        while self.asks:
            top = self.asks[0]
            if top.order_id in self.active_orders:
                return top
            # cancelled
            heapq.heappop(self.asks)
        return None