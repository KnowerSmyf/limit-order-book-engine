from dataclasses import dataclass, field
import heapq
import itertools
from typing import List

BUY = 1
SELL = -1

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
    side: int = field(compare=False)
    qty: int = field(compare=False)

    @staticmethod
    def limit(side: int, price: float, qty: int) -> "Order":
        oid = next(_id_counter)
        return Order(time=oid, side=side, price=price, qty=qty)


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
    aggressor_side: int
    qty: int


class OrderBook:
    def __init__(self):
        self.bids: List[Order] = []  # max-heap sorted by price, then time (requires Python 3.14+)
        self.asks: List[Order] = []  # min-heap sorted by price, then time
        self._trade_seq = itertools.count()
        
    def add_limit_order(self, order: Order) -> List[Trade]:
        trades: List[Trade] = []
        if order.side == BUY:
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

                if best_ask.qty == 0:
                    heapq.heappop(self.asks)

            if order.qty > 0:
                heapq.heappush_max(self.bids, order)
            
        elif order.side == SELL:
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


                if best_bid.qty == 0:
                    heapq.heappop_max(self.bids)

            if order.qty > 0:
                heapq.heappush(self.asks, order)
        else:
            raise ValueError("Invalid side")
        
        return trades

    def _best_bid(self):
        return self.bids[0] if self.bids else None

    def _best_ask(self):
        return self.asks[0] if self.asks else None
    