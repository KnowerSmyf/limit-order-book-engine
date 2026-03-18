from dataclasses import dataclass, field
from typing import Union
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
