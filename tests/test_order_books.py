import pytest

from order_book import (
    Side,
    NewLimit,
    NewMarket,
    Cancel,
    OrderBook,
    OrderBookPriceLevel,
)


# =========================================================
# Helpers
# =========================================================

BOOK_CLASSES = [OrderBook, OrderBookPriceLevel]


def trade_tuples(trades):
    """Normalise trades for easy equality checks."""
    return [(t.price, t.qty, t.aggressor_side) for t in trades]


def best_bid_price(ob):
    """
    Return the best bid price for either implementation.
    - OrderBook returns a RestingOrder
    - OrderBookPriceLevel returns a price
    """
    b = ob._best_bid()
    if b is None:
        return None
    return b if isinstance(b, (int, float)) else b.price


def best_ask_price(ob):
    """
    Return the best ask price for either implementation.
    - OrderBook returns a RestingOrder
    - OrderBookPriceLevel returns a price
    """
    a = ob._best_ask()
    if a is None:
        return None
    return a if isinstance(a, (int, float)) else a.price


def assert_not_crossed(ob):
    """Assert bid < ask whenever both sides are non-empty."""
    bid = best_bid_price(ob)
    ask = best_ask_price(ob)
    if bid is not None and ask is not None:
        assert bid < ask


def get_resting_order(ob, order_id):
    """
    Access resting order metadata for either implementation.

    Assumes:
    - OrderBookPriceLevel has `resting_orders`
    - OrderBook has `active_orders` only, so exact per-id lookup is unavailable
      unless you later add it.
    """
    if hasattr(ob, "resting_orders"):
        return ob.resting_orders.get(order_id)
    return None


def get_price_level_queue(ob, side, price):
    """
    Return the queue at a price level for OrderBookPriceLevel, else None.
    Useful for asserting FIFO and cancel behavior in the price-level book.
    """
    if not hasattr(ob, "bid_levels"):
        return None

    if side is Side.BUY:
        return ob.bid_levels.get(price)
    elif side is Side.SELL:
        return ob.ask_levels.get(price)
    else:
        raise ValueError("Invalid side")


# =========================================================
# Core behavior tests
# =========================================================

@pytest.mark.parametrize("book_cls", BOOK_CLASSES)
def test_simple_cross_trade_price_and_resting(book_cls):
    """
    Rest one ask. Submit a larger crossing buy limit.
    Should trade at resting price, and the residual buy should rest.
    """
    ob = book_cls()

    ob.process_event(NewLimit(price=101.0, side=Side.SELL, qty=5, order_id=1))
    trades = ob.process_event(NewLimit(price=105.0, side=Side.BUY, qty=7, order_id=2))

    assert trade_tuples(trades) == [(101.0, 5, Side.BUY)]
    assert best_bid_price(ob) == 105.0
    assert best_ask_price(ob) is None
    assert_not_crossed(ob)

    ro = get_resting_order(ob, 2)
    if ro is not None:
        assert ro.price == 105.0
        assert ro.qty == 2
        assert ro.side is Side.BUY


@pytest.mark.parametrize("book_cls", BOOK_CLASSES)
def test_walk_book_multiple_levels(book_cls):
    """
    Incoming order should walk multiple price levels in price order.
    """
    ob = book_cls()

    ob.process_event(NewLimit(price=101.0, side=Side.SELL, qty=5, order_id=10))
    ob.process_event(NewLimit(price=102.0, side=Side.SELL, qty=10, order_id=11))

    trades = ob.process_event(NewLimit(price=105.0, side=Side.BUY, qty=12, order_id=12))

    assert trade_tuples(trades) == [
        (101.0, 5, Side.BUY),
        (102.0, 7, Side.BUY),
    ]

    assert best_ask_price(ob) == 102.0
    assert best_bid_price(ob) is None
    assert_not_crossed(ob)

    ro = get_resting_order(ob, 11)
    if ro is not None:
        assert ro.qty == 3


@pytest.mark.parametrize("book_cls", BOOK_CLASSES)
def test_fifo_same_price_level(book_cls):
    """
    FIFO must hold within a price level.
    """
    ob = book_cls()

    ob.process_event(NewLimit(price=101.0, side=Side.SELL, qty=5, order_id=20))
    ob.process_event(NewLimit(price=101.0, side=Side.SELL, qty=5, order_id=21))

    trades = ob.process_event(NewLimit(price=101.0, side=Side.BUY, qty=7, order_id=22))

    assert trade_tuples(trades) == [
        (101.0, 5, Side.BUY),
        (101.0, 2, Side.BUY),
    ]

    assert best_ask_price(ob) == 101.0
    assert_not_crossed(ob)

    # In the price-level book we can inspect the surviving FIFO state directly.
    level = get_price_level_queue(ob, Side.SELL, 101.0)
    if level is not None:
        assert list(level.keys()) == [21]
        assert level[21].qty == 3


@pytest.mark.parametrize("book_cls", BOOK_CLASSES)
def test_market_order_partial_fill(book_cls):
    """
    Market order partially consumes best resting liquidity.
    """
    ob = book_cls()

    ob.process_event(NewLimit(price=100.0, side=Side.SELL, qty=10, order_id=30))
    trades = ob.process_event(NewMarket(order_id=31, side=Side.BUY, qty=6))

    assert trade_tuples(trades) == [(100.0, 6, Side.BUY)]
    assert best_ask_price(ob) == 100.0
    assert_not_crossed(ob)

    ro = get_resting_order(ob, 30)
    if ro is not None:
        assert ro.qty == 4


@pytest.mark.parametrize("book_cls", BOOK_CLASSES)
def test_market_order_exhausts_liquidity(book_cls):
    """
    Market order should fill available liquidity and discard the remainder.
    """
    ob = book_cls()

    ob.process_event(NewLimit(price=105.0, side=Side.SELL, qty=3, order_id=40))
    trades = ob.process_event(NewMarket(order_id=41, side=Side.BUY, qty=10))

    assert trade_tuples(trades) == [(105.0, 3, Side.BUY)]
    assert best_ask_price(ob) is None
    assert_not_crossed(ob)

    ro = get_resting_order(ob, 40)
    if ro is not None:
        assert ro is None  # unreachable in current price-level impl, kept for symmetry


@pytest.mark.parametrize("book_cls", BOOK_CLASSES)
def test_non_crossing_limit_order_rests(book_cls):
    """
    A non-crossing limit order should simply rest on the book.
    """
    ob = book_cls()

    ob.process_event(NewLimit(price=101.0, side=Side.SELL, qty=5, order_id=50))
    trades = ob.process_event(NewLimit(price=100.0, side=Side.BUY, qty=7, order_id=51))

    assert trades == []
    assert best_bid_price(ob) == 100.0
    assert best_ask_price(ob) == 101.0
    assert_not_crossed(ob)

    ro = get_resting_order(ob, 51)
    if ro is not None:
        assert ro.qty == 7


@pytest.mark.parametrize("book_cls", BOOK_CLASSES)
def test_sell_limit_crosses_resting_bid(book_cls):
    """
    Symmetry check for sell-side crossing behavior.
    """
    ob = book_cls()

    ob.process_event(NewLimit(price=100.0, side=Side.BUY, qty=8, order_id=60))
    trades = ob.process_event(NewLimit(price=99.0, side=Side.SELL, qty=5, order_id=61))

    assert trade_tuples(trades) == [(100.0, 5, Side.SELL)]
    assert best_bid_price(ob) == 100.0
    assert best_ask_price(ob) is None
    assert_not_crossed(ob)

    ro = get_resting_order(ob, 60)
    if ro is not None:
        assert ro.qty == 3


# =========================================================
# Cancellation tests
# =========================================================

@pytest.mark.parametrize("book_cls", BOOK_CLASSES)
def test_cancel_resting_order_removes_it(book_cls):
    """
    Cancelling a resting order should remove it from the book.
    """
    ob = book_cls()

    ob.process_event(NewLimit(price=99.0, side=Side.BUY, qty=4, order_id=70))
    ob.process_event(Cancel(order_id=70))

    assert best_bid_price(ob) is None
    assert_not_crossed(ob)

    ro = get_resting_order(ob, 70)
    if ro is not None:
        assert ro is None  # kept only for interface symmetry


@pytest.mark.parametrize("book_cls", BOOK_CLASSES)
def test_cancel_unknown_order_is_noop(book_cls):
    """
    Cancelling an unknown order should not crash and should not change state.
    """
    ob = book_cls()

    ob.process_event(NewLimit(price=101.0, side=Side.SELL, qty=5, order_id=80))
    ob.process_event(Cancel(order_id=999999))

    assert best_ask_price(ob) == 101.0
    assert best_bid_price(ob) is None
    assert_not_crossed(ob)


@pytest.mark.parametrize("book_cls", BOOK_CLASSES)
def test_cancel_one_of_two_same_price_orders(book_cls):
    """
    Cancelling one order at a level should leave the other intact.
    """
    ob = book_cls()

    ob.process_event(NewLimit(price=99.0, side=Side.BUY, qty=4, order_id=90))
    ob.process_event(NewLimit(price=99.0, side=Side.BUY, qty=9, order_id=91))
    ob.process_event(Cancel(order_id=90))

    assert best_bid_price(ob) == 99.0
    assert_not_crossed(ob)

    level = get_price_level_queue(ob, Side.BUY, 99.0)
    if level is not None:
        assert list(level.keys()) == [91]
        assert level[91].qty == 9


# =========================================================
# Sequence / invariants tests
# =========================================================

@pytest.mark.parametrize("book_cls", BOOK_CLASSES)
def test_trade_sequence_monotone(book_cls):
    """
    Trade sequence numbers should increase monotonically.
    """
    ob = book_cls()

    ob.process_event(NewLimit(price=101.0, side=Side.SELL, qty=5, order_id=100))
    ob.process_event(NewLimit(price=102.0, side=Side.SELL, qty=5, order_id=101))

    trades = ob.process_event(NewMarket(order_id=102, side=Side.BUY, qty=9))
    seqs = [t.seq for t in trades]

    assert seqs == sorted(seqs)
    assert len(set(seqs)) == len(seqs)


@pytest.mark.parametrize("book_cls", BOOK_CLASSES)
def test_book_not_crossed_after_mixed_sequence(book_cls):
    """
    Mixed event sequence should preserve the non-crossed-book invariant.
    """
    ob = book_cls()

    events = [
        NewLimit(price=100.0, side=Side.BUY, qty=5, order_id=110),
        NewLimit(price=101.0, side=Side.SELL, qty=7, order_id=111),
        NewLimit(price=99.0, side=Side.SELL, qty=2, order_id=112),   # crosses bid
        Cancel(order_id=110),
        NewLimit(price=103.0, side=Side.SELL, qty=3, order_id=113),
        NewMarket(order_id=114, side=Side.BUY, qty=4),
    ]

    for ev in events:
        ob.process_event(ev)
        assert_not_crossed(ob)


# =========================================================
# Price-level-specific internal tests
# =========================================================

def test_price_level_book_removes_empty_level_and_marks_price_inactive():
    """
    More specific internal test for OrderBookPriceLevel:
    when a level is depleted, it should disappear from the level map and
    active-price set. Heap cleanup is allowed to be lazy.
    """
    ob = OrderBookPriceLevel()

    ob.process_event(NewLimit(price=100.0, side=Side.SELL, qty=5, order_id=200))
    assert 100.0 in ob.ask_levels
    assert 100.0 in ob.active_ask_prices

    ob.process_event(NewMarket(order_id=201, side=Side.BUY, qty=5))

    assert 100.0 not in ob.ask_levels
    assert 100.0 not in ob.active_ask_prices
    assert ob._best_ask() is None


def test_price_level_book_fifo_queue_order_visible():
    """
    Internal FIFO visibility test for OrderBookPriceLevel.
    """
    ob = OrderBookPriceLevel()

    ob.process_event(NewLimit(price=101.0, side=Side.SELL, qty=1, order_id=300))
    ob.process_event(NewLimit(price=101.0, side=Side.SELL, qty=1, order_id=301))
    ob.process_event(NewLimit(price=101.0, side=Side.SELL, qty=1, order_id=302))

    level = ob.ask_levels[101.0]
    assert list(level.keys()) == [300, 301, 302]