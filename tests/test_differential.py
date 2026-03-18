import pytest

from order_book import OrderBook, OrderBookPriceLevel
from order_book.replay import run_and_collect
from order_book.synthetic import generate_random_events


def normalise_trades(trades):
    """
    Convert trades into a comparison-friendly representation.
    """
    return [(t.price, t.qty, t.aggressor_side) for t in trades]


@pytest.mark.parametrize("seed", [1, 7, 42, 123, 999])
def test_differential_random_stream(seed):
    """
    Replay the same random event stream through both implementations and
    verify that they produce the same trades and final L2 snapshot.
    """
    events = generate_random_events(
        n_events=500,
        seed=seed,
    )

    book_heap, trades_heap = run_and_collect(OrderBook, events)
    book_price_level, trades_price_level = run_and_collect(OrderBookPriceLevel, events)

    assert normalise_trades(trades_heap) == normalise_trades(trades_price_level)
    assert book_heap.get_l2(depth=10) == book_price_level.get_l2(depth=10)


def test_differential_random_stream_longer_run():
    """
    A slightly longer random stream for extra stress.
    """
    events = generate_random_events(
        n_events=5000,
        seed=2025,
    )

    book_heap, trades_heap = run_and_collect(OrderBook, events)
    book_price_level, trades_price_level = run_and_collect(OrderBookPriceLevel, events)

    assert normalise_trades(trades_heap) == normalise_trades(trades_price_level)
    assert book_heap.get_l2(depth=20) == book_price_level.get_l2(depth=20)