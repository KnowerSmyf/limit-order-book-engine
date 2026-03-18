from typing import Iterable

from .models import InboundEvent, Trade


def replay_events(book, events: Iterable[InboundEvent]) -> list[Trade]:
    """
    Replay a stream of inbound events through a book and collect all trades.
    """
    all_trades: list[Trade] = []
    for ev in events:
        all_trades.extend(book.process_event(ev))
    return all_trades


def run_and_collect(book_cls, events: Iterable[InboundEvent]):
    """
    Construct a book, replay events through it, and return (book, trades).
    """
    book = book_cls()
    trades = replay_events(book, events)
    return book, trades