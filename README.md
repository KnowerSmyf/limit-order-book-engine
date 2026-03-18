# Order Book

A toy matching engine project for learning how exchanges work under the hood, exploring order book data structures, and building intuition for market microstructure.

## Project goals

This project began as a way to understand the mechanics of modern electronic markets more concretely. The main goals are:

- implement a simple matching engine from scratch
- learn how limit orders, market orders, and cancellations interact
- compare different internal order book data structures empirically
- build a sandbox for later exploration of market microstructure
- develop a small but well-structured systems project with tests, replay utilities, and benchmarking potential

## What is currently implemented

The project currently includes two order book implementations with a shared event-driven API:

### 1. `OrderBook`
A heap-based implementation that stores individual resting orders directly in bid/ask heaps.

Characteristics:
- straightforward initial implementation
- uses lazy cancellation of stale heap entries
- useful baseline for correctness and benchmarking

### 2. `OrderBookPriceLevel`
A price-level implementation that stores:
- `price -> FIFO queue of resting orders`
- heaps of live prices for best-price retrieval
- an order index for O(1)-style cancellation lookup

Characteristics:
- models the book at the level of price queues
- preserves FIFO priority within each level
- supports cleaner reasoning about price-level behavior
- intended as the main comparison point against the original heap-based design

## Supported event types

The engine currently supports:

- new limit orders
- new market orders
- order cancellations

These are represented as immutable event dataclasses and processed through a shared method:

```python
process_event(event)
```

## Matching rules implemented

- price-time priority
- FIFO matching within each price level
- market orders consume available liquidity immediately and do not rest
- unfilled residual of a crossing limit order rests on the book
- cancellation removes a resting order if present

## Testing

The project now includes a pytest-based test suite covering both implementations.

Current tests cover:

- simple crossing trades
- walking the book across multiple price levels
- FIFO behavior at the same price level
- market order partial fills
- liquidity exhaustion
- cancellation behavior
- monotone trade sequencing
- non-crossed-book invariants
- implementation consistency checks

Run tests with:

```bash
pytest -q
```

## Current project structure
    order_book/
        __init__.py
        models.py
        heap_book.py
        price_level_book.py
        replay.py
        synthetic.py

    tests/
        test_order_books.py

## Current utilities

`get_l2()`

Both order books expose an L2-style snapshot method returning aggregated quantity by price level.

`replay.py`

Contains helpers for replaying event streams through an order book implementation and collecting trades.

`synthetic.py`

Contains synthetic event stream generation utilities for testing, differential validation, and future benchmarking.

## Research / exploration intent

This project is not just about implementing an order book mechanically. It is also intended as a sandbox for exploring questions such as:

- what are the tradeoffs between heap-based and price-level representations?
- how do cancellation-heavy vs execution-heavy workloads affect performance?
- when does a price-level structure outperform an order-level heap?
- how sensitive is performance to the ratio of active price levels to total resting orders?
- how can simple matching-engine outputs be turned into microstructure signals?

Longer term, this project may be used to explore:

- L2 and L3 book inspection
- event replay on synthetic or real order-flow-style data
- differential testing between implementations
- timing and memory benchmarking
- market impact and queue-position experiments
- richer trade records and additional order types

## Next Steps 
Planned next steps include:

- expanding synthetic stream generation to support different workload regimes
- adding replay-based differential testing on larger event streams
- benchmarking runtime and memory usage across both implementations
- exploring workload scenarios such as:
    - cancel-heavy flow
    - deep books
    - sparse books
    - many price levels
    - many orders per level
- adding richer snapshot / inspection utilities
- exploring basic microstructure metrics such as spread, mid-price, and imbalance

## Why this project exists

Order books are often described abstractly, but many of their interesting properties only become intuitive when you implement and stress-test them directly. This project is meant to bridge that gap: part systems exercise, part market mechanics sandbox, and part foundation for later microstructure experiments.