import random 

from .models import NewLimit, NewMarket, Cancel, Side


def generate_random_events(
    n_events: int,
    *,
    seed: int = 42,
    start_price: float = 100.0,
    tick_size: float = 1.0,
    max_offset: int = 5,
    max_qty: int = 10,
    p_limit: float = 0.70,
    p_market: float = 0.20,
    p_cancel: float = 0.10,
):
    """
    Generate a synthetic stream of order events.

    Event mix is controlled by p_limit / p_market / p_cancel.
    Prices are sampled around `start_price` in discrete ticks.
    """
    rng = random.Random(seed)
    events = []

    next_order_id = 1
    active_orders: dict[int, tuple[Side, float, int]] = {}

    for _ in range(n_events):
        u = rng.random()

        # -------------------------------------------------
        # New limit order
        # -------------------------------------------------
        if u < p_limit:
            side = rng.choice([Side.BUY, Side.SELL])
            offset = rng.randint(-max_offset, max_offset)
            price = start_price + offset * tick_size
            qty = rng.randint(1, max_qty)

            ev = NewLimit(
                price=price,
                side=side,
                qty=qty,
                order_id=next_order_id,
            )
            events.append(ev)

            # Track for possible future cancellation.
            # Note: this does not know whether the order will later cross and
            # rest partially, fully fill immediately, etc. It is only a simple
            # generator-side approximation.
            active_orders[next_order_id] = (side, price, qty)
            next_order_id += 1

        # -------------------------------------------------
        # New market order
        # -------------------------------------------------
        elif u < p_limit + p_market:
            side = rng.choice([Side.BUY, Side.SELL])
            qty = rng.randint(1, max_qty)

            ev = NewMarket(
                order_id=next_order_id,
                side=side,
                qty=qty,
            )
            events.append(ev)
            next_order_id += 1

        # -------------------------------------------------
        # Cancel
        # -------------------------------------------------
        else:
            if active_orders:
                order_id = rng.choice(list(active_orders.keys()))
                ev = Cancel(order_id=order_id)
                events.append(ev)

                # Remove from generator-side pool so we do not repeatedly cancel
                # the same ID in the synthetic stream.
                del active_orders[order_id]
            else:
                # Fallback: if nothing is active, emit a limit order instead
                side = rng.choice([Side.BUY, Side.SELL])
                offset = rng.randint(-max_offset, max_offset)
                price = start_price + offset * tick_size
                qty = rng.randint(1, max_qty)

                ev = NewLimit(
                    price=price,
                    side=side,
                    qty=qty,
                    order_id=next_order_id,
                )
                events.append(ev)
                active_orders[next_order_id] = (side, price, qty)
                next_order_id += 1

    return events

