"""
main.py  —  EcoGrid Energy Platform Demo
─────────────────────────────────────────
Runs a full end-to-end simulation of the EcoGrid P2P energy trading platform.

Flow:
  1.  Register users (sellers & buyers) via User Management Service
  2.  Register IoT smart meter devices
  3.  Ingest smart meter readings (publishes to [meter.readings])
  4.  Marketplace consumes meter readings via ACL
  5.  Sellers post energy offers to the Marketplace
  6.  Buyers place bids
  7.  Marketplace matches trades (publishes to [trade.confirmed])
  8.  Settlement Service runs Saga (publishes to [payment.completed])
  9.  Notification Service consumes remaining events & sends alerts
  10. Print final system state summary
"""

import user_service
import meter_service
import marketplace_service
import settlement_service
import data_store as db
from models import new_id


# ── Notification Service (inline — pure event consumer) ───────────────────────

def run_notification_service():
    """
    Notification Service: consume [trade.confirmed] and [payment.completed].
    Pure downstream consumer — no writes to Kafka, no domain state.
    """
    trade_events = db.consume("trade.confirmed")
    payment_events = db.consume("payment.completed")

    for event in trade_events:
        p = event.payload
        print(
            f"  [Notification]  Trade alert sent to seller={p['seller_id']} "
            f"and buyer={p['buyer_id']} — trade {p['trade_id']} confirmed"
        )
        db.notifications.append({"type": "trade.confirmed", **p})

    for event in payment_events:
        p = event.payload
        s = p.get("settlement", {})
        print(
            f"  [Notification]  Payment alert sent to buyer={p['buyer_id']} "
            f"and seller={p['seller_id']} — ${s.get('amount', 0):.4f} settled"
        )
        db.notifications.append({"type": "payment.completed", **p})


# ── Pretty printer ────────────────────────────────────────────────────────────

def section(title: str):
    width = 60
    print(f"\n{'═' * width}")
    print(f"  {title}")
    print(f"{'═' * width}")


def subsection(title: str):
    print(f"\n  ── {title} {'─' * (50 - len(title))}")


def print_dict_list(items: list):
    if not items:
        print("    (none)")
        return
    for item in items:
        for k, v in item.items():
            if v is not None:
                print(f"    {k:<25} {v}")
        print()


# ── Main demo ─────────────────────────────────────────────────────────────────

def main():
    print("\n" + "*" * 60)
    print("  EcoGrid Energy — P2P Renewable Energy Trading Platform")
    print("  End-to-End System Demo")
    print("*" * 60)

    # ─────────────────────────────────────────────────────────────
    section("STEP 1 — Register Participants (User Management Service)")
    # ─────────────────────────────────────────────────────────────

    print("\n  Registering sellers (solar panel owners)...")
    alice = user_service.register_user("Alice",  "alice@ecogrid.io",  "SELLER", initial_balance=0.0)
    bob   = user_service.register_user("Bob",    "bob@ecogrid.io",    "SELLER", initial_balance=0.0)

    print("\n  Registering buyers (energy consumers)...")
    carol = user_service.register_user("Carol",  "carol@ecogrid.io",  "BUYER",  initial_balance=50.0)
    dave  = user_service.register_user("Dave",   "dave@ecogrid.io",   "BUYER",  initial_balance=30.0)

    # ─────────────────────────────────────────────────────────────
    section("STEP 2 — Register IoT Smart Meter Devices")
    # ─────────────────────────────────────────────────────────────

    meter_service.register_device("METER-A1", alice.user_id)
    meter_service.register_device("METER-B2", bob.user_id)

    # ─────────────────────────────────────────────────────────────
    section("STEP 3 — Ingest Smart Meter Readings (Smart Meter Service → Kafka [meter.readings])")
    # ─────────────────────────────────────────────────────────────

    print()
    meter_service.ingest_reading("METER-A1", kwh_available=5.2, kwh_consumed=1.8)
    meter_service.ingest_reading("METER-B2", kwh_available=3.0, kwh_consumed=2.5)

    # ─────────────────────────────────────────────────────────────
    section("STEP 4 — Marketplace Consumes Meter Readings via ACL")
    # ─────────────────────────────────────────────────────────────

    print()
    processed = marketplace_service.process_meter_readings()
    print(f"  [Marketplace] Processed {processed} meter reading event(s)")

    # Also drain user events from registration
    marketplace_service.process_user_events()

    # ─────────────────────────────────────────────────────────────
    section("STEP 5 — Sellers Post Energy Offers")
    # ─────────────────────────────────────────────────────────────

    print()
    offer_a = marketplace_service.post_offer(alice.user_id, kwh_amount=5.0, price_per_kwh=0.25)
    offer_b = marketplace_service.post_offer(bob.user_id,   kwh_amount=2.5, price_per_kwh=0.20)

    # ─────────────────────────────────────────────────────────────
    section("STEP 6 — Buyers Place Bids")
    # ─────────────────────────────────────────────────────────────

    print()
    # Carol bids on Alice's offer — willing to pay up to $0.30/kWh (above ask → will match)
    bid_c = marketplace_service.place_bid(carol.user_id, offer_a.offer_id,
                                          kwh_requested=5.0, max_price_per_kwh=0.30)

    # Dave bids on Bob's offer — willing to pay up to $0.18/kWh (below ask → will NOT match)
    bid_d = marketplace_service.place_bid(dave.user_id, offer_b.offer_id,
                                          kwh_requested=2.0, max_price_per_kwh=0.18)

    # ─────────────────────────────────────────────────────────────
    section("STEP 7 — Marketplace Matches Trades → Kafka [trade.confirmed]")
    # ─────────────────────────────────────────────────────────────

    print()
    # Match Carol's bid — should succeed
    try:
        trade1 = marketplace_service.match_trade(bid_c.bid_id)
    except ValueError as e:
        print(f"  [Marketplace]  Match failed: {e}")

    # Match Dave's bid — should fail (price too low)
    try:
        trade2 = marketplace_service.match_trade(bid_d.bid_id)
    except ValueError as e:
        print(f"  [Marketplace]  Match failed (expected): {e}")

    # ─────────────────────────────────────────────────────────────
    section("STEP 8 — Settlement Service Runs Saga → Kafka [payment.completed]")
    # ─────────────────────────────────────────────────────────────

    print()
    count = settlement_service.process_trade_confirmations()
    print(f"\n  [Settlement] Processed {count} trade confirmation(s)")

    # ─────────────────────────────────────────────────────────────
    section("STEP 9 — Notification Service Sends Alerts")
    # ─────────────────────────────────────────────────────────────

    print()
    run_notification_service()

    # ─────────────────────────────────────────────────────────────
    section("STEP 10 — Final System State Summary")
    # ─────────────────────────────────────────────────────────────

    subsection("Users & Balances")
    print_dict_list(user_service.list_users())

    subsection("Meter Readings")
    print_dict_list(meter_service.get_all_readings())

    subsection("Open Offers (still available)")
    open_offers = marketplace_service.list_open_offers()
    if open_offers:
        print_dict_list(open_offers)
    else:
        print("    (no open offers remaining)")

    subsection("Matched Trades")
    print_dict_list(marketplace_service.list_all_trades())

    subsection("Settlement Records")
    print_dict_list(settlement_service.list_settlements())

    subsection("Notifications Sent")
    if db.notifications:
        for n in db.notifications:
            print(f"    [{n['type']}] trade={n.get('trade_id', '—')}")
    else:
        print("    (none)")

    subsection("Kafka Topic Status (pending messages)")
    for topic, count in db.topic_summary().items():
        status = "empty" if count == 0 else f"⚠️  {count} unconsumed"
        print(f"    [{topic}]  {status}")

    print(f"\n{'*' * 60}")
    print("  EcoGrid Demo Complete")
    print(f"{'*' * 60}\n")


if __name__ == "__main__":
    main()