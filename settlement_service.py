"""
settlement_service.py  —  Financial Settlement Service
────────────────────────────────────────────────────────
Bounded Context: Financial Settlement
Kafka topic consumed: [trade.confirmed]
Kafka topic published: [payment.completed]

Responsibilities:
  • Consume trade.confirmed events to initiate settlement
  • Process micro-transactions (debit buyer, credit seller)
  • Implement Saga pattern with compensating transactions on failure
  • Publish payment.completed events for Notification Service
"""

import data_store as db
import user_service
from models import SettlementRecord, PaymentStatus, KafkaEvent, new_id
from datetime import datetime


# ── Saga: Trade Settlement ────────────────────────────────────────────────────
#
# Happy path (Choreography-based Saga):
#   1. Consume [trade.confirmed]
#   2. Create SettlementRecord (PENDING)
#   3. Debit buyer account
#   4. Credit seller account
#   5. Mark settlement COMPLETED
#   6. Publish [payment.completed]
#
# Compensation (on failure):
#   - Reverse any partial account changes
#   - Mark settlement FAILED
#   - Log for manual reconciliation

def _publish_payment_completed(settlement: SettlementRecord, trade_payload: dict) -> None:
    event = KafkaEvent(
        event_id=new_id(),
        topic="payment.completed",
        payload={
            "event_type": "PaymentProcessed",
            "settlement": settlement.to_dict(),
            "trade_id": trade_payload.get("trade_id"),
            "seller_id": trade_payload.get("seller_id"),
            "buyer_id": trade_payload.get("buyer_id"),
        },
    )
    db.publish(event)


def _compensate(settlement: SettlementRecord, buyer_debited: bool, seller_credited: bool, reason: str) -> None:
    """Compensating transaction: reverse any partial account changes."""
    print(f"  [Settlement]   Saga compensation triggered: {reason}")
    if buyer_debited:
        user_service.credit_account(settlement.buyer_id, settlement.amount)
        print(f"  [Settlement]   Reversed buyer debit (${settlement.amount:.4f})")
    if seller_credited:
        user_service.debit_account(settlement.seller_id, settlement.amount)
        print(f"  [Settlement]   Reversed seller credit (${settlement.amount:.4f})")
    settlement.status = PaymentStatus.FAILED
    print(f"  [Settlement]  Settlement {settlement.settlement_id} marked FAILED")


def process_trade_confirmations() -> int:
    """
    Consume all pending [trade.confirmed] events and run the settlement Saga.
    Returns count of settlements processed.
    """
    events = db.consume("trade.confirmed")
    count = 0
    for event in events:
        payload = event.payload
        _run_settlement_saga(payload)
        count += 1
    return count


def _run_settlement_saga(trade_payload: dict) -> SettlementRecord:
    """Execute the Trade Settlement Saga for one confirmed trade."""
    trade_id = trade_payload["trade_id"]
    seller_id = trade_payload["seller_id"]
    buyer_id = trade_payload["buyer_id"]
    amount = trade_payload["total_amount"]

    print(f"\n  [Settlement]  Starting Saga for trade={trade_id}, amount=${amount:.4f}")

    # Step 1: Create settlement record (idempotency key = trade_id)
    if any(s.trade_id == trade_id for s in db.settlements.values()):
        print(f"  [Settlement]  Duplicate event — settlement for trade {trade_id} already exists, skipping.")
        return

    settlement = SettlementRecord(
        settlement_id=new_id(),
        trade_id=trade_id,
        seller_id=seller_id,
        buyer_id=buyer_id,
        amount=amount,
        status=PaymentStatus.PENDING,
    )
    db.settlements[settlement.settlement_id] = settlement

    buyer_debited = False
    seller_credited = False

    try:
        # Step 2: Debit buyer
        user_service.debit_account(buyer_id, amount)
        buyer_debited = True

        # Step 3: Credit seller
        user_service.credit_account(seller_id, amount)
        seller_credited = True

        # Step 4: Mark complete
        settlement.status = PaymentStatus.COMPLETED
        settlement.completed_at = datetime.now()

        # Step 5: Publish payment.completed
        _publish_payment_completed(settlement, trade_payload)

        print(f"  [Settlement] Settlement {settlement.settlement_id} COMPLETED — ${amount:.4f} transferred")

    except ValueError as e:
        _compensate(settlement, buyer_debited, seller_credited, str(e))

    return settlement


# ── Direct settlement (used when trade data is already in scope) ──────────────

def settle_trade(trade_id: str, seller_id: str, buyer_id: str, amount: float) -> SettlementRecord:
    """Manually initiate a settlement (e.g., for testing individual trades)."""
    payload = {
        "trade_id": trade_id,
        "seller_id": seller_id,
        "buyer_id": buyer_id,
        "total_amount": amount,
    }
    return _run_settlement_saga(payload)


# ── Queries ───────────────────────────────────────────────────────────────────

def list_settlements() -> list:
    return [s.to_dict() for s in db.settlements.values()]


def get_settlement_by_trade(trade_id: str) -> dict:
    for s in db.settlements.values():
        if s.trade_id == trade_id:
            return s.to_dict()
    return None