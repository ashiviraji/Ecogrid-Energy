"""
marketplace_service.py  —  Marketplace Service
────────────────────────────────────────────────
Bounded Context: Marketplace
Kafka topics consumed: [meter.readings], [user.events]
Kafka topic published: [trade.confirmed]

Responsibilities:
  • Post energy offers (sellers listing available kWh at a price)
  • Place bids (buyers requesting energy at a max price)
  • Match offers and bids — execute trades
  • Consume meter.readings via ACL to update available supply
  • Publish trade.confirmed events downstream (Settlement, Notification)
"""

import csv
import os
import data_store as db
import user_service
from models import (
    EnergyOffer, TradeBid, MatchedTrade,
    TradeStatus, KafkaEvent, TradeUnmatchedRecord, UnmatchedTrade, new_id
)


# ── ACL: Consume meter.readings ───────────────────────────────────────────────
# The Marketplace never uses raw IoT fields (device_id, sensor watt-hours).
# It only reads the ACL-translated energy_availability payload.
CSV_FILEUnmatched = "Unmatched_trade_details.csv"
CSV_HEADERSUnmatched = [
    "trade_id",
    "seller_id",
    "seller_name",
    "buyer_id",
    "buyer_name",
    "kwh_requested",
    "price_per_kwh",
    "status",
    "created_at",
]
def process_meter_readings() -> int:
    """
    Consume pending [meter.readings] events.
    Uses the ACL-translated EnergyAvailability payload — not raw IoT data.
    Returns count of events processed.
    """
    events = db.consume("meter.readings")
    for event in events:
        ea = event.payload.get("energy_availability", {})
        print(
            f"  [Marketplace/ACL]  EnergyAvailability — "
            f"seller={ea.get('seller_id')}, {ea.get('kwh_available')}kWh available"
        )
    return len(events)


def process_user_events() -> int:
    """Consume pending [user.events] to stay aware of participant state."""
    events = db.consume("user.events")
    for event in events:
        etype = event.payload.get("event_type")
        name = event.payload.get("name")
        print(f"  [Marketplace]  User event received — {etype}: {name}")
    return len(events)


# ── Offer Management ──────────────────────────────────────────────────────────

def post_offer(seller_id: str, kwh_amount: float, price_per_kwh: float) -> EnergyOffer:
    """Seller posts an energy offer to the marketplace."""
    seller = user_service.get_user(seller_id)
    #data validation check
    if seller.role.value not in ("SELLER", "ADMIN"):
        raise ValueError(f"User {seller.name} is not a seller")
    if kwh_amount <= 0:
        raise ValueError("kWh amount must be positive")
    if price_per_kwh <= 0:
        raise ValueError("Price per kWh must be positive")

    offer = EnergyOffer(
        offer_id=new_id(),
        seller_id=seller_id,
        seller_name=seller.name,
        kwh_amount=kwh_amount,
        price_per_kwh=price_per_kwh,
    )
    db.energy_offers[offer.offer_id] = offer
    print(
        f"  [Marketplace] Offer posted — seller={seller.name}, "
        f"{kwh_amount}kWh @ ${price_per_kwh}/kWh (total=${offer.total_value:.4f})"
    )
    return offer


def place_bid(buyer_id: str, offer_id: str, kwh_requested: float, max_price_per_kwh: float) -> TradeBid:
    """Buyer places a bid on an existing offer."""
    buyer = user_service.get_user(buyer_id)
    if buyer.role.value not in ("BUYER", "ADMIN"):
        raise ValueError(f"User {buyer.name} is not a buyer")

    offer = db.energy_offers.get(offer_id)
    if not offer:
        raise ValueError(f"Offer not found: {offer_id}")
    if offer.status != TradeStatus.PENDING:
        raise ValueError(f"Offer {offer_id} is no longer available")
    if kwh_requested > offer.kwh_amount:
        raise ValueError(
            f"Requested {kwh_requested}kWh exceeds offer of {offer.kwh_amount}kWh"
        )

    bid = TradeBid(
        bid_id=new_id(),
        offer_id=offer_id,
        buyer_id=buyer_id,
        buyer_name=buyer.name,
        kwh_requested=kwh_requested,
        max_price_per_kwh=max_price_per_kwh,
    )
    db.trade_bids[bid.bid_id] = bid
    print(
        f"  [Marketplace]  Bid placed — buyer={buyer.name}, "
        f"{kwh_requested}kWh, max=${max_price_per_kwh}/kWh"
    )
    return bid


# ── Trade Matching Engine ─────────────────────────────────────────────────────

def match_trade(bid_id: str) -> MatchedTrade:
    """
    Attempt to match a bid against its target offer.
    Matching rule: buyer's max_price >= offer's price_per_kwh.
    On success, publishes [trade.confirmed] to Kafka.
    """
    bid = db.trade_bids.get(bid_id)
    if not bid:
        raise ValueError(f"Bid not found: {bid_id}")

    offer = db.energy_offers.get(bid.offer_id)
    if not offer or offer.status != TradeStatus.PENDING:
        raise ValueError(f"Offer {bid.offer_id} is unavailable for matching")

    # Matching logic: buyer must accept seller's price
    if bid.max_price_per_kwh < offer.price_per_kwh:
        offer.status=TradeStatus.CANCELLED
        unmatched_trade=TradeUnmatchedRecord(
            trade_id=new_id(),
            offer_id=offer.offer_id,
            bid_id=bid.bid_id,
            seller_id=offer.seller_id,
            seller_name=offer.seller_name,
            buyer_id=bid.buyer_id,
            buyer_name=bid.buyer_name,
            kwh_requested=bid.kwh_requested,
            price_per_kwh=offer.price_per_kwh,
            status=TradeStatus.CANCELLED,
        )
        
        print("Dave's bid did not meet the offer price — trade unmatched and offer cancelled.")
        _write_to_csv_unmatchTrade(unmatched_trade)
        raise ValueError(
            f"Bid price ${bid.max_price_per_kwh}/kWh is below offer ${offer.price_per_kwh}/kWh — no match"
        )

    # Execute trade at the offer's price
    trade = MatchedTrade(
        trade_id=new_id(),
        offer_id=offer.offer_id,
        bid_id=bid.bid_id,
        seller_id=offer.seller_id,
        seller_name=offer.seller_name,
        buyer_name=bid.buyer_name,
        buyer_id=bid.buyer_id,
        kwh_traded=bid.kwh_requested,
        price_per_kwh=offer.price_per_kwh,
        status=TradeStatus.CONFIRMED,
    )
    
  
    db.matched_trades[trade.trade_id]=trade

    # Mark offer as confirmed (sold)
    offer.status = TradeStatus.CONFIRMED

    # Publish trade.confirmed → consumed by Settlement & Notification
    event = KafkaEvent(
        event_id=new_id(),
        topic="trade.confirmed",
        payload={"event_type": "TradeMatched", **trade.to_dict()},
    )
    db.publish(event)

    seller = user_service.get_user(offer.seller_id)
    buyer = user_service.get_user(bid.buyer_id)
    print(
        f"  [Marketplace] Trade MATCHED — {trade.kwh_traded}kWh, "
        f"seller={seller.name} → buyer={buyer.name}, "
        f"total=${trade.total_amount:.4f}"
    )
    return trade


def _write_to_csv_unmatchTrade(unmatched_trade: TradeUnmatchedRecord) -> None:
    """
    Append a Unmatched Trade record to the CSV file.
    Creates the file with headers automatically if it doesn't exist yet.
    """
    print("Failed write to csv for unmatched trade")
    file_exists = os.path.isfile(CSV_FILEUnmatched)
    with open(CSV_FILEUnmatched, mode="a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERSUnmatched)
        if not file_exists:
            writer.writeheader()
        writer.writerow(_build_row_unmatched(unmatched_trade))
    print(f"  [Unmatched Trade] Written to CSV → {CSV_FILEUnmatched}")

def _build_row_unmatched(unmatched_trade: TradeUnmatchedRecord) -> dict:
    """Build a CSV row dict from a unmatched trade record, including seller/buyer names."""
    return {
        "trade_id":      unmatched_trade.trade_id,
        "seller_id":     unmatched_trade.seller_id,
        "seller_name":   unmatched_trade.seller_name,
        "buyer_id":      unmatched_trade.buyer_id,
        "buyer_name":    unmatched_trade.buyer_name,
        "kwh_requested": unmatched_trade.kwh_requested,
        "price_per_kwh": f"{unmatched_trade.price_per_kwh:.4f}",
        "status":        unmatched_trade.status.value,
        "created_at":    unmatched_trade.created_at.isoformat(),
    }

# ── Queries ───────────────────────────────────────────────────────────────────

def list_open_offers() -> list:
    return [o.to_dict() for o in db.energy_offers.values() if o.status == TradeStatus.PENDING]


def list_all_trades() -> list:
    return [t.to_dict() for t in db.matched_trades.values()]