"""
data_store.py
─────────────
Simulates the persistence layer and Kafka message broker in memory.

In a real deployment:
  - Each service would have its own database (PostgreSQL / TimescaleDB).
  - Kafka would be a real cluster (Apache Kafka / Confluent Cloud).

Here we use plain Python dicts and lists so the project runs with zero
external dependencies.
"""

from typing import Dict, List
from models import (
    User, MeterReading, EnergyOffer, TradeBid,
    MatchedTrade, UnmatchedTrade, SettlementRecord, KafkaEvent
)

# ── Kafka topic queues (simulated) ───────────────────────────────────────────
# Topics match the Context Map:
#   meter.readings   → Smart Meter Service → Marketplace (via ACL)
#   user.events      → User Management     → Marketplace
#   trade.confirmed  → Marketplace         → Settlement, Notification
#   payment.completed→ Settlement          → Notification

TOPICS = {
    "meter.readings": [],
    "user.events": [],
    "trade.confirmed": [],
    "payment.completed": [],
}

# ── Per-service "databases" ──────────────────────────────────────────────────

# User Management Service DB
users: Dict[str, User] = {}

# Smart Meter Service DB
meter_readings: Dict[str, MeterReading] = {}

# Marketplace Service DB
energy_offers: Dict[str, EnergyOffer] = {}
trade_bids: Dict[str, TradeBid] = {}
matched_trades: Dict[str, MatchedTrade] = {}
unmatched_trades:Dict[str,UnmatchedTrade] ={}

# Financial Settlement Service DB
settlements: Dict[str, SettlementRecord] = {}

# Notification Service log (append-only)
notifications: List[dict] = []


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def publish(event: KafkaEvent) -> None:
    """Publish an event to a Kafka topic queue."""
    if event.topic not in TOPICS:
        raise ValueError(f"Unknown topic: {event.topic}")
    TOPICS[event.topic].append(event)
    print(f"  [Kafka] ▶  Published to [{event.topic}] — event_id={event.event_id}")


def consume(topic: str) -> List[KafkaEvent]:
    """Consume and drain all pending events from a topic."""
    if topic not in TOPICS:
        raise ValueError(f"Unknown topic: {topic}")
    events = list(TOPICS[topic])
    TOPICS[topic].clear()
    return events


def peek(topic: str) -> List[KafkaEvent]:
    """Read events without consuming (for display purposes)."""
    return list(TOPICS.get(topic, []))


def topic_summary() -> dict:
    return {t: len(msgs) for t, msgs in TOPICS.items()}