from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional
import uuid


class TradeStatus(Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"


class PaymentStatus(Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class UserRole(Enum):
    SELLER = "SELLER"
    BUYER = "BUYER"
    ADMIN = "ADMIN"


@dataclass
class User:
    user_id: str
    name: str
    email: str
    role: UserRole
    account_balance: float = 0.0
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "name": self.name,
            "email": self.email,
            "role": self.role.value,
            "account_balance": self.account_balance,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class MeterReading:
    reading_id: str
    device_id: str
    user_id: str
    kwh_available: float          # energy available to sell
    kwh_consumed: float           # energy consumed
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self):
        return {
            "reading_id": self.reading_id,
            "device_id": self.device_id,
            "user_id": self.user_id,
            "kwh_available": self.kwh_available,
            "kwh_consumed": self.kwh_consumed,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class EnergyOffer:
    offer_id: str
    seller_id: str
    seller_name:str
    kwh_amount: float
    price_per_kwh: float          # in dollars
    status: TradeStatus = TradeStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)

    @property
    def total_value(self):
        return round(self.kwh_amount * self.price_per_kwh, 4)

    def to_dict(self):
        return {
            "offer_id": self.offer_id,
            "seller_id": self.seller_id,
            "seller_name": self.seller_name,
            "kwh_amount": self.kwh_amount,
            "price_per_kwh": self.price_per_kwh,
            "total_value": self.total_value,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class TradeBid:
    bid_id: str
    offer_id: str
    buyer_id: str
    buyer_name:str
    kwh_requested: float
    max_price_per_kwh: float
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self):
        return {
            "bid_id": self.bid_id,
            "offer_id": self.offer_id,
            "buyer_id": self.buyer_id,
            "buyer_name": self.buyer_name,
            "kwh_requested": self.kwh_requested,
            "max_price_per_kwh": self.max_price_per_kwh,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class MatchedTrade:
    trade_id: str
    offer_id: str
    bid_id: str
    seller_id: str
    seller_name:str
    buyer_id: str
    buyer_name:str
    kwh_traded: float
    price_per_kwh: float
    status: TradeStatus = TradeStatus.CONFIRMED
    matched_at: datetime = field(default_factory=datetime.now)

    @property
    def total_amount(self):
        return round(self.kwh_traded * self.price_per_kwh, 4)

    def to_dict(self):
        return {
            "trade_id": self.trade_id,
            "offer_id": self.offer_id,
            "bid_id": self.bid_id,
            "seller_id": self.seller_id,
            "buyer_id": self.buyer_id,
            "kwh_traded": self.kwh_traded,
            "price_per_kwh": self.price_per_kwh,
            "total_amount": self.total_amount,
            "status": self.status.value,
            "matched_at": self.matched_at.isoformat(),
        }


@dataclass
class UnmatchedTrade:
    trade_id: str
    offer_id: str
    bid_id: str
    seller_id: str
    buyer_id: str
    kwh_traded: float
    price_per_kwh: float
    status: TradeStatus = TradeStatus.CANCELLED
    unmatched_at: datetime = field(default_factory=datetime.now)

    @property
    def total_amount(self):
        return round(self.kwh_traded * self.price_per_kwh, 4)

    def to_dict(self):
        return {
            "trade_id": self.trade_id,
            "offer_id": self.offer_id,
            "bid_id": self.bid_id,
            "seller_id": self.seller_id,
            "buyer_id": self.buyer_id,
            "kwh_traded": self.kwh_traded,
            "price_per_kwh": self.price_per_kwh,
            "total_amount": self.total_amount,
            "status": self.status.value,
            "unmatched_at": self.unmatched_at.isoformat(),
        }


@dataclass
class SettlementRecord:
    settlement_id: str
    trade_id: str
    seller_id: str
    buyer_id: str
    amount: float
    status: PaymentStatus = PaymentStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None

    def to_dict(self):
        return {
            "settlement_id": self.settlement_id,
            "trade_id": self.trade_id,
            "seller_id": self.seller_id,
            "buyer_id": self.buyer_id,
            "amount": self.amount,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


@dataclass
class TradeUnmatchedRecord:
    trade_id: str
    seller_id: str
    seller_name:str
    bid_id:str
    offer_id:str
    buyer_id: str
    buyer_name:str
    kwh_requested: float
    price_per_kwh: float
    status:TradeStatus=TradeStatus.CANCELLED
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self):
        return {
            "trade_id": self.trade_id,
            "seller_id": self.seller_id,
            "seller_name": self.seller_name,
            "bid_id": self.bid_id,
            "offer_id": self.offer_id,
            "buyer_id": self.buyer_id,
            "buyer_name": self.buyer_name,
            "kwh_requested": self.kwh_requested,
            "price_per_kwh": self.price_per_kwh,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class KafkaEvent:
    event_id: str
    topic: str
    payload: dict
    published_at: datetime = field(default_factory=datetime.now)

    def to_dict(self):
        return {
            "event_id": self.event_id,
            "topic": self.topic,
            "payload": self.payload,
            "published_at": self.published_at.isoformat(),
        }


def new_id() -> str:
    return str(uuid.uuid4())[:8].upper()