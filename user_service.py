"""
user_service.py  —  User Management Service
--------------------------------------------
Bounded Context: User Management
Kafka topic published: [user.events]

Responsibilities:
  • Register buyers and sellers
  • Manage account balances (credit / debit)
  • Publish user.events to Kafka for downstream services
"""

import data_store as db
from models import User, UserRole, KafkaEvent, new_id


# -- Internal helpers ------------------------------------

def _publish_user_event(event_type: str, user: User) -> None:
    event = KafkaEvent(
        event_id=new_id(),
        topic="user.events",
        payload={"event_type": event_type, **user.to_dict()},
    )
    db.publish(event)


# -- Public API ------------------------------------------------

def register_user(name: str, email: str, role: str, initial_balance: float = 0.0) -> User:
    """Register a new participant and publish a UserRegistered event."""
    user = User(
        user_id=new_id(),
        name=name,
        email=email,
        role=UserRole[role.upper()],
        account_balance=initial_balance,
    )
    db.users[user.user_id] = user
    _publish_user_event("UserRegistered", user)
    print(f"  [UserSvc] Registered {role} '{name}' (id={user.user_id}, balance=${initial_balance:.2f})")
    return user


def get_user(user_id: str) -> User:
    user = db.users.get(user_id)
    if not user:
        raise ValueError(f"User not found: {user_id}")
    return user


def credit_account(user_id: str, amount: float) -> User:
    """Add funds to a user's account (e.g., seller receives payment)."""
    user = get_user(user_id)
    user.account_balance = round(user.account_balance + amount, 4)
    _publish_user_event("AccountCredited", user)
    print(f"  [UserSvc] Credited ${amount:.4f} to {user.name} → balance=${user.account_balance:.4f}")
    return user


def debit_account(user_id: str, amount: float) -> User:
    """Deduct funds from a user's account (e.g., buyer pays for energy)."""
    user = get_user(user_id)
    if user.account_balance < amount:
        raise ValueError(
            f"Insufficient balance for {user.name}: "
            f"has ${user.account_balance:.4f}, needs ${amount:.4f}"
        )
    user.account_balance = round(user.account_balance - amount, 4)
    _publish_user_event("AccountDebited", user)
    print(f"  [UserSvc] Debited ${amount:.4f} from {user.name} → balance=${user.account_balance:.4f}")
    return user


def list_users() -> list:
    return [u.to_dict() for u in db.users.values()]