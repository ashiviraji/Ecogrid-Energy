"""
meter_service.py  —  Smart Meter Service
----------------------------------------
Bounded Context: Smart Meter / IoT Data Ingestion
Kafka topic published: [meter.readings]

Responsibilities:
  • Ingest real-time energy generation & consumption readings from IoT devices
  • Validate readings (non-negative values, known device)
  • Publish meter.readings events to Kafka (consumed by Marketplace via ACL)
  • Store time-series data (simulated in-memory)
"""

import data_store as db
from models import MeterReading, KafkaEvent, new_id


# ── Device registry (simulates IoT device registration) ──────────────────────
# Maps device_id → user_id (which household owns this meter)
_device_registry: dict = {}


def register_device(device_id: str, user_id: str) -> None:
    """Register an IoT smart meter to a user account."""
    _device_registry[device_id] = user_id
    print(f"  [MeterSvc] Device '{device_id}' registered to user {user_id}")


# -- Anti - Corruption Layer (ACL) ------------------------------------
# Translates raw IoT MeterReading into the Marketplace's domain language:
# EnergyAvailability { seller_id, kwh_available, price_hint }

def _to_energy_availability(reading: MeterReading) -> dict:
    """
    ACL: Translate raw meter data into Marketplace domain concepts.
    The Marketplace never sees device_id or raw watt-hour sensor values.
    """
    return {
        "seller_id": reading.user_id,
        "kwh_available": reading.kwh_available,
        "source_reading_id": reading.reading_id,
        "timestamp": reading.timestamp.isoformat(),
    }


# -- Public API -----------------------------------------------------------

def ingest_reading(device_id: str, kwh_available: float, kwh_consumed: float) -> MeterReading:
    """
    Ingest a meter reading from an IoT device.
    Validates the data, stores it, and publishes to Kafka [meter.readings].
    """
    #Data validation part
    if device_id not in _device_registry:
        raise ValueError(f"Unregistered device: {device_id}")
    if kwh_available < 0 or kwh_consumed < 0:
        raise ValueError("Energy values must be non-negative")

    user_id = _device_registry[device_id]
    reading = MeterReading(
        reading_id=new_id(),
        device_id=device_id,
        user_id=user_id,
        kwh_available=kwh_available,
        kwh_consumed=kwh_consumed,
    )
    db.meter_readings[reading.reading_id] = reading

    # Publish to Kafka using ACL-translated payload
    event = KafkaEvent(
        event_id=new_id(),
        topic="meter.readings",
        payload={
            "event_type": "MeterReadingReceived",
            "raw_reading": reading.to_dict(),             # stored internally
            "energy_availability": _to_energy_availability(reading),  # ACL output for Marketplace
        },
    )
    db.publish(event)
    print(
        f"  [MeterSvc] ⚡ Reading ingested — device={device_id}, "
        f"available={kwh_available}kWh, consumed={kwh_consumed}kWh"
    )
    return reading


def get_readings_for_user(user_id: str) -> list:
    return [r.to_dict() for r in db.meter_readings.values() if r.user_id == user_id]


def get_all_readings() -> list:
    return [r.to_dict() for r in db.meter_readings.values()]