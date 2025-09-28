# utils/kafka_io.py
from __future__ import annotations
import os
from typing import Optional

def get_producer() -> Optional["KafkaProducer"]:
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not servers:
        return None
    try:
        from kafka import KafkaProducer
        return KafkaProducer(
            bootstrap_servers=[s.strip() for s in servers.split(",") if s.strip()],
            value_serializer=lambda v: v if isinstance(v, (bytes, bytearray)) else v,
            linger_ms=50, retries=3
        )
    except Exception:
        return None

def get_consumer(topic: str, group_id: str="mass_device") -> Optional["KafkaConsumer"]:
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not servers:
        return None
    try:
        from kafka import KafkaConsumer
        return KafkaConsumer(
            topic,
            bootstrap_servers=[s.strip() for s in servers.split(",") if s.strip()],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=group_id,
            consumer_timeout_ms=1000
        )
    except Exception:
        return None