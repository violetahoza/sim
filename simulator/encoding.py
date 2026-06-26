"""Canonical wire-format encoder for the simulator.

Every payload is serialised exactly once to msgpack.  All byte-count
metrics are derived from ``len()`` of the resulting buffer — no
multiplicative fudge factors.
"""
from __future__ import annotations

import msgpack

from simulator.models.models import BatchUpdate, ParkingEvent


def encode_event(event: ParkingEvent) -> bytes:
    return msgpack.packb(event.to_dict(), use_bin_type=True)


def encode_batch(batch: BatchUpdate) -> bytes:
    return msgpack.packb(batch.to_dict(), use_bin_type=True)
