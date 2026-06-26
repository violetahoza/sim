from __future__ import annotations
import os
from pathlib import Path
import json
import msgpack
from simulator.models.models import BatchUpdate, ParkingEvent, SpotState


def read_env_file() -> dict[str, str]:
    env_path = Path(__file__).parent.parent / ".env"
    result: dict[str, str] = {}
    if not env_path.exists():
        return result
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            result[k.strip()] = v.strip()
    return result


def load_dotenv() -> None:
    for k, v in read_env_file().items():
        os.environ.setdefault(k, v)


def get_groq_api_key() -> str:
    load_dotenv()
    return os.environ.get("AI_API_KEY", "").strip()

def deserialize_batch(payload: bytes) -> BatchUpdate:
    obj = json.loads(payload.decode())
    events = []
    for e in obj.get("events", []):
        events.append(ParkingEvent(
            sensor_id=e["sensor_id"],
            spot_id=int(e["spot_id"]),
            state=SpotState(e["state"]),
            timestamp=float(e.get("timestamp", 0.0)),
            sequence=int(e.get("sequence", 0)),
            is_initial=bool(e.get("is_initial", False)),
            is_heartbeat_event=bool(e.get("is_heartbeat_event", False))
        ))
    return BatchUpdate(edge_id=obj.get("edge_id", "edge"), events=events)

def encode_event(event: ParkingEvent) -> bytes:
    return msgpack.packb(event.to_dict(), use_bin_type=True)


def encode_batch(batch: BatchUpdate) -> bytes:
    return msgpack.packb(batch.to_dict(), use_bin_type=True)
