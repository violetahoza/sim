from __future__ import annotations
import os
from pathlib import Path


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