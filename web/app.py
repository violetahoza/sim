from __future__ import annotations
import json
import logging
import sys
from pathlib import Path
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

sys.path.insert(0, str(Path(__file__).parent.parent))

from simulator.utils import load_dotenv
from web.state import state, RESULTS_DIR
from web.routes import router

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv()

app = FastAPI(title="Smart Parking IoT Simulator", version="3.0.0")

BASE_DIR = Path(__file__).parent
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

app.include_router(router)

@app.on_event("startup")
async def _startup() -> None:
    if not RESULTS_DIR.exists():
        return
    for path in sorted(RESULTS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime):
        try:
            data = json.loads(path.read_text())
            data.setdefault("source", "historical")
            data.setdefault("latency_samples", [])
            state.results.append(data)
        except Exception as exc:
            logger.warning(f"Could not load {path}: {exc}")
    if state.results:
        logger.info(f"Loaded {len(state.results)} historical result(s) from {RESULTS_DIR}")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

def start() -> None:
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="warning")


if __name__ == "__main__":
    start()