# api_ws.py — FastAPI wrapper that runs the orchestrator in-process (no subprocess),
# preloads datasets, and streams real-time flow events via the orchestrator logger.

import os
import json
import uuid
import time
import asyncio
import contextlib
import logging
from typing import Any, Dict, Optional, List, Tuple, Union

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ---- Import orchestrator + dataset utils from your main.py ----
from main import (
    graph,
    load_alarms_csv,
    nodes_from_alarms,
    load_performance_csv_local,
    load_inventory_csv_local,
    load_customers_csv_local,
    enhance_customer_df,
    _union_nodes_by_name,
)

# ----------------------------- App & CORS -----------------------------
app = FastAPI(title="OSS Agent API (in-process)", version="3.2")

def _parse_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "t", "yes", "y", "on"}

CORS_ALLOW_ORIGINS = [
    o.strip()
    for o in os.getenv(
        "CORS_ALLOW_ORIGINS",
        os.getenv("CORS_ALLOW_ORIGIN", "http://localhost:5173,http://localhost:8080")
    ).split(",")
    if o.strip()
]
CORS_ALLOW_CREDENTIALS = _parse_bool(os.getenv("CORS_ALLOW_CREDENTIALS", "true"), True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS if CORS_ALLOW_ORIGINS != ["*"] or not CORS_ALLOW_CREDENTIALS else ["http://localhost:5173"],
    allow_credentials=CORS_ALLOW_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------- Warm datasets (loaded once) -----------------------------
ALARMS: List[Dict[str, Any]] = []
PERF:   List[Dict[str, Any]] = []
INV:    List[Dict[str, Any]] = []
CUST:   List[Dict[str, Any]] = []
NODES:  List[Dict[str, Any]] = []

def _load_all_datasets() -> None:
    """Load CSVs once and prepare records + derived nodes."""
    global ALARMS, PERF, INV, CUST, NODES

    alarms_csv = os.getenv("ALARMS_CSV_PATH", "data/ran_network_alarms.csv")
    perf_csv   = os.getenv("PERFORMANCE_CSV_PATH", "data/ran_network_performance.csv")
    inv_csv    = os.getenv("INVENTORY_CSV_PATH", "data/ran_network_inventory.csv")
    cust_csv   = os.getenv("CUSTOMERS_CSV_PATH", "data/mobile_customers_usa_by_region.csv")

    # Alarms
    alarms_df = load_alarms_csv(alarms_csv)
    ALARMS = alarms_df.to_dict(orient="records") if alarms_df is not None else []

    # Performance
    perf_df = load_performance_csv_local(perf_csv)
    PERF = perf_df.to_dict(orient="records") if perf_df is not None else []

    # Inventory
    inv_df = load_inventory_csv_local(inv_csv)
    INV = inv_df.to_dict(orient="records") if inv_df is not None else []

    # Customers (enrich)
    cust_df_raw = load_customers_csv_local(cust_csv)
    cust_df = enhance_customer_df(cust_df_raw) if cust_df_raw is not None else None
    CUST = cust_df.to_dict(orient="records") if cust_df is not None else []

    # Derived nodes from alarms + union with perf/inv
    derived_nodes = nodes_from_alarms(alarms_df) if alarms_df is not None else []
    NODES = _union_nodes_by_name(derived_nodes, PERF, INV)

def _build_initial_state(user_input: str, session_id: str, request_id: Optional[str]) -> Dict[str, Any]:
    """Create the initial AgentState payload for graph.invoke."""
    return {
        "user_input": user_input,
        "session_id": session_id,
        "request_id": request_id,             # pre-seed to correlate, if used
        "nodes": NODES,
        "alarms": ALARMS,
        "performance_records": PERF,
        "inventory_records": INV,
        "customer_records": CUST,
    }

# ----------------------------- Lifespan -----------------------------
@app.on_event("startup")
def _startup():
    _load_all_datasets()
    app.state.ready = True

@app.on_event("shutdown")
def _shutdown():
    app.state.ready = False

# ----------------------------- Utilities -----------------------------
async def _invoke_graph(initial_state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run graph.invoke in a thread so we don't block the event loop.
    Your graph.invoke is synchronous; this keeps FastAPI responsive.
    """
    return await asyncio.to_thread(graph.invoke, initial_state)

# ----------------------------- Health & reload -----------------------------
@app.get("/healthz")
async def health():
    return {
        "ok": bool(getattr(app.state, "ready", False)),
        "version": app.version,
        "datasets": {
            "alarms": len(ALARMS),
            "performance": len(PERF),
            "inventory": len(INV),
            "customers": len(CUST),
            "nodes": len(NODES),
        },
    }

@app.post("/reload")
async def reload_datasets():
    """Reload CSVs on demand without restarting the API."""
    _load_all_datasets()
    return {
        "reloaded": True,
        "datasets": {
            "alarms": len(ALARMS),
            "performance": len(PERF),
            "inventory": len(INV),
            "customers": len(CUST),
            "nodes": len(NODES),
        },
    }

# ----------------------------- POST /run (non-stream) -----------------------------
@app.post("/run")
async def run_once(body: Dict[str, Any] = Body(..., example={"session_id": "web_session", "user_input": "Show open tickets"})):
    user_input = (body.get("user_input") or "").strip()
    if not user_input:
        return JSONResponse(status_code=400, content={"error": "user_input is required"})

    session_id = (body.get("session_id") or "web_session").strip() or "web_session"
    request_id = f"req-{uuid.uuid4().hex[:12]}"

    init = _build_initial_state(user_input=user_input, session_id=session_id, request_id=request_id)
    output = await _invoke_graph(init)

    # 'output' is the dict returned by finalize_agent in main.py (includes 'result', 'trace', etc.)
    return JSONResponse(content={"output": output})

# ----------------------------- WS /ws/chat (streaming with logger tap) -----------------------------
@app.websocket("/ws/chat")
async def ws_chat(ws: WebSocket):
    await ws.accept()
    loop = asyncio.get_running_loop()
    log_q: asyncio.Queue = asyncio.Queue()

    async def _send(kind: str, payload: dict):
        try:
            await ws.send_text(json.dumps({"event": kind, **payload}))
        except Exception:
            pass

    class _WsFlowHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                msg = record.getMessage()
                obj = None
                if isinstance(msg, str) and msg.strip().startswith("{") and msg.strip().endswith("}"):
                    try:
                        parsed = json.loads(msg)
                        if isinstance(parsed, dict) and parsed.get("event"):
                            obj = parsed
                    except Exception:
                        obj = None
                if obj:
                    loop.call_soon_threadsafe(log_q.put_nowait, ("flow", obj))
                else:
                    loop.call_soon_threadsafe(
                        log_q.put_nowait,
                        ("log", {"source": getattr(record, "name", "orchestrator"),
                                 "level": record.levelname.lower(), "line": msg})
                    )
            except Exception:
                pass

    orch_logger = logging.getLogger("orchestrator")
    ws_handler = _WsFlowHandler()
    ws_handler.setFormatter(logging.Formatter("%(message)s"))

    STOP = ("__stop__", None)

    async def pump():
        try:
            while True:
                kind, payload = await log_q.get()
                if kind == "__stop__":
                    break
                if kind == "flow":
                    await _send("flow", {"data": payload})
                else:
                    await _send("log", payload)
        except asyncio.CancelledError:
            pass

    pump_task = None
    try:
        # Attach once for the whole connection
        orch_logger.addHandler(ws_handler)
        if orch_logger.level > logging.INFO:
            orch_logger.setLevel(logging.INFO)

        pump_task = asyncio.create_task(pump())

        # 🔁 Process messages in a loop, keep the socket open
        while True:
            init_msg = await ws.receive_text()          # <-- waits for next query
            try:
                req = json.loads(init_msg)
            except Exception:
                await _send("error", {"message": "Invalid JSON"}); continue

            user_input = (req.get("user_input") or "").strip()
            if not user_input:
                await _send("error", {"message": "user_input is required"}); continue

            session_id = (req.get("session_id") or "web_session").strip() or "web_session"
            request_id = f"req-{uuid.uuid4().hex[:12]}"

            await _send("log", {"source": "system", "level": "info", "line": "Processing started"})

            init_state = _build_initial_state(user_input=user_input, session_id=session_id, request_id=request_id)

            t0 = time.perf_counter()
            output = await _invoke_graph(init_state)
            dt_ms = int((time.perf_counter() - t0) * 1000)

            await _send("final", {"payload": {"output": output, "duration_ms": dt_ms}})
            await _send("log", {"source": "system", "level": "info", "line": "Flow complete"})

    except WebSocketDisconnect:
        pass
    finally:
        with contextlib.suppress(Exception):
            orch_logger.removeHandler(ws_handler)
        if pump_task and not pump_task.done():
            with contextlib.suppress(Exception):
                log_q.put_nowait(STOP)
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await pump_task
        with contextlib.suppress(Exception):
            await ws.close()

# ----------------------------- Root -----------------------------
@app.get("/")
def root():
    return {
        "service": app.title,
        "version": app.version,
        "endpoints": {
            "ws_stream": "/ws/chat (or /ws/cli)",
            "run_once": "POST /run",
            "reload": "POST /reload",
            "health": "GET /healthz",
        },
        "datasets": {
            "alarms": len(ALARMS),
            "performance": len(PERF),
            "inventory": len(INV),
            "customers": len(CUST),
            "nodes": len(NODES),
        },
    }

if __name__ == "__main__":
    uvicorn.run("api_ws:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
