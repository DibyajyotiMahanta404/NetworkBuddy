# background_orchestrator.py
# -*- coding: utf-8 -*-
"""
Background Orchestrator:
- Watches alarms CSV for changes.
- Detects NEW 'Active' alarm groups (CorrelationID-first).
- For new Critical (configurable), asks tt_agent to create/reuse tickets.
- Performs lightweight Inventory & Customer impact checks.
- Emits structured JSONL events for observability.

Prereqs: alarm_loader.py, tt_agent.py
"""

import os
import json
import time
import uuid
import math
import logging
from typing import Dict, Any, List, Optional, Set, Tuple

import pandas as pd
import datetime as dt

from dotenv import load_dotenv
load_dotenv()

# --- Config (env-driven) -----------------------------------------------------
ALARMS_CSV_PATH = os.getenv("ALARMS_CSV_PATH", "data/ran_network_alarms.csv")
INVENTORY_CSV_PATH = os.getenv("INVENTORY_CSV_PATH", "data/ran_network_inventory.csv")
CUSTOMERS_CSV_PATH = os.getenv("CUSTOMERS_CSV_PATH", "data/mobile_customers_usa_by_region.csv")
PERF_CSV_PATH = os.getenv("PERF_CSV_PATH", "data/ran_network_performance.csv")
BG_ORCH_LOG_UNCHANGED = os.getenv("BG_ORCH_LOG_UNCHANGED", "false").strip().lower() in {"1","true","yes","on"}


POLL_INTERVAL_SEC = float(os.getenv("BG_ORCH_POLL_SEC", "5"))
CREATE_TT_FOR_SEVERITY = os.getenv("BG_ORCH_TT_SEVERITY", "critical").strip().lower()  # critical | major_plus | any

# State & logs
STATE_FILE = os.getenv("BG_ORCH_STATE", "data/bg_seen_active_keys.json")
EVENTS_LOG = os.getenv("BG_ORCH_EVENTS_LOG", "data/bg_orch_events.jsonl")

# Avoid LLM usage anywhere in this headless daemon
os.environ.setdefault("USE_LLM_SUMMARY", "false")

# --- Logging -----------------------------------------------------------------
LOG_LEVEL = os.getenv("BG_ORCH_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s [%(levelname)s] [bg-orch] %(message)s")

# --- Imports from project modules --------------------------------------------
# CSV normalization & status parsing
from tools.alarm_loader import load_alarms_csv  # harmonizes schema & status
# ^ This produces canonical columns like NodeID, AlarmID, AlarmType, AlarmStatus, Severity, CreateTime, CorrelationID

# Ticket creation and deduplication stores (correlation-aware)
#from tt_agent import create_ticket_external  # uses OPEN/INDEX stores + emits TT_EVENTS_LOG acks. [2](https://cognizantonline-my.sharepoint.com/personal/2319536_cognizant_com/_layouts/15/download.aspx?UniqueId=223e16bc-ae49-4774-99f5-3b485f09fe59&Translate=false&tempauth=v1.eyJzaXRlaWQiOiIwMmQxNzBiYi01ZTBkLTQ5NGUtYTA3Zi02NTJiNGUwNDYzZjIiLCJhcHBfZGlzcGxheW5hbWUiOiJPZmZpY2UgMzY1IFNlYXJjaCBTZXJ2aWNlIiwiYXBwaWQiOiI2NmE4ODc1Ny0yNThjLTRjNzItODkzYy0zZThiZWQ0ZDY4OTkiLCJhdWQiOiIwMDAwMDAwMy0wMDAwLTBmZjEtY2UwMC0wMDAwMDAwMDAwMDAvY29nbml6YW50b25saW5lLW15LnNoYXJlcG9pbnQuY29tQGRlMDhjNDA3LTE5YjktNDI3ZC05ZmU4LWVkZjI1NDMwMGNhNyIsImV4cCI6IjE3NTgwMjkzODUifQ.CgoKBGFjcnMSAmMxCkAKDGVudHJhX2NsYWltcxIwQ05XcXBjWUdFQUFhRm1Sc1JFMVRWRVpSTWxVdGNXMVVaMHg0TmpWWVFVRXFBQT09CjIKCmFjdG9yYXBwaWQSJDAwMDAwMDAzLTAwMDAtMDAwMC1jMDAwLTAwMDAwMDAwMDAwMAoKCgRzbmlkEgI2NBILCIibpIfYybo-EAUaDjIwLjE5MC4xNDQuMTcwKixXUDdnaFVOL05PWU1lQ3ErM1hJYVBBaDZZcCtncndleU9kR0krMjRFekVJPTCgATgBQhChxkrXgGAAUMUVm3MECOx9ShBoYXNoZWRwcm9vZnRva2VuUhJbImttc2kiLCJkdmNfY21wIl1qJDAwN2RlYTc5LWNhYWQtZjc0YS1hMGE1LTZjOWEzZTE4NWJjZXIpMGguZnxtZW1iZXJzaGlwfDEwMDMyMDAzYTk1ZDliMmFAbGl2ZS5jb216ATKCARIJB8QI3rkZfUIRn-jt8lQwDKeSAQVNb2hpdJoBBkdhdXRhbaIBFTIzMTk1MzZAY29nbml6YW50LmNvbaoBEDEwMDMyMDAzQTk1RDlCMkGyAU1jb250YWluZXIuc2VsZWN0ZWQgYWxsZmlsZXMud3JpdGUgbXlmaWxlcy53cml0ZSBteWZpbGVzLnJlYWQgYWxscHJvZmlsZXMucmVhZMgBAQ.yOIHQZyE9_SR5YJP8Oqufm3g3qHUm-CDE8GgbQVBfDg&ApiVersion=2.0&web=1)
from tt_agent import create_ticket_external, close_ticket_external_by_key
from tt_agent import create_ticket_external

from agents_core import (
FaultAgent, ImpactHelper, NotificationAgent,
load_inventory_csv, load_customers_csv, enrich_customers,
load_performance_csv, PerformanceAgent, ProcessFlow
)

##HELPER##
import hashlib

def _file_sha256(path: str) -> str:
    """Fast, stable fingerprint of a file's content. Empty string if path missing/error."""
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return ""

# -----------------------------------------------------------------------------
# Simple in-memory caches to avoid reloading on every mtime tick
_INV_CACHE = None
_CUST_CACHE_ENRICHED = None

def _get_inventory() -> Optional[pd.DataFrame]:
    global _INV_CACHE
    if _INV_CACHE is None:
        _INV_CACHE = load_inventory_csv(INVENTORY_CSV_PATH)
    return _INV_CACHE

def _get_customers_enriched() -> Optional[pd.DataFrame]:
    global _CUST_CACHE_ENRICHED
    if _CUST_CACHE_ENRICHED is None:
        raw = load_customers_csv(CUSTOMERS_CSV_PATH)
        _CUST_CACHE_ENRICHED = enrich_customers(raw)
        # NEW: explicit CustomerAgent logs for visibility
        ProcessFlow.agent(
            "CustomerAgent", "load_and_enrich",
            path=CUSTOMERS_CSV_PATH,
            rows=0 if _CUST_CACHE_ENRICHED is None else len(_CUST_CACHE_ENRICHED)
        )
        ProcessFlow.log(
            0, "Customers loaded & enriched",
            path=CUSTOMERS_CSV_PATH,
            total=0 if _CUST_CACHE_ENRICHED is None else len(_CUST_CACHE_ENRICHED)
        )
    return _CUST_CACHE_ENRICHED


SEV_RANK = {"critical": 3, "major": 2, "minor": 1, "warning": 0}
SEV_RANK_PERF = {"critical": 3, "major": 2, "minor": 1, "ok": 0}
PERF_IMPROVEMENT_DELTA = int(os.getenv("PERF_IMPROVEMENT_DELTA", "2"))  # score drop to call out improvement
def _sev_rank_value(s: str) -> int:
    return SEV_RANK.get((s or "").strip().lower(), -1)

def _safe_dir(path: str) -> str:
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    return d
# --- Replace these two functions in background_orchestrator.py ----------------
STATE_FILE = os.getenv("BG_ORCH_STATE", "data/bg_state.json")

STATE_FILE = os.getenv("BG_ORCH_STATE", "data/bg_state.json")

def _load_state() -> Dict[str, Any]:
    """
    State schema:
      {
        "groups": { ... },           # alarm groups
        "perf":   { ... },           # performance degradations (per key)
        "perf_meta": {               # performance-global metadata
            "csv_mtime": <float>,    # last processed mod-time
            "csv_hash": "<hex>"      # sha256 of last processed file content
        }
      }
    """
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                obj = json.load(f)
            obj.setdefault("groups", {})
            obj.setdefault("perf", {})
            pm = obj.setdefault("perf_meta", {})
            pm.setdefault("csv_mtime", 0.0)
            pm.setdefault("csv_hash", "")
            obj["perf_meta"] = pm
            return obj

        # Back-compat import of old seen-keys file, if any
        old_path = os.getenv("BG_ORCH_STATE_OLD", "data/bg_seen_active_keys.json")
        if os.path.exists(old_path):
            with open(old_path, "r", encoding="utf-8") as f:
                old = json.load(f)
            seen = old.get("seen_keys") or []
            return {
                "groups": {k: {"sig": (), "severity": "", "last_seen": ""} for k in seen},
                "perf": {},
                "perf_meta": {"csv_mtime": 0.0, "csv_hash": ""},
            }

        return {"groups": {}, "perf": {}, "perf_meta": {"csv_mtime": 0.0, "csv_hash": ""}}
    except Exception:
        return {"groups": {}, "perf": {}, "perf_meta": {"csv_mtime": 0.0, "csv_hash": ""}}

def _save_state(obj: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
    tmp = STATE_FILE + f".tmp-{uuid.uuid4().hex}.json"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, default=str)
    os.replace(tmp, STATE_FILE)


def _emit_event(ev: Dict[str, Any]) -> None:
    try:
        _safe_dir(EVENTS_LOG)
        payload = {**ev}
        payload.setdefault("source", "bg-orchestrator")
        payload["ts"] = dt.datetime.now(dt.timezone.utc).isoformat()
        with open(EVENTS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    except Exception:
        logging.exception("[bg-orch] Failed to write event")

def _key_for_row(row: Dict[str, Any]) -> str:
    """Correlation-first unique key; else per alarm"""
    corr = (row.get("CorrelationID") or "").strip()
    if corr:
        return json.dumps(["corr", corr], ensure_ascii=False, separators=(",", ":"))
    node = (row.get("NodeID") or "").strip()
    aid = (row.get("AlarmID") or "").strip()
    atype = (row.get("AlarmType") or "").strip()
    return json.dumps([node, aid, atype], ensure_ascii=False, separators=(",", ":"))

def _group_active_alarms(df: Optional[pd.DataFrame]) -> Dict[str, Dict[str, Any]]:
    """
    Return map key->payload for ACTIVE alarm groups.
    Payload includes: node, alarm_id, alarm_type, severity, description, correlation_id, ip_address, rows.
    """
    out: Dict[str, Dict[str, Any]] = {}
    if df is None or df.empty or "AlarmStatus" not in df.columns:
        return out

    dd = df[df["AlarmStatus"].astype(str).str.strip().str.lower() == "active"].copy()
    if dd.empty:
        return out

    # A parsed time column for tie-break
    parsed = None
    for c in ("CreateTime", "Timestamp", "EventTime"):
        if c in dd.columns:
            parsed = pd.to_datetime(dd[c], errors="coerce", utc=True)
            dd["_t"] = parsed
            break

    # Split by correlation id presence
    dd["__corr"] = dd.get("CorrelationID", "").astype(str).str.strip()
    has_corr = dd[dd["__corr"].str.len() > 0]
    no_corr = dd[dd["__corr"].str.len() == 0]

    # 1) With correlation id
    if not has_corr.empty:
        g = has_corr.groupby("__corr", dropna=False)
        for corr_id, grp in g:
            grp = grp.copy()
            grp["_rank"] = grp["Severity"].map(lambda s: _sev_rank_value(s))
            grp.sort_values(by=["_rank", "_t"], ascending=[False, False], inplace=True, na_position="last")
            primary = grp.iloc[0].to_dict()
            key = json.dumps(["corr", corr_id], ensure_ascii=False, separators=(",", ":"))
            out[key] = {
                "node": (primary.get("NodeID") or "").strip(),
                "alarm_id": (primary.get("AlarmID") or "").strip(),
                "alarm_type": (primary.get("AlarmType") or "").strip(),
                "severity": (primary.get("Severity") or "").strip(),
                "description": (primary.get("AlarmDescription") or "").strip(),
                "correlation_id": corr_id,
                "ip_address": (primary.get("IPAddress") or primary.get("IP_Address") or "").strip(),
                "rows": [r._asdict() if hasattr(r, "_asdict") else {**r} for _, r in grp.iterrows()],
            }

    # 2) Without correlation id -> per-alarm uniqueness
    if not no_corr.empty:
        seen: Set[str] = set()
        for _, r in no_corr.iterrows():
            row = r.to_dict()
            node = (row.get("NodeID") or "").strip()
            aid = (row.get("AlarmID") or "").strip()
            atype = (row.get("AlarmType") or "").strip()
            key = json.dumps([node, aid, atype], ensure_ascii=False, separators=(",", ":"))
            if key in seen:
                continue
            out[key] = {
                "node": node,
                "alarm_id": aid,
                "alarm_type": atype,
                "severity": (row.get("Severity") or "").strip(),
                "description": (row.get("AlarmDescription") or "").strip(),
                "correlation_id": "",
                "ip_address": (row.get("IPAddress") or row.get("IP_Address") or "").strip(),
                "rows": [row],
            }
            seen.add(key)

    return out

def _severity_gate(sev: str) -> bool:
    s = (sev or "").strip().lower()
    if CREATE_TT_FOR_SEVERITY == "any":
        return True
    if CREATE_TT_FOR_SEVERITY == "major_plus":
        return SEV_RANK.get(s, -1) >= SEV_RANK["major"]
    return s == "critical"

# --- Lightweight inventory & customer impact ---------------------------------

def _load_inventory_df(path: str) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(path)
        # Canonicalize like in alarm_loader (subset for inventory). [3](https://cognizantonline-my.sharepoint.com/personal/2319536_cognizant_com/_layouts/15/download.aspx?UniqueId=0a6446ee-d9cc-4253-a118-83745583027a&Translate=false&tempauth=v1.eyJzaXRlaWQiOiIwMmQxNzBiYi01ZTBkLTQ5NGUtYTA3Zi02NTJiNGUwNDYzZjIiLCJhcHBfZGlzcGxheW5hbWUiOiJPZmZpY2UgMzY1IFNlYXJjaCBTZXJ2aWNlIiwiYXBwaWQiOiI2NmE4ODc1Ny0yNThjLTRjNzItODkzYy0zZThiZWQ0ZDY4OTkiLCJhdWQiOiIwMDAwMDAwMy0wMDAwLTBmZjEtY2UwMC0wMDAwMDAwMDAwMDAvY29nbml6YW50b25saW5lLW15LnNoYXJlcG9pbnQuY29tQGRlMDhjNDA3LTE5YjktNDI3ZC05ZmU4LWVkZjI1NDMwMGNhNyIsImV4cCI6IjE3NTgwMjkzODUifQ.CgoKBGFjcnMSAmMxCkAKDGVudHJhX2NsYWltcxIwQ05XcXBjWUdFQUFhRm1Sc1JFMVRWRVpSTWxVdGNXMVVaMHg0TmpWWVFVRXFBQT09CjIKCmFjdG9yYXBwaWQSJDAwMDAwMDAzLTAwMDAtMDAwMC1jMDAwLTAwMDAwMDAwMDAwMAoKCgRzbmlkEgI2NBILCIrl-YnYybo-EAUaDjIwLjE5MC4xNDQuMTcwKixxNzh2MFg1YlJuVEQxY0NWZjZ2ZWJJWmlvU2JIczhnOVJZZExaUWd6R0dRPTCgATgBQhChxkrXktAAUMUVllPPkxuEShBoYXNoZWRwcm9vZnRva2VuUhJbImttc2kiLCJkdmNfY21wIl1qJDAwN2RlYTc5LWNhYWQtZjc0YS1hMGE1LTZjOWEzZTE4NWJjZXIpMGguZnxtZW1iZXJzaGlwfDEwMDMyMDAzYTk1ZDliMmFAbGl2ZS5jb216ATKCARIJB8QI3rkZfUIRn-jt8lQwDKeSAQVNb2hpdJoBBkdhdXRhbaIBFTIzMTk1MzZAY29nbml6YW50LmNvbaoBEDEwMDMyMDAzQTk1RDlCMkGyAU1jb250YWluZXIuc2VsZWN0ZWQgYWxsZmlsZXMud3JpdGUgbXlmaWxlcy53cml0ZSBteWZpbGVzLnJlYWQgYWxscHJvZmlsZXMucmVhZMgBAQ.62tw8oYfJ3h6bXhHBh-PUerYw5BZczoSFZEXdmL7QPA&ApiVersion=2.0&web=1)
        df.columns = [str(c).strip().lower().replace("-", "_").replace("%", "percent").replace(" ", "_") for c in df.columns]
        rename = {
            "nodename": "node",
            "sitestatus": "site_status",
            "equipmenttype": "equipment_type",
            "equipmentstatus": "equipment_status",
            "region": "region",
            "ipaddress": "ip_address",
            "hostname": "hostname",
            "siteid": "site_id",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        if "node" not in df.columns:
            raise ValueError("Inventory CSV missing Nodename->node column")
        return df
    except Exception as e:
        logging.warning(f"[bg-orch] inventory load failed: {e}")
        return None

def _load_customers_df_enriched(path: str) -> Optional[pd.DataFrame]:
    """
    Reuse your enrichment heuristics from main.py (deterministic satisfaction/call_drop/churn/risk).
    For now we inline a minimal subset to avoid importing the LLM-attached main.py. 
    """
    try:
        df = pd.read_csv(path)
        import re as _re
        df.columns = [_re.sub(r"[^\w]+", "_", str(c).strip()).lower() for c in df.columns]
        rename = {
            "customerid": "CustomerID",
            "region": "Region",
            "tenuremonths": "TenureMonths",
            "arpu_inr": "ARPU_INR",
            "complaintslast90days": "ComplaintsLast90Days",
            "nps": "NPS",
        }
        out = {}
        for c in df.columns:
            out[rename.get(c, c)] = df[c]
        z = pd.DataFrame(out)
        # defaults
        if "ComplaintsLast90Days" not in z.columns:
            z["ComplaintsLast90Days"] = 0
        if "NPS" not in z.columns:
            z["NPS"] = 0

        # Deterministic enrichment (mirrors main.py approach). [1](https://cognizantonline-my.sharepoint.com/personal/2319536_cognizant_com/_layouts/15/download.aspx?UniqueId=f2c85913-a331-4c3d-91be-40e4027481c0&Translate=false&tempauth=v1.eyJzaXRlaWQiOiIwMmQxNzBiYi01ZTBkLTQ5NGUtYTA3Zi02NTJiNGUwNDYzZjIiLCJhcHBfZGlzcGxheW5hbWUiOiJPZmZpY2UgMzY1IFNlYXJjaCBTZXJ2aWNlIiwiYXBwaWQiOiI2NmE4ODc1Ny0yNThjLTRjNzItODkzYy0zZThiZWQ0ZDY4OTkiLCJhdWQiOiIwMDAwMDAwMy0wMDAwLTBmZjEtY2UwMC0wMDAwMDAwMDAwMDAvY29nbml6YW50b25saW5lLW15LnNoYXJlcG9pbnQuY29tQGRlMDhjNDA3LTE5YjktNDI3ZC05ZmU4LWVkZjI1NDMwMGNhNyIsImV4cCI6IjE3NTgwMjkzODUifQ.CgoKBGFjcnMSAmMxCkAKDGVudHJhX2NsYWltcxIwQ05XcXBjWUdFQUFhRm1Sc1JFMVRWRVpSTWxVdGNXMVVaMHg0TmpWWVFVRXFBQT09CjIKCmFjdG9yYXBwaWQSJDAwMDAwMDAzLTAwMDAtMDAwMC1jMDAwLTAwMDAwMDAwMDAwMAoKCgRzbmlkEgI2NBILCMKklYfYybo-EAUaDjIwLjE5MC4xNDQuMTcwKixsT2lpdlZPcWs3NEhTTnF5eENOcHRKamxvZFBWUWRLV2liVGIyYytLVDh3PTCgATgBQhChxkrXfaAAUMUVldCBXs-TShBoYXNoZWRwcm9vZnRva2VuUhJbImttc2kiLCJkdmNfY21wIl1qJDAwN2RlYTc5LWNhYWQtZjc0YS1hMGE1LTZjOWEzZTE4NWJjZXIpMGguZnxtZW1iZXJzaGlwfDEwMDMyMDAzYTk1ZDliMmFAbGl2ZS5jb216ATKCARIJB8QI3rkZfUIRn-jt8lQwDKeSAQVNb2hpdJoBBkdhdXRhbaIBFTIzMTk1MzZAY29nbml6YW50LmNvbaoBEDEwMDMyMDAzQTk1RDlCMkGyAU1jb250YWluZXIuc2VsZWN0ZWQgYWxsZmlsZXMud3JpdGUgbXlmaWxlcy53cml0ZSBteWZpbGVzLnJlYWQgYWxscHJvZmlsZXMucmVhZMgBAQ.ZbSQp1RWs8XpteQGNLCufwTZht5FL8Ymr0HeULvQQOU&ApiVersion=2.0&web=1)
        def _seed(k: str) -> int:
            return hash(k) % (2**32)
        import numpy as _np
        _np.random.seed(42)
        z["satisfaction_score"] = z["CustomerID"].astype(str).map(lambda cid: _np.random.RandomState(_seed(f"{cid}_s")).choice(range(1, 11), p=[0.05,0.05,0.10,0.10,0.15,0.15,0.15,0.15,0.05,0.05]))
        z["call_drop_rate"] = z["CustomerID"].astype(str).map(lambda cid: float(_np.random.RandomState(_seed(f"{cid}_n")).uniform(0.5, 8.0)))
        def _churn(row) -> float:
            prob = 0.0
            s = row.get("satisfaction_score", 7)
            if s <= 3: prob += 0.4
            elif s <= 5: prob += 0.25
            elif s <= 7: prob += 0.1
            c = row.get("ComplaintsLast90Days", 0)
            if c > 2: prob += 0.2
            elif c > 0: prob += 0.1
            t = row.get("TenureMonths", 0)
            if t < 6: prob += 0.15
            elif t < 12: prob += 0.1
            d = row.get("call_drop_rate", 0.0)
            if d > 5.0: prob += 0.15
            elif d > 3.0: prob += 0.1
            nps = row.get("NPS", 0)
            if nps < -20: prob += 0.1
            elif nps < 0: prob += 0.05
            return min(prob, 1.0)
        z["churn_probability"] = z.apply(_churn, axis=1)
        def _risk(p: float) -> str:
            if p >= 0.7: return "Critical"
            if p >= 0.5: return "High"
            if p >= 0.3: return "Medium"
            return "Low"
        z["risk_level"] = z["churn_probability"].apply(_risk)
        return z
    except Exception as e:
        logging.warning(f"[bg-orch] customers load failed: {e}")
        return None

def _inventory_impact(inv_df: Optional[pd.DataFrame], nodes: List[str]) -> Dict[str, Any]:
    if inv_df is None or not nodes:
        return {"regions": [], "issues": []}
    inv_df = inv_df.copy()
    inv_df["node_l"] = inv_df["node"].astype(str).str.strip().str.lower()
    node_set = {n.strip().lower() for n in nodes}
    sub = inv_df[inv_df["node_l"].isin(node_set)]
    issues = []
    for _, r in sub.iterrows():
        equip = str(r.get("equipment_status", "")).strip().lower()
        site = str(r.get("site_status", "")).strip().lower()
        if site and site != "active":
            issues.append(f"Site inactive: {r.get('node')} (site_status={r.get('site_status')})")
        if equip and equip not in {"operational", "active", "ok"}:
            issues.append(f"Equipment status issue: {r.get('node')} = {r.get('equipment_status')}")
    regions = sorted({str(x) for x in sub.get("region", pd.Series(dtype=str)).dropna().unique().tolist()})
    return {"regions": regions, "issues": issues}

def _customer_impact(cust_df: Optional[pd.DataFrame], regions: List[str]) -> Dict[str, Any]:
    if cust_df is None or not regions:
        return {"regions_considered": [], "high_risk_count": 0, "top_regions": []}
    sub = cust_df[cust_df["Region"].isin(regions)].copy()
    sub["is_high"] = sub["churn_probability"] >= 0.5
    high = int(sub["is_high"].sum())
    by_region = (
        sub.groupby("Region")["is_high"].sum()
        .sort_values(ascending=False)
        .head(5)
        .reset_index()
        .values.tolist()
    )
    return {"regions_considered": regions, "high_risk_count": high, "top_regions": by_region}

# --- Core loop ----------------------------------------------------------------

def _process_change(df: pd.DataFrame, state: Dict[str, Any]) -> Dict[str, Any]:
    # 0) Group Active alarms (FaultAgent)
    groups_now = FaultAgent.group_active_by_correlation(df)
    state.setdefault("groups", {})
    prev: Dict[str, Dict[str, Any]] = state["groups"]

    inv_df   = _get_inventory()
    cust_all = _get_customers_enriched()

    for key, payload in groups_now.items():
        node = payload.get("node", "")
        sev  = (payload.get("severity") or "").strip()
        at   = (payload.get("alarm_type") or "").strip()
        corr = (payload.get("correlation_id") or "").strip()

        # --- FaultAgent: grouped this key
        ProcessFlow.agent("FaultAgent", "group_active", key=key, node=node,
                          severity=sev, alarm_type=at, correlation_id=corr)

       # --- FaultAgent: grouped this key
        ProcessFlow.agent("FaultAgent", "group_active", key=key, node=node,
                        severity=sev, alarm_type=at, correlation_id=corr)

        # Signature for change detection
        def _last_event_epoch(rows):
            vals = []
            for r in rows or []:
                for c in ("CreateTime", "Timestamp", "EventTime", "ModifiedTime"):
                    v = r.get(c)
                    if v:
                        vals.append(v)
            if not vals: 
                return 0
            ts = pd.to_datetime(pd.Series(vals), errors="coerce", utc=True)
            return 0 if ts.isna().all() else int(ts.max().timestamp())

        new_sig = (SEV_RANK.get(sev.lower(), -1),
                len(payload.get("rows", [])),
                _last_event_epoch(payload.get("rows", [])))
        old = prev.get(key) or {}
        old_sig = tuple(old.get("sig") or ())
        old_sev = (old.get("severity") or "").strip().lower()
        severity_up = SEV_RANK.get(sev.lower(), -1) > SEV_RANK.get(old_sev, -1)
        changed = (old == {}) or (old_sig != new_sig) or severity_up

        # Log 1 (only when changed), or optionally log an "unchanged" line if enabled
        if changed:
            ProcessFlow.log(1, "Alarm identified", key=key, node=node,
                            severity=sev, alarm_type=at, correlation_id=corr)
        elif BG_ORCH_LOG_UNCHANGED:
            ProcessFlow.log(1, "Alarm observed (unchanged)", key=key, node=node,
                            severity=sev, alarm_type=at, correlation_id=corr)

        # ImpactAgent: map nodes -> regions
        ProcessFlow.agent("Orchestrator", "impact.map_nodes_to_regions", key=key, node=node)
        regions, node_map = ImpactHelper.map_nodes_to_regions([node], inv_df)

        ProcessFlow.log(2, "Alarm correlated and customer identified",
                        key=key, node=node, regions=regions, correlation_id=corr, mapping=node_map)

        # ImpactAgent: compute blast radius (customers in regions)
        # NEW: explicit CustomerAgent log for filtering customers by region
        ProcessFlow.agent("CustomerAgent", "filter_by_regions", key=key, node=node, regions=regions,
                        source_rows=0 if cust_all is None else len(cust_all))
        cust_sub = ImpactHelper.customers_in_regions(cust_all, regions)
        ProcessFlow.log(3, "Customers selected for impacted regions", key=key, node=node,
                        regions=regions, selected=len(cust_sub))

        # Orchestrator: compute blast radius next
        ProcessFlow.agent("Orchestrator", "impact.compute_blast_radius", key=key, node=node, regions=regions)
        blast = ImpactHelper.blast_radius(cust_sub)


        ProcessFlow.log(3, "Analyse customer churn", key=key, node=node, regions=regions,
                        blast=blast, high_risk=blast.get("high_risk", 0))

        # Orchestrator: severity gate (critical only)
        ProcessFlow.agent("Orchestrator", "severity_gate",
                          required=os.getenv("BG_ORCH_TT_SEVERITY", "critical"),
                          current=sev, key=key, node=node)
        if not _severity_gate(sev):
            # Non-critical path
            ProcessFlow.log(4, "Non-critical alarm observed; TT skipped", key=key, node=node,
                            severity=sev, alarm_type=at, correlation_id=corr)
        else:
            # Changed (new/escalated/updated rows) -> notify + ticket
            if changed:
                # NotificationAgent: incident
                ProcessFlow.agent("NotificationAgent", "send_incident", key=key, node=node, regions=regions)
                inc_payload = NotificationAgent.send_incident(
                    node=node, regions=regions, sev=sev,
                    alarm_type=at, blast=blast, correlation_id=corr
                )
                ProcessFlow.log(4, "Initiate notification", key=key, node=node, notification=inc_payload)

                # tt_agent: create ticket
                ProcessFlow.agent("tt_agent", "create_ticket", key=key, node=node, alarm_type=at,
                                  severity=sev, correlation_id=corr)
                res = create_ticket_external(
                    node=node, alarm_id=payload.get("alarm_id", ""), alarm_type=at,
                    severity=sev, create_time="", description=payload.get("description", ""),
                    correlation_id=corr, related_alarm="", ip_address=""
                )
                ProcessFlow.log(5, "Create ticket and initiate self heal", key=key, node=node,
                                ticket_result=res, correlation_id=corr)
            else:
                # Optional: silent when nothing changed; or emit a debug line if desired
                pass

        # Update snapshot
        prev[key] = {"sig": new_sig, "severity": sev.lower(),
                     "last_seen": dt.datetime.now(dt.timezone.utc).isoformat(),
                     "regions": regions, "alarm_type": at, "node": node, "correlation_id": corr}

    # Handle resolves (keys that disappeared)
    active_keys = set(groups_now.keys())
    stale = [k for k in prev.keys() if k not in active_keys]
    for k in stale:
        snapshot = prev.get(k, {})
        node = snapshot.get("node", "")
        regions = snapshot.get("regions", []) or []
        at = snapshot.get("alarm_type", "")
        corr = snapshot.get("correlation_id", "")

        # FaultAgent: we interpret disappearance as 'detected clear'
        ProcessFlow.agent("FaultAgent", "detect_clear", key=k, node=node)

        # tt_agent: close
        ProcessFlow.agent("tt_agent", "close_ticket", key=k, node=node, correlation_id=corr)
        try:
            res = close_ticket_external_by_key(k)
        except Exception as e:
            res = {"error": str(e)}

        # NotificationAgent: resolution
        ProcessFlow.agent("NotificationAgent", "send_resolution", key=k, node=node, regions=regions)
        res_payload = NotificationAgent.send_resolution(node=node, regions=regions, alarm_type=at, correlation_id=corr)

        ProcessFlow.log(6, "Issue resolved & customer notified", key=k, node=node,
                        ticket_close_result=res, resolution_notification=res_payload)

        del prev[k]

    state["groups"] = prev
    return state

def _process_performance(state: Dict[str, Any], perf_force: bool = False) -> Dict[str, Any]:
    """
    Detect performance degradations, improvements, and recovery; deduped across restarts.

    Guards against duplicates:
      - Optional mtime gate (disabled when perf_force=True)
      - Content hash (csv_hash) skip: saving the same file bytes won't reprocess
      - Per key 'notified_sig' tracks last notified signature

    Signature stability:
      - Use (score, severity, canonical_reason_keys) where canonical_reason_keys
        strips numeric jitter (e.g., "Latency high: 92.79 ms" -> "latency_high")

    State updates:
      - state["perf_meta"]["csv_mtime"], state["perf_meta"]["csv_hash"]
      - state["perf"][key] -> {'sig', 'notified_sig', 'severity', ...}
    """
    state.setdefault("perf", {})
    meta = state.setdefault("perf_meta", {"csv_mtime": 0.0, "csv_hash": ""})

    # --- Current file state ---
    current_mtime = 0.0
    current_hash = ""
    try:
        if os.path.exists(PERF_CSV_PATH):
            current_mtime = float(os.path.getmtime(PERF_CSV_PATH))
            current_hash = _file_sha256(PERF_CSV_PATH)
    except Exception:
        pass

    last_mtime = float(meta.get("csv_mtime", 0.0))
    last_hash = meta.get("csv_hash", "")

    # --- Skip if nothing changed ---
    # 1) If not forced and mtime didn't advance -> skip
    if (not perf_force) and (current_mtime <= last_mtime):
        return state
    # 2) If content bytes identical (hash unchanged) -> skip
    #    This prevents re-notify when you only save the file without KPI changes.
    if current_hash and (current_hash == last_hash):
        # Still update mtime to avoid repeated checks on same content
        meta["csv_mtime"] = current_mtime
        state["perf_meta"] = meta
        return state

    # --- Load and detect ---
    dfp = load_performance_csv(PERF_CSV_PATH)
    perf_now = PerformanceAgent.detect_degradation(dfp)  # key -> payload
    prev: Dict[str, Dict[str, Any]] = state["perf"]

    inv_df = _get_inventory()
    cust_all = _get_customers_enriched()

    def _canon_reason_keys(reasons: List[str]) -> Tuple[str, ...]:
        """
        Convert 'Latency high: 92.79 ms' -> 'latency_high'
        Convert 'Call drop above target: 2.18%' -> 'call_drop_above_target'
        Ensures signature ignores numeric jitter.
        """
        keys = []
        for r in reasons or []:
            base = r.split(":", 1)[0].strip().lower().replace(" ", "_").replace("-", "_")
            if base:
                keys.append(base)
        # Sort & dedupe
        return tuple(sorted(set(keys)))

    # ---- NEW/CHANGED/IMPROVING degradations ----
    for key, payload in perf_now.items():
        node = payload.get("node", "")
        site_id = payload.get("site_id", "")
        sev = (payload.get("severity") or "").strip().lower()
        reasons = payload.get("reasons", [])
        metrics = payload.get("metrics", {})
        score = int(payload.get("score", 0))

        ProcessFlow.agent("PerformanceAgent", "detect_degradation", key=key, node=node, site_id=site_id, severity=sev)
        ProcessFlow.log(1, "Performance degradation identified", key=key, node=node, site_id=site_id,
                        severity=sev, reasons=reasons, metrics=metrics)

        # Stable signature uses canonical reason keys (no numeric parts)
        new_sig = (score, sev, _canon_reason_keys(reasons))
        old = prev.get(key) or {}
        old_sig = tuple(old.get("sig") or ())
        notified_sig = tuple(old.get("notified_sig") or ())
        old_sev = (old.get("severity") or "").strip().lower()
        old_score = 0
        try:
            old_score = int(old_sig[0]) if old_sig else 0
        except Exception:
            pass
        # Regions & customers
        ProcessFlow.agent("Orchestrator", "impact.map_nodes_to_regions", key=key, node=node)
        regions, node_map = ImpactHelper.map_nodes_to_regions([node], inv_df)
        ProcessFlow.log(2, "Performance correlated to regions", key=key, node=node, site_id=site_id,
                        regions=regions, mapping=node_map)

        # NEW: explicit CustomerAgent log for filtering customers by region (performance flow)
        ProcessFlow.agent("CustomerAgent", "filter_by_regions", key=key, node=node, regions=regions,
                        source_rows=0 if cust_all is None else len(cust_all))
        cust_sub = ImpactHelper.customers_in_regions(cust_all, regions)
        ProcessFlow.log(3, "Customers selected for performance impact", key=key, node=node,
                        regions=regions, selected=len(cust_sub))

        # Orchestrator: compute blast radius next
        ProcessFlow.agent("Orchestrator", "impact.compute_blast_radius", key=key, node=node, regions=regions)
        blast = ImpactHelper.blast_radius(cust_sub)

        affected = int(blast.get("total", 0))
        ProcessFlow.log(3, "Analyse customer impact from performance", key=key, node=node, regions=regions,
                        blast=blast, affected_customers=affected)

        # Change classification
        old_rank = SEV_RANK_PERF.get(old_sev, 0)
        new_rank = SEV_RANK_PERF.get(sev, 0)
        sev_improved = new_rank < old_rank
        sev_worsened = new_rank > old_rank
        score_improved = (old_sig and (old_score >= score + max(1, PERF_IMPROVEMENT_DELTA)))
        changed = (old == {}) or (old_sig != new_sig)

        # Notify only if signature changed AND not already notified
        should_notify = changed and (new_sig != notified_sig)
        if should_notify:
            if sev_improved or score_improved:
                ProcessFlow.log(4, "Performance trending better", key=key, node=node,
                                old_severity=old_sev, new_severity=sev, old_score=old_score, new_score=score)
                ProcessFlow.agent("NotificationAgent", "send_performance_improvement", key=key, node=node, regions=regions)
                perf_msg = NotificationAgent.send_performance_improvement(
                    node=node, site_id=site_id, regions=regions, severity=sev,
                    old_severity=old_sev, metrics=metrics, affected_customers=affected,
                    reasons=reasons, old_score=old_score, new_score=score, correlation_id=""
                )
                ProcessFlow.log(5, "Performance improvement notification sent", key=key, node=node, notification=perf_msg)
            else:
                ProcessFlow.agent("NotificationAgent", "send_performance", key=key, node=node, regions=regions)
                perf_msg = NotificationAgent.send_performance(
                    node=node, site_id=site_id, regions=regions, severity=sev,
                    metrics=metrics, affected_customers=affected, reasons=reasons, correlation_id=""
                )
                ProcessFlow.log(5, "Performance notification sent", key=key, node=node, notification=perf_msg)

            notified_sig = new_sig  # mark as notified

        # Update snapshot
        prev[key] = {
            "sig": new_sig,
            "notified_sig": notified_sig,
            "severity": sev,
            "last_seen": dt.datetime.now(dt.timezone.utc).isoformat(),
            "regions": regions,
            "node": node,
            "site_id": site_id,
        }

    # ---- RECOVERY (no longer degraded) ----
    active_keys = set(perf_now.keys())
    stale = [k for k in list(prev.keys()) if k not in active_keys]
    for k in stale:
        snapshot = prev.get(k, {})
        node = snapshot.get("node", "")
        site_id = snapshot.get("site_id", "")
        regions = snapshot.get("regions", []) or []
        had_notified = bool(snapshot.get("notified_sig"))

        ProcessFlow.agent("PerformanceAgent", "detect_recovery", key=k, node=node, site_id=site_id)

        if had_notified:
            ProcessFlow.agent("NotificationAgent", "send_resolution", key=k, node=node, regions=regions)
            res_payload = NotificationAgent.send_resolution(node=node, regions=regions, alarm_type="performance")
            ProcessFlow.log(6, "Performance issue resolved & customers notified", key=k, node=node,
                            resolution_notification=res_payload)

        del prev[k]

    # Persist latest mtime & hash (prevents reprocess on save-only)
    meta["csv_mtime"] = current_mtime
    meta["csv_hash"] = current_hash
    state["perf_meta"] = meta
    state["perf"] = prev
    return state

def run_daemon() -> None:
    logging.info(
        f"[bg-orch] Watching alarms={ALARMS_CSV_PATH} & performance={PERF_CSV_PATH} "
        f"every {POLL_INTERVAL_SEC:.0f}s"
    )

    state = _load_state()

    # Start with persisted perf mtime to avoid false reprocessing on restart
    last_alarm_mtime = 0.0
    last_perf_mtime = float(state.get("perf_meta", {}).get("csv_mtime", 0.0))

    while True:
        try:
            alarm_changed = False
            perf_changed = False

            # --- Check alarms CSV mtime (if present) ---
            try:
                if os.path.exists(ALARMS_CSV_PATH):
                    m_alarm = os.path.getmtime(ALARMS_CSV_PATH)
                    if m_alarm > last_alarm_mtime:
                        alarm_changed = True
                        last_alarm_mtime = m_alarm
                else:
                    # Optional: log occasionally if missing
                    pass
            except Exception:
                logging.exception("[bg-orch] Failed to stat alarms CSV")

            # --- Check performance CSV mtime (if present) ---
            try:
                if os.path.exists(PERF_CSV_PATH):
                    m_perf = os.path.getmtime(PERF_CSV_PATH)
                    if m_perf > last_perf_mtime:
                        perf_changed = True
                        last_perf_mtime = m_perf
                else:
                    # Optional: log occasionally if missing
                    pass
            except Exception:
                logging.exception("[bg-orch] Failed to stat performance CSV")

            # If nothing changed, sleep and poll again
            if not alarm_changed and not perf_changed:
                time.sleep(POLL_INTERVAL_SEC)
                continue

            # --- Process alarms flow if alarms CSV changed ---
            if alarm_changed:
                ProcessFlow.agent(
                    "FaultAgent",
                    "detect_alarms",
                    details="Alarms CSV mtime changed; scanning for Active alarms",
                    path=ALARMS_CSV_PATH,
                )
                df = load_alarms_csv(ALARMS_CSV_PATH)  # normalized & parsed
                state = _process_change(df, state)      # alarms flow (Log 1..6)

            # --- Process performance flow if performance CSV changed ---
            if perf_changed:
                ProcessFlow.agent(
                    "PerformanceAgent",
                    "detect_file_change",
                    path=PERF_CSV_PATH,
                    details="Performance CSV mtime changed; scanning for degradations"
                )
                # We saw a real mtime change, so force processing; dedupe still prevents re-notify
                state = _process_performance(state, perf_force=True)

            # Persist updated state (both "groups" and "perf"+"perf_meta")
            _save_state(state)

        except KeyboardInterrupt:
            print("\n[bg-orch] Stopped.")
            break
        except Exception as e:
            logging.exception(f"[bg-orch] Error: {e}")
            time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Background Orchestrator daemon.")
    p.add_argument("-d", "--daemon", action="store_true", help="Run daemon loop (default).")
    p.add_argument("--once", action="store_true", help="Run a single pass if CSV changed.")
    args = p.parse_args()

    if args.once:
        df = load_alarms_csv(ALARMS_CSV_PATH)
        state = _load_state()
        state = _process_change(df, state)                 # alarms flow
        state = _process_performance(state, perf_force=True)  # force performance once, but dedupe by notified_sig
        _save_state(state)

        # optional: print a small summary
        print(json.dumps({
            "ok": True,
            "groups": len(state.get("groups", {})),
            "perf_keys": len(state.get("perf", {})),
            "perf_csv_mtime": state.get("perf_meta", {}).get("csv_mtime", 0.0)
        }, indent=2))
        sys.exit(0)
    else:
        run_daemon()
