# tt_agent.py
# Active-only ticketing daemon:
# - Correlation-based grouping (one ticket per correlation_id)
# - Windows-safe I/O (atomic replace + retries)
# - Conversation scoping + batch create
# - Append-only ACK events (TT_CREATED / TT_ESCALATED / TT_CLOSED) without ticket_id

import os
import sys
import json
import time
import uuid
import math
import logging
from typing import Dict, Any, Optional, Tuple, Set, List
import pandas as pd
import datetime as dt

# Optional: load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ----- Config -----
ALARMS_CSV_PATH = os.getenv("ALARMS_CSV_PATH", "data/ran_network_alarms.csv")
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "5"))

# Only OPEN tickets live here (remove entries when cleared)
OPEN_TICKETS_PATH = os.getenv("OPEN_TICKETS_PATH", "data/tickets_open.json")
# Key -> internal_id mapping for idempotent create while alarm remains Active
TICKETS_INDEX_PATH = os.getenv("TICKETS_INDEX_PATH", "data/tickets_index.json")
# Append-only audit log for removed tickets
CLOSED_TICKETS_LOG = os.getenv("CLOSED_TICKETS_LOG", "data/tickets_closed.jsonl")
# Severity filter for daemon loop creation: 'critical' (default), 'major_plus', 'any'
CREATE_TT_FOR_SEVERITY = os.getenv("CREATE_TT_FOR_SEVERITY", "critical").strip().lower()

# Conversation-scope config
CONV_TARGETS_FILE = os.getenv("CONV_TARGETS_FILE", "data/conv_targets.json").strip()
DAEMON_SCOPE = os.getenv("DAEMON_SCOPE", "all").strip().lower()  # 'all' or 'conversation_only'
CONV_TARGETS_TTL_SEC = int(os.getenv("CONV_TARGETS_TTL_SEC", "1800"))  # 30 min

# Event bus (append-only ACK from daemon to orchestrator)
TT_EVENTS_LOG = os.getenv("TT_EVENTS_LOG", "data/tt_events.jsonl")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s [%(levelname)s] %(message)s")

SEV_RANK = {"critical": 3, "major": 2, "minor": 1, "warning": 0}

# ----- Correlation helpers & schema shaping -----
ALLOWED_TT_FIELDS = [
    "correlation_id",
    "node",
    "status",
    "alarm_id",
    "alarm_type",
    "severity",
    "description",
    "ip_address",
    "related_alarm",  # single string
]

def _shape_tt_row(d: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only the fields we want in the TT JSON."""
    out = {}
    for k in ALLOWED_TT_FIELDS:
        v = d.get(k, "")
        if k == "related_alarm" and isinstance(v, list):
            v = ", ".join(str(x) for x in v)
        out[k] = v
    return out

# ----- Helpers (normalization & keys) -----
def _norm_string(v: Any) -> str:
    if v is None:
        return ""
    try:
        if isinstance(v, float) and math.isnan(v):
            return ""
    except Exception:
        pass
    s = str(v)
    return s.strip()

def _first_nonempty(*vals) -> str:
    for v in vals:
        s = _norm_string(v)
        if s:
            return s
    return ""

def _ticket_key(node: str, alarm_id: Any, alarm_type: str) -> str:
    """Per-alarm key (JSON list)."""
    parts = [_norm_string(node), _norm_string(alarm_id), _norm_string(alarm_type)]
    return json.dumps(parts, ensure_ascii=False, separators=(",", ":"))

def _legacy_ticket_key(node: str, alarm_id: Any, alarm_type: str) -> str:
    """Legacy newline-joined key (backward compatibility)."""
    return "\n".join([_norm_string(node), _norm_string(alarm_id), _norm_string(alarm_type)])

def _unpack_key(key: str) -> Tuple[str, str, str]:
    """Decode either JSON-encoded key or legacy newline key."""
    try:
        parts = json.loads(key)
        if isinstance(parts, list) and len(parts) == 3:
            return _norm_string(parts[0]), _norm_string(parts[1]), _norm_string(parts[2])
    except Exception:
        pass
    try:
        node, alarm_id, alarm_type = key.split("\n", 2)
        return _norm_string(node), _norm_string(alarm_id), _norm_string(alarm_type)
    except Exception:
        return "", "", ""

def _ticket_key_corr(corr_id: str) -> str:
    """Correlation-based key."""
    return json.dumps(["corr", _norm_string(corr_id)], ensure_ascii=False, separators=(",", ":"))

def _corr_from_row(r: Dict[str, Any]) -> str:
    # Accept normalized and raw header forms
    return _first_nonempty(r.get("CorrelationID"), r.get("Correlation_Id"), r.get("Correlation_ID"))

def _sev_rank_val(s: str) -> int:
    return SEV_RANK.get((_norm_string(s).lower()), -1)

def _alarm_type_from_row(r: Dict[str, Any]) -> str:
    return _first_nonempty(r.get("AlarmType"), r.get("Alarm_Type"), r.get("AlarmName"))

def _get_ip_from_row(r: Dict[str, Any]) -> str:
    return _first_nonempty(r.get("IPAddress"), r.get("IP_Address"))

def _format_related_alarm_string(rows: List[Dict[str, Any]], primary_rank: int) -> str:
    """
    Build: "Node:AlarmID(IP), Node2:AlarmID2(IP2)" using only lower-severity rows.
    """
    items: List[str] = []
    for r in rows:
        rank = _sev_rank_val(r.get("Severity"))
        if rank < primary_rank:
            node = _norm_string(r.get("NodeID"))
            aid = _norm_string(r.get("AlarmID"))
            ip  = _get_ip_from_row(r)
            node_part = node if node else "-"
            aid_part  = aid if aid else "-"
            ip_part   = ip if ip else "-"
            items.append(f"{node_part}:{aid_part}({ip_part})")
    return ", ".join(items)

def _find_existing_tid_for_rows(rows: List[Dict[str, Any]]) -> Optional[str]:
    """
    If any legacy per-alarm key already exists in index, return its ticket_id.
    Used to adopt an existing ticket for a correlation group (no duplicates).
    """
    idx = _load_index()
    for r in rows:
        node = _norm_string(r.get("NodeID"))
        alarm_id = _norm_string(r.get("AlarmID"))
        alarm_type = _alarm_type_from_row(r)
        legacy_key = _ticket_key(node, alarm_id, alarm_type)
        if legacy_key in idx:
            return idx[legacy_key]
    return None

# ----- Event bus -----
def _safe_dir(path: str) -> str:
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    return d

def _emit_event(ev: Dict[str, Any]) -> None:
    """Append a one-line JSON event to TT_EVENTS_LOG (no ticket_id in payload)."""
    try:
        _safe_dir(TT_EVENTS_LOG)
        payload = {**ev}
        payload.setdefault("source", "daemon")
        payload.setdefault("scope", DAEMON_SCOPE)
        payload["ts"] = dt.datetime.now(dt.timezone.utc).isoformat()
        line = json.dumps(payload, ensure_ascii=False, default=str)
        with open(TT_EVENTS_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        logging.exception(f"[Daemon] Failed to write event: {e}")

# ----- JSON storage (Windows-safe, atomic, cached) -----
_INDEX_CACHE: Optional[Dict[str, str]] = None
_OPEN_CACHE: Optional[Dict[str, Dict[str, Any]]] = None

def _load_json(path: str, default):
    try:
        _safe_dir(path)
        if not os.path.exists(path):
            return default
        for _ in range(2):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    text = f.read()
                if not text.strip():
                    raise ValueError("empty-json")
                return json.loads(text)
            except json.JSONDecodeError as e:
                logging.warning(f"Invalid JSON in {path}: {e}. Will heal.")
            except ValueError as ve:
                if str(ve) == "empty-json":
                    logging.warning(f"Empty JSON in {path}. Will heal.")
                else:
                    logging.exception(f"Error reading {path}: {ve}")
                break
            time.sleep(0.1)
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = f"{path}.corrupt-{ts}.json"
        try:
            os.replace(path, backup)
            logging.warning(f"Healed {path}: moved corrupt file to {backup}")
        except Exception as e:
            logging.warning(f"Could not backup corrupt {path}: {e}")
        return default
    except Exception as e:
        logging.exception(f"Failed to load JSON from {path}: {e}")
        return default

def _save_json(path: str, obj) -> None:
    dirpath = _safe_dir(path)
    tmp_path = os.path.join(dirpath, f".tmp-{uuid.uuid4().hex}.json")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, ensure_ascii=False, default=str)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        max_retries = 10
        backoff = 0.1
        for attempt in range(max_retries):
            try:
                os.replace(tmp_path, path)
                return
            except PermissionError as e:
                if attempt == 0:
                    logging.warning(f"Replace locked for {path}; will retry. Error: {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 1.0)
            except Exception as e:
                logging.exception(f"Unexpected error during replace to {path}: {e}")
                break
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(obj, f, indent=2, ensure_ascii=False, default=str)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
            logging.warning(f"Fell back to direct write for {path} after replace retries.")
        except Exception as e:
            logging.exception(f"Failed fallback direct write for {path}: {e}")
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

def _load_open_tickets() -> Dict[str, Dict[str, Any]]:
    global _OPEN_CACHE
    if _OPEN_CACHE is not None:
        return _OPEN_CACHE
    _OPEN_CACHE = _load_json(OPEN_TICKETS_PATH, {})
    return _OPEN_CACHE

def _save_open_tickets(data: Dict[str, Dict[str, Any]]):
    global _OPEN_CACHE
    _OPEN_CACHE = data
    _save_json(OPEN_TICKETS_PATH, data)

def _load_index() -> Dict[str, str]:
    global _INDEX_CACHE
    if _INDEX_CACHE is not None:
        return _INDEX_CACHE
    _INDEX_CACHE = _load_json(TICKETS_INDEX_PATH, {})
    return _INDEX_CACHE

def _save_index(idx: Dict[str, str]):
    global _INDEX_CACHE
    _INDEX_CACHE = idx
    _save_json(TICKETS_INDEX_PATH, idx)

def _append_closed_ticket(row: Dict[str, Any]) -> None:
    try:
        _safe_dir(CLOSED_TICKETS_LOG)
        row = {**row}  # already shaped
        row["closed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        row["status"] = "CLOSED"
        line = json.dumps(row, ensure_ascii=False, default=str)
        with open(CLOSED_TICKETS_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        logging.exception(f"Failed to append closed ticket audit: {e}")
# tt_agent.py
# Active-only ticketing daemon:
# - Correlation-based grouping (one ticket per correlation_id)
# - Windows-safe I/O (atomic replace + retries)
# - Conversation scoping + batch create
# - Append-only ACK events (TT_CREATED / TT_ESCALATED / TT_CLOSED) without ticket_id

import os
import sys
import json
import time
import uuid
import math
import logging
from typing import Dict, Any, Optional, Tuple, Set, List
import pandas as pd
import datetime as dt

# Optional: load .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ----- Config -----
ALARMS_CSV_PATH = os.getenv("ALARMS_CSV_PATH", "data/ran_network_alarms.csv")
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "5"))

# Only OPEN tickets live here (remove entries when cleared)
OPEN_TICKETS_PATH = os.getenv("OPEN_TICKETS_PATH", "data/tickets_open.json")
# Key -> internal_id mapping for idempotent create while alarm remains Active
TICKETS_INDEX_PATH = os.getenv("TICKETS_INDEX_PATH", "data/tickets_index.json")
# Append-only audit log for removed tickets
CLOSED_TICKETS_LOG = os.getenv("CLOSED_TICKETS_LOG", "data/tickets_closed.jsonl")
# Severity filter for daemon loop creation: 'critical' (default), 'major_plus', 'any'
CREATE_TT_FOR_SEVERITY = os.getenv("CREATE_TT_FOR_SEVERITY", "critical").strip().lower()

# Conversation-scope config
CONV_TARGETS_FILE = os.getenv("CONV_TARGETS_FILE", "data/conv_targets.json").strip()
DAEMON_SCOPE = os.getenv("DAEMON_SCOPE", "all").strip().lower()  # 'all' or 'conversation_only'
CONV_TARGETS_TTL_SEC = int(os.getenv("CONV_TARGETS_TTL_SEC", "1800"))  # 30 min

# Event bus (append-only ACK from daemon to orchestrator)
TT_EVENTS_LOG = os.getenv("TT_EVENTS_LOG", "data/tt_events.jsonl")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s [%(levelname)s] %(message)s")

SEV_RANK = {"critical": 3, "major": 2, "minor": 1, "warning": 0}

# ----- Correlation helpers & schema shaping -----
ALLOWED_TT_FIELDS = [
    "correlation_id",
    "node",
    "status",
    "alarm_id",
    "alarm_type",
    "severity",
    "description",
    "ip_address",
    "related_alarm",  # single string
]

def _shape_tt_row(d: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only the fields we want in the TT JSON."""
    out = {}
    for k in ALLOWED_TT_FIELDS:
        v = d.get(k, "")
        if k == "related_alarm" and isinstance(v, list):
            v = ", ".join(str(x) for x in v)
        out[k] = v
    return out

# ----- Helpers (normalization & keys) -----
def _norm_string(v: Any) -> str:
    if v is None:
        return ""
    try:
        if isinstance(v, float) and math.isnan(v):
            return ""
    except Exception:
        pass
    s = str(v)
    return s.strip()

def _first_nonempty(*vals) -> str:
    for v in vals:
        s = _norm_string(v)
        if s:
            return s
    return ""

def _ticket_key(node: str, alarm_id: Any, alarm_type: str) -> str:
    """Per-alarm key (JSON list)."""
    parts = [_norm_string(node), _norm_string(alarm_id), _norm_string(alarm_type)]
    return json.dumps(parts, ensure_ascii=False, separators=(",", ":"))

def _legacy_ticket_key(node: str, alarm_id: Any, alarm_type: str) -> str:
    """Legacy newline-joined key (backward compatibility)."""
    return "\n".join([_norm_string(node), _norm_string(alarm_id), _norm_string(alarm_type)])

def _unpack_key(key: str) -> Tuple[str, str, str]:
    """Decode either JSON-encoded key or legacy newline key."""
    try:
        parts = json.loads(key)
        if isinstance(parts, list) and len(parts) == 3:
            return _norm_string(parts[0]), _norm_string(parts[1]), _norm_string(parts[2])
    except Exception:
        pass
    try:
        node, alarm_id, alarm_type = key.split("\n", 2)
        return _norm_string(node), _norm_string(alarm_id), _norm_string(alarm_type)
    except Exception:
        return "", "", ""

def _ticket_key_corr(corr_id: str) -> str:
    """Correlation-based key."""
    return json.dumps(["corr", _norm_string(corr_id)], ensure_ascii=False, separators=(",", ":"))

def _corr_from_row(r: Dict[str, Any]) -> str:
    # Accept normalized and raw header forms
    return _first_nonempty(r.get("CorrelationID"), r.get("Correlation_Id"), r.get("Correlation_ID"))

def _sev_rank_val(s: str) -> int:
    return SEV_RANK.get((_norm_string(s).lower()), -1)

def _alarm_type_from_row(r: Dict[str, Any]) -> str:
    return _first_nonempty(r.get("AlarmType"), r.get("Alarm_Type"), r.get("AlarmName"))

def _get_ip_from_row(r: Dict[str, Any]) -> str:
    return _first_nonempty(r.get("IPAddress"), r.get("IP_Address"))

def _format_related_alarm_string(rows: List[Dict[str, Any]], primary_rank: int) -> str:
    """
    Build: "Node:AlarmID(IP), Node2:AlarmID2(IP2)" using only lower-severity rows.
    """
    items: List[str] = []
    for r in rows:
        rank = _sev_rank_val(r.get("Severity"))
        if rank < primary_rank:
            node = _norm_string(r.get("NodeID"))
            aid = _norm_string(r.get("AlarmID"))
            ip  = _get_ip_from_row(r)
            node_part = node if node else "-"
            aid_part  = aid if aid else "-"
            ip_part   = ip if ip else "-"
            items.append(f"{node_part}:{aid_part}({ip_part})")
    return ", ".join(items)

def _find_existing_tid_for_rows(rows: List[Dict[str, Any]]) -> Optional[str]:
    """
    If any legacy per-alarm key already exists in index, return its ticket_id.
    Used to adopt an existing ticket for a correlation group (no duplicates).
    """
    idx = _load_index()
    for r in rows:
        node = _norm_string(r.get("NodeID"))
        alarm_id = _norm_string(r.get("AlarmID"))
        alarm_type = _alarm_type_from_row(r)
        legacy_key = _ticket_key(node, alarm_id, alarm_type)
        if legacy_key in idx:
            return idx[legacy_key]
    return None

# ----- Event bus -----
def _safe_dir(path: str) -> str:
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    return d

def _emit_event(ev: Dict[str, Any]) -> None:
    """Append a one-line JSON event to TT_EVENTS_LOG (no ticket_id in payload)."""
    try:
        _safe_dir(TT_EVENTS_LOG)
        payload = {**ev}
        payload.setdefault("source", "daemon")
        payload.setdefault("scope", DAEMON_SCOPE)
        payload["ts"] = dt.datetime.now(dt.timezone.utc).isoformat()
        line = json.dumps(payload, ensure_ascii=False, default=str)
        with open(TT_EVENTS_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        logging.exception(f"[Daemon] Failed to write event: {e}")

# ----- JSON storage (Windows-safe, atomic, cached) -----
_INDEX_CACHE: Optional[Dict[str, str]] = None
_OPEN_CACHE: Optional[Dict[str, Dict[str, Any]]] = None

def _load_json(path: str, default):
    try:
        _safe_dir(path)
        if not os.path.exists(path):
            return default
        for _ in range(2):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    text = f.read()
                if not text.strip():
                    raise ValueError("empty-json")
                return json.loads(text)
            except json.JSONDecodeError as e:
                logging.warning(f"Invalid JSON in {path}: {e}. Will heal.")
            except ValueError as ve:
                if str(ve) == "empty-json":
                    logging.warning(f"Empty JSON in {path}. Will heal.")
                else:
                    logging.exception(f"Error reading {path}: {ve}")
                break
            time.sleep(0.1)
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = f"{path}.corrupt-{ts}.json"
        try:
            os.replace(path, backup)
            logging.warning(f"Healed {path}: moved corrupt file to {backup}")
        except Exception as e:
            logging.warning(f"Could not backup corrupt {path}: {e}")
        return default
    except Exception as e:
        logging.exception(f"Failed to load JSON from {path}: {e}")
        return default

def _save_json(path: str, obj) -> None:
    dirpath = _safe_dir(path)
    tmp_path = os.path.join(dirpath, f".tmp-{uuid.uuid4().hex}.json")
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2, ensure_ascii=False, default=str)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        max_retries = 10
        backoff = 0.1
        for attempt in range(max_retries):
            try:
                os.replace(tmp_path, path)
                return
            except PermissionError as e:
                if attempt == 0:
                    logging.warning(f"Replace locked for {path}; will retry. Error: {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 1.0)
            except Exception as e:
                logging.exception(f"Unexpected error during replace to {path}: {e}")
                break
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(obj, f, indent=2, ensure_ascii=False, default=str)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
            logging.warning(f"Fell back to direct write for {path} after replace retries.")
        except Exception as e:
            logging.exception(f"Failed fallback direct write for {path}: {e}")
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

def _load_open_tickets() -> Dict[str, Dict[str, Any]]:
    global _OPEN_CACHE
    if _OPEN_CACHE is not None:
        return _OPEN_CACHE
    _OPEN_CACHE = _load_json(OPEN_TICKETS_PATH, {})
    return _OPEN_CACHE

def _save_open_tickets(data: Dict[str, Dict[str, Any]]):
    global _OPEN_CACHE
    _OPEN_CACHE = data
    _save_json(OPEN_TICKETS_PATH, data)

def _load_index() -> Dict[str, str]:
    global _INDEX_CACHE
    if _INDEX_CACHE is not None:
        return _INDEX_CACHE
    _INDEX_CACHE = _load_json(TICKETS_INDEX_PATH, {})
    return _INDEX_CACHE

def _save_index(idx: Dict[str, str]):
    global _INDEX_CACHE
    _INDEX_CACHE = idx
    _save_json(TICKETS_INDEX_PATH, idx)

def _append_closed_ticket(row: Dict[str, Any]) -> None:
    try:
        _safe_dir(CLOSED_TICKETS_LOG)
        row = {**row}  # already shaped
        row["closed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        row["status"] = "CLOSED"
        line = json.dumps(row, ensure_ascii=False, default=str)
        with open(CLOSED_TICKETS_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        logging.exception(f"Failed to append closed ticket audit: {e}")
# ----- CSV read with retry -----
def _read_csv_with_retry(path: str, retries: int = 2, delay: float = 0.25):
    last_err = None
    for _ in range(retries + 1):
        try:
            from tools.alarm_loader import load_alarms_csv
            return load_alarms_csv(path)
        except Exception as e:
            last_err = e
            time.sleep(delay)
    raise last_err

# ----- Conversation targets (scoping) -----
def _load_conv_targets(now_utc: Optional[dt.datetime] = None) -> Optional[Set[str]]:
    try:
        if not os.path.exists(CONV_TARGETS_FILE):
            return None
        with open(CONV_TARGETS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        nodes = data.get("nodes") or []
        expires_at = data.get("expires_at")
        if expires_at:
            try:
                ts = pd.to_datetime(expires_at, errors="coerce", utc=True)
                if not pd.isna(ts):
                    now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
                    if ts.to_pydatetime() <= now_utc:
                        return None
            except Exception:
                pass
        norm = {_norm_string(n) for n in nodes if _norm_string(n)}
        return {n for n in norm if n}
    except Exception as e:
        logging.exception(f"[Daemon] Failed to read conv targets: {e}")
        return None

def set_conversation_targets(nodes: list, ttl_sec: int = CONV_TARGETS_TTL_SEC) -> Dict[str, Any]:
    try:
        now = dt.datetime.now(dt.timezone.utc)
        expires = now + dt.timedelta(seconds=int(max(1, ttl_sec)))
        payload = {
            "nodes": sorted({_norm_string(n) for n in nodes if _norm_string(n)}),
            "expires_at": expires.isoformat(),
        }
        _save_json(CONV_TARGETS_FILE, payload)
        logging.info(f"[Daemon] Conversation targets set: {payload['nodes']} (ttl={ttl_sec}s)")
        return payload
    except Exception as e:
        logging.exception(f"[Daemon] Failed to set conversation targets: {e}")
        return {}

# ----- Severity gates -----
def _sev_ok(sev: str) -> bool:
    s = (sev or "").strip().lower()
    if CREATE_TT_FOR_SEVERITY == "any":
        return True
    if CREATE_TT_FOR_SEVERITY == "major_plus":
        return SEV_RANK.get(s, -1) >= SEV_RANK["major"]
    return s == "critical"

def _sev_ok_mode(sev: str, mode: str) -> bool:
    s = (sev or "").strip().lower()
    if mode == "any":
        return True
    if mode == "major_plus":
        return SEV_RANK.get(s, -1) >= SEV_RANK["major"]
    if mode == "critical":
        return s == "critical"
    return s == "major"

# ----- Active alarm grouping (correlation-first) -----
def _active_alarm_groups(df: Optional[pd.DataFrame]) -> Dict[str, Dict[str, Any]]:
    """
    Build a map of active alarms grouped by CorrelationID when present.
    - key: correlation-based key ["corr", <id>] if CorrelationID present, else per-alarm key.
    - value: payload including description, correlation_id, ip_address, related_alarm (string).
    """
    out: Dict[str, Dict[str, Any]] = {}
    if df is None or df.empty:
        return out

    # Active rows only
    mask = df["AlarmStatus"].astype(str).str.strip().str.lower() == "active"
    dd = df[mask].copy()

    # Time for tie-breaks
    for c in ("CreateTime", "Timestamp", "EventTime"):
        if c in dd.columns:
            dd["_parsed_time"] = pd.to_datetime(dd[c], errors="coerce", utc=True)
            break

    # Split by correlation id presence
    dd["__corr"] = dd.apply(lambda r: _corr_from_row(r), axis=1)
    has_corr = dd[dd["__corr"].astype(str).str.len() > 0]
    no_corr = dd[dd["__corr"].astype(str).str.len() == 0]

    # 1) Groups with correlation id
    for corr_id, g in has_corr.groupby("__corr", dropna=False):
        g = g.copy()
        g["__rank"] = g["Severity"].map(lambda s: _sev_rank_val(s))
        g = g.sort_values(by=["__rank", "_parsed_time"], ascending=[False, False], na_position="last")
        primary_row = g.iloc[0].to_dict()
        primary_rank = int(primary_row.get("__rank", -1))
        related_alarm_str = _format_related_alarm_string([r for _, r in g.iterrows()], primary_rank)
        key = _ticket_key_corr(corr_id)
        out[key] = {
            "primary": primary_row,
            "node": _norm_string(primary_row.get("NodeID")),
            "alarm_id": _norm_string(primary_row.get("AlarmID")),
            "alarm_type": _alarm_type_from_row(primary_row),
            "severity": _norm_string(primary_row.get("Severity")),
            "description": _norm_string(primary_row.get("AlarmDescription")),
            "correlation_id": corr_id,
            "ip_address": _get_ip_from_row(primary_row),
            "related_alarm": related_alarm_str,
            "all_rows": [r.to_dict() for _, r in g.iterrows()],  # for adoption
        }

    # 2) Rows without correlation id -> legacy per-alarm uniqueness
    seen: Set[str] = set()
    for _, r in no_corr.iterrows():
        node = _norm_string(r.get("NodeID"))
        alarm_id = _norm_string(r.get("AlarmID"))
        alarm_type = _alarm_type_from_row(r)
        key = _ticket_key(node, alarm_id, alarm_type)
        if key in seen:
            continue
        rowd = r.to_dict()
        out[key] = {
            "primary": rowd,
            "node": node,
            "alarm_id": alarm_id,
            "alarm_type": alarm_type,
            "severity": _norm_string(rowd.get("Severity")),
            "description": _norm_string(rowd.get("AlarmDescription")),
            "correlation_id": "",
            "ip_address": _get_ip_from_row(rowd),
            "related_alarm": "",
            "all_rows": [rowd],
        }
        seen.add(key)
    return out
# ----- Create & remove ticket entries (with ACK events, no ticket_id in payload) -----
def _create_or_get_ticket(
    node: str,
    alarm_id: Any,
    alarm_type: str,
    severity: str,
    create_time: Any,   # not persisted; kept for compatibility with callers
    description: str = "",
    correlation_id: str = "",
    related_alarm: Optional[str] = None,
    ip_address: str = ""
) -> Dict[str, Any]:
    idx = _load_index()
    # Prefer correlation-based key when present
    key = _ticket_key_corr(correlation_id) if _norm_string(correlation_id) else _ticket_key(node, alarm_id, alarm_type)
    open_tickets = _load_open_tickets()

    if key in idx:
        tid = idx[key]
        if tid in open_tickets:
            row = dict(open_tickets[tid])  # base on stored row
            # Escalate severity if higher
            old = (row.get("severity") or "").strip().lower()
            new = (severity or "").strip().lower()
            if SEV_RANK.get(new, -1) > SEV_RANK.get(old, -1):
                row["severity"] = severity
                logging.info(f"[Daemon] Escalated TT {tid} severity {old} → {new}")
                _emit_event({
                    "event": "TT_ESCALATED",
                    "node": row.get("node") or node,
                    "alarm_id": row.get("alarm_id") or alarm_id,
                    "alarm_type": row.get("alarm_type") or alarm_type,
                    "old_severity": old,
                    "new_severity": new,
                    "correlation_id": row.get("correlation_id") or correlation_id,
                })
            # Enrich (no last_seen_at / ticket_id in persisted JSON)
            if description:
                row["description"] = description
            if correlation_id:
                row["correlation_id"] = correlation_id
            if related_alarm is not None:
                row["related_alarm"] = related_alarm  # string
            if ip_address:
                row["ip_address"] = ip_address

            open_tickets[tid] = _shape_tt_row(row)
            _save_open_tickets(open_tickets)
            return {"status": "existing", **open_tickets[tid]}

    # Create new (use correlation_id as internal ID when present; else a generated one)
    ticket_id = (_norm_string(correlation_id) or f"TT-{uuid.uuid4().hex[:8].upper()}")
    row = {
        "status": "OPEN",
        "node": node,
        "alarm_id": alarm_id,
        "alarm_type": alarm_type,
        "severity": severity,
        "description": description,
        "correlation_id": correlation_id,
        "related_alarm": related_alarm or "",
        "ip_address": ip_address,
    }
    open_tickets[ticket_id] = _shape_tt_row(row)
    idx[key] = ticket_id
    _save_open_tickets(open_tickets)
    _save_index(idx)

    logging.info(f"[Daemon] Created TT {ticket_id} for {node} • {alarm_type} • Sev={severity}")
    _emit_event({
        "event": "TT_CREATED",
        "node": node,
        "alarm_id": alarm_id,
        "alarm_type": alarm_type,
        "severity": severity,
        "correlation_id": correlation_id,
        "ip_address": ip_address,
    })
    return {"status": "created", **open_tickets[ticket_id]}

def _remove_ticket(node: str, alarm_id: Any, alarm_type: str) -> Optional[Dict[str, Any]]:
    """Remove from open tickets + index (legacy route), write audit log + ACK (no ticket_id)."""
    idx = _load_index()
    key_new = _ticket_key(node, alarm_id, alarm_type)
    key_old = _legacy_ticket_key(node, alarm_id, alarm_type)
    tid = idx.get(key_new) or idx.get(key_old)
    if not tid:
        return None
    open_tickets = _load_open_tickets()
    removed = open_tickets.pop(tid, None)
    if key_new in idx:
        del idx[key_new]
    if key_old in idx:
        del idx[key_old]
    _save_open_tickets(open_tickets)
    _save_index(idx)
    if removed:
        _append_closed_ticket(removed)
        logging.info(f"[Daemon] Removed TT {tid} (alarm cleared) for {removed.get('node')} • {removed.get('alarm_type')}")
        _emit_event({
            "event": "TT_CLOSED",
            "node": removed.get("node"),
            "alarm_id": removed.get("alarm_id"),
            "alarm_type": removed.get("alarm_type"),
            "severity": removed.get("severity"),
            "correlation_id": removed.get("correlation_id", ""),
        })
    else:
        ghost_row = {
            "correlation_id": "",
            "node": node,
            "status": "CLOSED",
            "alarm_id": alarm_id,
            "alarm_type": alarm_type,
            "severity": "",
            "description": "",
            "ip_address": "",
            "related_alarm": "",
            "note": "Index cleanup without open ticket (healed)",
        }
        _append_closed_ticket(ghost_row)
        logging.warning(f"[Daemon] Healed missing open ticket for legacy key; index removed.")
        _emit_event({
            "event": "TT_CLOSED",
            "node": node,
            "alarm_id": alarm_id,
            "alarm_type": alarm_type,
            "severity": "",
            "correlation_id": "",
            "note": "index_heal",
        })
        removed = ghost_row
    return removed

def _remove_ticket_by_key(key: str) -> Optional[Dict[str, Any]]:
    """Remove ticket using the exact index key (supports correlation keys)."""
    idx = _load_index()
    tid = idx.get(key)
    if not tid:
        return None
    open_tickets = _load_open_tickets()
    removed = open_tickets.pop(tid, None)
    if key in idx:
        del idx[key]
    _save_open_tickets(open_tickets)
    _save_index(idx)
    if removed:
        _append_closed_ticket(removed)
        logging.info(f"[Daemon] Removed TT {tid} (alarm cleared) for {removed.get('node')} • {removed.get('alarm_type')}")
        _emit_event({
            "event": "TT_CLOSED",
            "node": removed.get("node"),
            "alarm_id": removed.get("alarm_id"),
            "alarm_type": removed.get("alarm_type"),
            "severity": removed.get("severity"),
            "correlation_id": removed.get("correlation_id", ""),
        })
    return removed

# ----- Batch creation API -----
def create_tickets_for_nodes(nodes: list, severity_mode: str = "major") -> Dict[str, Any]:
    """Batch-create tickets for Active alarms on the given nodes only (grouped by correlation)."""
    nodes_norm = {_norm_string(n) for n in (nodes or []) if _norm_string(n)}
    if not nodes_norm:
        return {"created": [], "existing": [], "skipped": 0}

    df = _read_csv_with_retry(ALARMS_CSV_PATH)
    active_map = _active_alarm_groups(df)

    created: List[Dict[str, Any]] = []
    existing: List[Dict[str, Any]] = []
    skipped = 0
    created_keys_this_cycle: Set[str] = set(_load_index().keys())

    for key_new, payload in active_map.items():
        node_val = _norm_string(payload.get("node"))
        if node_val not in nodes_norm:
            continue
        sev = _norm_string(payload.get("severity"))
        if not _sev_ok_mode(sev, severity_mode):
            continue
        if key_new in created_keys_this_cycle:
            skipped += 1
            continue

        t = _create_or_get_ticket(
            node=node_val,
            alarm_id=_norm_string(payload.get("alarm_id")),
            alarm_type=_norm_string(payload.get("alarm_type")),
            severity=sev,
            create_time="",
            description=_norm_string(payload.get("description")),
            correlation_id=_norm_string(payload.get("correlation_id")),
            related_alarm=_norm_string(payload.get("related_alarm")),
            ip_address=_norm_string(payload.get("ip_address")),
        )
        created_keys_this_cycle.add(key_new)
        if t.get("status") == "created":
            created.append(t)
        else:
            existing.append(t)

    logging.info(
        f"[Daemon] Batch-create for nodes={sorted(nodes_norm)} • mode={severity_mode} • "
        f"created={len(created)} existing={len(existing)} skipped={skipped}"
    )
    return {"created": created, "existing": existing, "skipped": skipped}
# ----- Daemon loop -----
def start_fault_daemon() -> None:
    logging.info(f"[Daemon] Watching {ALARMS_CSV_PATH} every {POLL_INTERVAL_SEC:.0f}s")
    logging.info(f"[Daemon] Severity filter: {CREATE_TT_FOR_SEVERITY}")
    logging.info(f"[Daemon] Scope: {DAEMON_SCOPE} (targets: {CONV_TARGETS_FILE})")
    logging.info(f"[Daemon] Open: {OPEN_TICKETS_PATH} • Index: {TICKETS_INDEX_PATH} • Closed audit: {CLOSED_TICKETS_LOG}")

    last_mtime = 0.0
    while True:
        try:
            if not os.path.exists(ALARMS_CSV_PATH):
                logging.warning(f"[Daemon] CSV not found: {ALARMS_CSV_PATH}. Sleeping {POLL_INTERVAL_SEC}s.")
                time.sleep(POLL_INTERVAL_SEC)
                continue

            mtime = os.path.getmtime(ALARMS_CSV_PATH)
            if mtime <= last_mtime:
                time.sleep(POLL_INTERVAL_SEC)
                continue
            last_mtime = mtime

            df = _read_csv_with_retry(ALARMS_CSV_PATH)

            # 1) Build groups of Active alarms (corr-first)
            groups = _active_alarm_groups(df)

            # Conversation-scope filter
            conv_allowed = _load_conv_targets()
            restrict_to_conv = (DAEMON_SCOPE == "conversation_only")
            if restrict_to_conv and not conv_allowed:
                logging.info("[Daemon] No conversation targets; skipping creation this cycle (removals still processed).")
                conv_allowed = set()

            # Active keyset for close checks (include legacy per-alarm keys for all members to avoid false-closures)
            active_keyset: Set[str] = set(groups.keys())
            for key, payload in groups.items():
                for r in payload.get("all_rows", []):
                    node = _norm_string(r.get("NodeID"))
                    alarm_id = _norm_string(r.get("AlarmID"))
                    alarm_type = _alarm_type_from_row(r)
                    legacy_key = _legacy_ticket_key(node, alarm_id, alarm_type)
                    active_keyset.add(legacy_key)

            # 2) CREATE/UPDATE tickets for Active groups
            created_count = 0
            created_keys_this_cycle: Set[str] = set(_load_index().keys())
            for key_new, payload in groups.items():
                if key_new in created_keys_this_cycle:
                    continue
                node_val = _norm_string(payload.get("node"))
                if restrict_to_conv and node_val not in conv_allowed:
                    continue
                sev = _norm_string(payload.get("severity"))
                if not _sev_ok(sev):
                    continue

                # Adoption: if correlation key not present in index, but any legacy member exists, map it.
                if key_new not in created_keys_this_cycle:
                    idx = _load_index()
                    if key_new not in idx:
                        tid = _find_existing_tid_for_rows(payload.get("all_rows", []))
                        if tid:
                            idx[key_new] = tid
                            _save_index(idx)
                            open_tickets = _load_open_tickets()
                            if tid in open_tickets:
                                row = dict(open_tickets[tid])
                                row["description"] = payload.get("description", row.get("description", ""))
                                row["correlation_id"] = payload.get("correlation_id", row.get("correlation_id", ""))
                                row["related_alarm"] = payload.get("related_alarm", row.get("related_alarm", ""))
                                row["ip_address"] = payload.get("ip_address", row.get("ip_address", ""))
                                # Escalate if needed
                                old = (row.get("severity") or "").lower()
                                new = sev.lower()
                                if SEV_RANK.get(new, -1) > SEV_RANK.get(old, -1):
                                    row["severity"] = payload.get("severity")
                                    _emit_event({
                                        "event": "TT_ESCALATED",
                                        "node": row.get("node"),
                                        "alarm_id": row.get("alarm_id"),
                                        "alarm_type": row.get("alarm_type"),
                                        "old_severity": old,
                                        "new_severity": new,
                                        "correlation_id": row.get("correlation_id"),
                                        "ip_address": row.get("ip_address", ""),
                                    })
                                open_tickets[tid] = _shape_tt_row(row)
                                _save_open_tickets(open_tickets)
                            created_keys_this_cycle.add(key_new)
                            continue

                t = _create_or_get_ticket(
                    node=node_val,
                    alarm_id=_norm_string(payload.get("alarm_id")),
                    alarm_type=_norm_string(payload.get("alarm_type")),
                    severity=sev,
                    create_time="",  # not persisted
                    description=_norm_string(payload.get("description")),
                    correlation_id=_norm_string(payload.get("correlation_id")),
                    related_alarm=_norm_string(payload.get("related_alarm")),
                    ip_address=_norm_string(payload.get("ip_address")),
                )
                created_keys_this_cycle.add(key_new)
                if t.get("status") == "created":
                    created_count += 1

            if created_count:
                logging.info(f"[Daemon] Created {created_count} ticket(s) from Active alarms.")

            # 3) REMOVE tickets for alarms that are no longer Active
            idx = _load_index()
            open_keys = list(idx.keys())  # snapshot
            removed_count = 0
            for key in open_keys:
                if key not in active_keyset:
                    removed = _remove_ticket_by_key(key)
                    if removed:
                        removed_count += 1
            if removed_count:
                logging.info(f"[Daemon] Removed {removed_count} ticket(s) whose alarms are cleared/missing).")

        except Exception as e:
            logging.exception(f"[Daemon] Error: {e}")
        time.sleep(POLL_INTERVAL_SEC)

# ----- Public bridge for orchestrator -> daemon store -----
def create_ticket_external(
    node: str, alarm_id: Any, alarm_type: str, severity: str, create_time: Any,
    description: str = "", correlation_id: str = "", related_alarm: str = "", ip_address: str = ""
) -> Dict[str, Any]:
    """
    Orchestrator-safe wrapper that creates/returns a ticket in the same
    OPEN/INDEX stores used by the fault daemon.
    Returns {"status", ...shaped fields...} (no ticket_id).
    """
    return _create_or_get_ticket(
        node=node, alarm_id=alarm_id, alarm_type=alarm_type, severity=severity, create_time=create_time,
        description=description, correlation_id=correlation_id, related_alarm=related_alarm, ip_address=ip_address
    )

def close_ticket_external_by_key(index_key: str) -> Optional[Dict[str, Any]]:
    """
    Public, orchestrator-safe wrapper to close a ticket using an exact index key.
    Accepts both correlation keys ["corr", "<id>"] and per-alarm keys [node, alarm_id, alarm_type].
    Returns the removed row (shaped) or None if not found.
    """
    return _remove_ticket_by_key(index_key)

def close_ticket_external(node: str, alarm_id: Any, alarm_type: str) -> Optional[Dict[str, Any]]:
    """
    Public wrapper to close a ticket using legacy per-alarm uniqueness.
    """
    return _remove_ticket(node, alarm_id, alarm_type)

# ----- CLI -----
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fault TT daemon & tools (correlation-based).")
    parser.add_argument("-d", "--daemon", action="store_true", help="Run fault monitor daemon loop.")
    parser.add_argument("--poll", type=float, default=POLL_INTERVAL_SEC, help="Poll interval seconds (daemon).")
    parser.add_argument("--alarms-csv", default=ALARMS_CSV_PATH, help="Path to the alarms CSV.")
    parser.add_argument(
        "--severity",
        choices=["any", "major_plus", "critical"],
        default=CREATE_TT_FOR_SEVERITY,
        help="Daemon creation severity filter (still gated by DAEMON_SCOPE).",
    )
    # Utility commands
    parser.add_argument("--set-targets", nargs="*", help="Set conversation targets (nodes).")
    parser.add_argument("--ttl", type=int, default=CONV_TARGETS_TTL_SEC, help="TTL for conversation targets (sec).")
    parser.add_argument("--batch-create", action="store_true", help="Immediately batch-create for --set-targets.")
    parser.add_argument(
        "--severity-mode",
        choices=["major", "major_plus", "critical", "any"],
        default="major",
        help="Batch-create severity mode (default: major)",
    )
    args = parser.parse_args()

    # Keep env in sync if flags used
    os.environ["ALARMS_CSV_PATH"] = args.alarms_csv
    os.environ["POLL_INTERVAL_SEC"] = str(args.poll)
    os.environ["CREATE_TT_FOR_SEVERITY"] = args.severity

    # Utility path: set targets and optionally batch-create now
    if args.set_targets is not None:
        payload = set_conversation_targets(args.set_targets, ttl_sec=args.ttl)
        if args.batch_create:
            res = create_tickets_for_nodes(args.set_targets, severity_mode=args.severity_mode)
            print(json.dumps({"ok": True, "targets": payload, "result": res}, indent=2, ensure_ascii=False))
        else:
            print(json.dumps({"ok": True, "targets": payload}, indent=2, ensure_ascii=False))
        sys.exit(0)

    # Daemon path
    if args.daemon:
        try:
            start_fault_daemon()
        except KeyboardInterrupt:
            print("\n[Daemon] Stopped by user.")
            sys.exit(0)

    # Default: print current settings
    print(json.dumps({
        "daemon_scope": DAEMON_SCOPE,
        "conv_targets_file": CONV_TARGETS_FILE,
        "create_tt_for_severity": CREATE_TT_FOR_SEVERITY,
        "alarms_csv": ALARMS_CSV_PATH,
        "events_log": TT_EVENTS_LOG,
    }, indent=2, ensure_ascii=False))
