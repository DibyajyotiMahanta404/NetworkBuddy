# main.py — Orchestrator with Major-only ticket prompt + consent-based TT creation
# Compatible with your tt_agent.py daemon stores
# --------------------------------------------------------------------------------
import os
import re
import json
import time
import uuid
import logging
import sys
from typing import TypedDict, List, Dict, Any, Optional, Tuple
import pandas as pd
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
from langgraph.graph import StateGraph, END
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.chat_history import InMemoryChatMessageHistory

# Project tools
from tools.alarm_loader import load_alarms_csv, nodes_from_alarms

# --- progress sinks for WS streaming ---
from typing import Callable, Awaitable

def register_progress_sink(request_id: str, sink: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
    _PROGRESS_SINKS[request_id] = sink
    emit_flow_event("ws_sink_registered", request_id=request_id)

def unregister_progress_sink(request_id: str) -> None:
    _PROGRESS_SINKS.pop(request_id, None)
    emit_flow_event("ws_sink_unregistered", request_id=request_id)

_PROGRESS_SINKS: Dict[str, Callable[[Dict[str, Any]], Awaitable[None]]] = {}

# ---------------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------------
LOG_LEVEL = os.getenv("ORCH_LOG_LEVEL", "INFO").strip().upper()
logger = logging.getLogger("orchestrator")
if not logger.handlers:
    _h = logging.StreamHandler(stream=sys.stdout)
    _h.setFormatter(logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [Orchestrator] %(message)s",
        datefmt="%H:%M:%S"
    ))
    logger.addHandler(_h)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# --- JSONL file logging for live streaming over /ws/logs/orchestrator ---
import json as _json
from logging.handlers import RotatingFileHandler as _RotatingFileHandler
ORCH_LOG_JSON = os.getenv("ORCH_LOG_JSON", "data/orchestrator.log.jsonl")

class _JsonlFormatter(logging.Formatter):
    def format(self, record):
        base = {
            "ts": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": "orchestrator",
            "message": record.getMessage(),
            # Optional context fields (used for filtering in WS)
            "session_id": getattr(record, "session_id", None),
            "request_id": getattr(record, "request_id", None),
        }
        # If message is a JSON object containing 'event', merge it into base
        try:
            if isinstance(record.msg, str):
                s = record.msg.strip()
                if s.startswith("{") and s.endswith("}"):
                    obj = _json.loads(s)
                    if isinstance(obj, dict) and "event" in obj:
                        base.update(obj)
        except Exception:
            pass
        return _json.dumps(base, ensure_ascii=False)

try:
    os.makedirs(os.path.dirname(ORCH_LOG_JSON) or ".", exist_ok=True)
    _jh = _RotatingFileHandler(ORCH_LOG_JSON, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    _jh.setFormatter(_JsonlFormatter())
    logger.addHandler(_jh)
except Exception:
    # Fail open: if file handler can't be added, console logs still work
    pass

# ----------------- ADD: context + event emitter + redaction -----------------
import contextvars
cv_session_id = contextvars.ContextVar("cv_session_id", default=None)
cv_request_id = contextvars.ContextVar("cv_request_id", default=None)

class _ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Attach context to every record automatically (used by JSON formatter too)
        setattr(record, "session_id", cv_session_id.get())
        setattr(record, "request_id", cv_request_id.get())
        return True

logger.addFilter(_ContextFilter())

ORCH_LOG_EVENT_LEVEL = os.getenv("ORCH_LOG_EVENT_LEVEL", "INFO").strip().upper()
_ORCH_EVENT_LEVEL = getattr(logging, ORCH_LOG_EVENT_LEVEL, logging.INFO)

# Redaction helpers (basic examples; extend as needed)
_REDACT_KEYS = {"ip_address", "hostname"}
def redact(obj: Any) -> Any:
    """Shallow redaction of known sensitive keys in dictionaries / lists."""
    try:
        if isinstance(obj, dict):
            return {k: ("***" if k in _REDACT_KEYS else redact(v)) for k, v in obj.items()}
        if isinstance(obj, list):
            return [redact(v) for v in obj]
        return obj
    except Exception:
        return obj

def _emit_ws(request_id: str, payload: Dict[str, Any]) -> None:
    """Fire-and-forget WS progress if a sink is registered for this request_id."""
    try:
        import asyncio
        sink = _PROGRESS_SINKS.get(request_id)
        if sink:
            asyncio.create_task(sink(payload))
    except Exception:
        pass

def emit_flow_event(event: str, /, **fields: Any) -> None:
    """
    Unified, typed flow events → JSONL + optional WS.
    Example: emit_flow_event('agent_started', agent='fault_agent')
    """
    try:
        payload = {
            "event": event,
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "session_id": cv_session_id.get(),
            "request_id": cv_request_id.get(),
            **fields,
        }
        # Log structured payload (merged by _JsonlFormatter)
        logger.log(_ORCH_EVENT_LEVEL, _json.dumps(payload, ensure_ascii=False))
        # Mirror to WS for the active request
        rid = cv_request_id.get()
        if rid:
            _emit_ws(rid, payload)
    except Exception:
        # Never break flow because of logging
        pass


# ---------------------------------------
# Env & LLM
# ---------------------------------------
load_dotenv()
llm = AzureChatOpenAI(
    deployment_name=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini"),
    temperature=0.0,
)

# ---------------------------------------
# Chat history (LangChain)
# ---------------------------------------
CHAT_HISTORY_TRACES = int(os.getenv("CHAT_HISTORY_TRACES", "5"))
_SESSION_STORES: Dict[str, InMemoryChatMessageHistory] = {}


def _prune_session_history(history: InMemoryChatMessageHistory, last_traces: int = CHAT_HISTORY_TRACES) -> None:
    try:
        max_msgs = max(0, last_traces * 2)
        if len(history.messages) > max_msgs:
            history.messages[:] = history.messages[-max_msgs:]
    except Exception:
        while len(history.messages) > last_traces * 2:
            del history.messages[0]


def chat_history(session_id: str) -> InMemoryChatMessageHistory:
    hist = _SESSION_STORES.get(session_id)
    if hist is None:
        hist = InMemoryChatMessageHistory()
        _SESSION_STORES[session_id] = hist
    _prune_session_history(hist, CHAT_HISTORY_TRACES)
    return hist


def reset_chat_history(session_id: str) -> None:
    _SESSION_STORES.pop(session_id, None)


def _invoke_with_history(session_id: str, input_text: str, system_preamble: str) -> str:
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_preamble),
        MessagesPlaceholder("history"),
        ("human", "{input}"),
    ])
    chain = prompt | llm
    with_hist = RunnableWithMessageHistory(
        chain,
        chat_history,
        input_messages_key="input",
        history_messages_key="history",
    )
    try:
        resp = with_hist.invoke({"input": input_text}, config={"configurable": {"session_id": session_id}})
        return (getattr(resp, "content", None) or str(resp)).strip()
    except Exception:
        return ""


def _get_recent_human_texts(session_id: str, max_msgs: int = 10) -> List[str]:
    try:
        hist = chat_history(session_id)
        texts = [m.content for m in hist.messages if getattr(m, "type", "") == "human"]
        return texts[-max_msgs:][::-1]
    except Exception:
        return []

# ---------------------------------------
# Feature flags
# ---------------------------------------
def _env_flag(name: str, default: bool = False) -> bool:
    val = os.getenv(name, str(default)).strip().lower()
    return val in {"1", "true", "t", "yes", "y", "on"}


USE_LLM_SUMMARY = _env_flag("USE_LLM_SUMMARY", True)
COMPACT_YESNO = _env_flag("COMPACT_YESNO", False)
OVERALL_RESPONSE = _env_flag("OVERALL_RESPONSE", True)
RESPONSE_STYLE = os.getenv("RESPONSE_STYLE", "concise").strip().lower()  # concise|sections|auto
STRICT_INTENT_OUTPUTS = _env_flag("STRICT_INTENT_OUTPUTS", True)

# --- New policy flags ---
# Prompt user to create TT for which severities? (affects proposal + consent creation)
# major_only (default) | major_plus | critical_plus
ORCH_TT_PROMPT_SEVERITY = os.getenv("ORCH_TT_PROMPT_SEVERITY", "major_only").strip().lower()

# Show summarized Orchestrator Flow (LLM) from agent trace
SHOW_FLOW_ANALYSIS = _env_flag("SHOW_FLOW_ANALYSIS", False)

# ---------------------------------------
# Utilities
# ---------------------------------------
import datetime as dt
from datetime import datetime, timezone

SEV_RANK_ORDER = {"critical": 3, "major": 2, "minor": 1, "warning": 0}


def _to_epoch(ct) -> float:
    """Return epoch seconds for many timestamp types (pandas/NumPy/strings/py-datetime).
    Safely handles pandas NaT/Timestamp('NaT') by returning 0.0.
    """
    if ct is None:
        return 0.0
    # --- pandas-aware branch ---
    try:
        import pandas as _pd  # type: ignore
        # Catch NaT (works for pd.NaT, Timestamp('NaT'), etc.)
        try:
            if hasattr(_pd, "isna") and _pd.isna(ct):
                return 0.0
        except Exception:
            pass
        if isinstance(ct, _pd.Timestamp):
            # Some Timestamp('NaT') still reach here — guard again
            try:
                if getattr(ct, "isnat", False):
                    return 0.0
            except Exception:
                pass
            try:
                d = ct.to_pydatetime()
                if str(d).lower() == "nat":
                    return 0.0
                if getattr(d, "tzinfo", None) is None:
                    from datetime import timezone as _tz
                    d = d.replace(tzinfo=_tz.utc)
                return d.timestamp()
            except Exception:
                return 0.0
    except Exception:
        pass

    # --- numpy datetime64 branch ---
    try:
        import numpy as _np  # type: ignore
        if isinstance(ct, getattr(_np, "datetime64", ())):
            try:
                import pandas as _pd  # reuse pandas for parsing numpy ts
                ts = _pd.Timestamp(ct)
                if hasattr(_pd, "isna") and _pd.isna(ts):
                    return 0.0
                d = ts.to_pydatetime()
                if str(d).lower() == "nat":
                    return 0.0
                if getattr(d, "tzinfo", None) is None:
                    from datetime import timezone as _tz
                    d = d.replace(tzinfo=_tz.utc)
                return d.timestamp()
            except Exception:
                return 0.0
    except Exception:
        pass

    # --- plain datetime ---
    from datetime import datetime as _dt, timezone as _tz
    if isinstance(ct, _dt):
        d = ct if ct.tzinfo else ct.replace(tzinfo=_tz.utc)
        try:
            return d.timestamp()
        except Exception:
            return 0.0

    # --- numeric epoch / ms ---
    if isinstance(ct, (int, float)):
        try:
            # If it's likely ms, convert to seconds
            return float(ct) / 1000.0 if ct > 1e12 else float(ct)
        except Exception:
            return 0.0

    # --- string parsing ---
    if isinstance(ct, str):
        s = ct.strip()
        if not s:
            return 0.0
        # ISO 'Z' → explicit offset
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        for parser in (
            lambda x: _dt.fromisoformat(x),
            lambda x: _dt.strptime(x, "%Y-%m-%d %H:%M:%S"),
            lambda x: _dt.strptime(x, "%d-%m-%Y %H:%M:%S"),
            lambda x: _dt.strptime(x, "%Y/%m/%d %H:%M:%S"),
        ):
            try:
                d = parser(s)
                if d.tzinfo is None:
                    d = d.replace(tzinfo=_tz.utc)
                return d.timestamp()
            except Exception:
                continue
        # Fallback
        return 0.0


def save_nodes(nodes: List[Dict[str, Any]], path: str = os.getenv("NODES_JSON_PATH", "data/nodes.json")) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(nodes, f, indent=2, ensure_ascii=False, default=str)
    except Exception:
        pass
def _wants_alarms(text: str) -> bool:
    t = (text or "").lower()
    return bool(re.search(r"\b(alarm|alarms|alaram|alarams|fault|faults|alert|alerts|incident|incidents)\b", t))


def _is_active_alarm_dict(a: Dict[str, Any]) -> bool:
    status = str(a.get("AlarmStatus") or "").strip().lower()
    if status == "active":
        return True
    if status == "cleared":
        return False
    # defensive
    end = a.get("EndTime")
    try:
        import pandas as _pd
        return _pd.isna(end)
    except Exception:
        return not bool(str(end or "").strip())


def _sev_rank(a: Dict[str, Any]) -> int:
    return SEV_RANK_ORDER.get(str(a.get("Severity") or "").strip().lower(), -1)

# ---------------------------------------
# Formatting helpers
# ---------------------------------------
# --- US States & Regions helpers (normalize & bucket) ---

STATE_ABBR_TO_NAME = {
    "AL":"Alabama","AK":"Alaska","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut",
    "DE":"Delaware","FL":"Florida","GA":"Georgia","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa",
    "KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MA":"Massachusetts","MI":"Michigan",
    "MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire",
    "NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","OH":"Ohio","OK":"Oklahoma",
    "OR":"Oregon","PA":"Pennsylvania","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee",
    "TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming",
    "DC":"District of Columbia"
}
STATE_NAME_TO_ABBR = {v: k for k, v in STATE_ABBR_TO_NAME.items()}

# A simple, commonly used regional bucketing (Census-style-ish)
US_REGION_BY_STATE = {
    # West
    "Alaska":"West","Arizona":"West","California":"West","Colorado":"West","Hawaii":"West","Idaho":"West","Montana":"West",
    "Nevada":"West","New Mexico":"West","Oregon":"West","Utah":"West","Washington":"West","Wyoming":"West",
    # Midwest
    "Illinois":"Midwest","Indiana":"Midwest","Iowa":"Midwest","Kansas":"Midwest","Michigan":"Midwest","Minnesota":"Midwest",
    "Missouri":"Midwest","Nebraska":"Midwest","North Dakota":"Midwest","Ohio":"Midwest","South Dakota":"Midwest","Wisconsin":"Midwest",
    # South
    "Alabama":"South","Arkansas":"South","Delaware":"South","District of Columbia":"South","Florida":"South","Georgia":"South",
    "Kentucky":"South","Louisiana":"South","Maryland":"South","Mississippi":"South","North Carolina":"South",
    "Oklahoma":"South","South Carolina":"South","Tennessee":"South","Texas":"South","Virginia":"South","West Virginia":"South",
    # Northeast
    "Connecticut":"Northeast","Maine":"Northeast","Massachusetts":"Northeast","New Hampshire":"Northeast","New Jersey":"Northeast",
    "New York":"Northeast","Pennsylvania":"Northeast","Rhode Island":"Northeast","Vermont":"Northeast",
}

def _norm_str(x: Any) -> str:
    return (str(x or "")).strip()

def _lc(x: Any) -> str:
    return _norm_str(x).lower()

def _normalize_state_all_tokens(s: str) -> set:
    """Return a set of normalized tokens for a state string: full name and abbr (both lowercased)."""
    s = _norm_str(s)
    if not s:
        return set()
    tokens = { _lc(s) }
    # If abbreviation, add full name
    if len(s) == 2 and s.upper() in STATE_ABBR_TO_NAME:
        tokens.add(_lc(STATE_ABBR_TO_NAME[s.upper()]))
    # If full name, add abbreviation
    if s.title() in STATE_NAME_TO_ABBR:
        tokens.add(_lc(STATE_NAME_TO_ABBR[s.title()]))
    return tokens

def _state_from_site_location(site_loc: str) -> Optional[str]:
    """
    Try to extract US state abbreviation from free-text site_location, e.g.
    '57700 Scott Ville, Brayview, MI 44427' → 'MI'
    """
    txt = _norm_str(site_loc)
    # Look for ', XX ' pattern
    m = re.search(r",\s*([A-Z]{2})\s+\d{3,5}\b", txt)
    if m and m.group(1) in STATE_ABBR_TO_NAME:
        return m.group(1)
    # fallback: any ' XX ' token near a 5-digit zip
    m = re.search(r"\b([A-Z]{2})\b\s+\d{3,5}\b", txt)
    if m and m.group(1) in STATE_ABBR_TO_NAME:
        return m.group(1)
    return None

def _states_for_region(region_name: str) -> set:
    """Return lowercase full names + abbrs for all states in the given region bucket."""
    r = (_norm_str(region_name).title())
    states = [s for s, reg in US_REGION_BY_STATE.items() if reg == r]
    toks: set = set()
    for s in states:
        toks.add(_lc(s))
        ab = STATE_NAME_TO_ABBR.get(s)
        if ab:
            toks.add(_lc(ab))
    return toks

def format_active_alarms(alarms: List[Dict[str, Any]], limit: int = 100, only_nodes: Optional[set] = None) -> str:
    tokens = {(x or "").strip().lower() for x in (only_nodes or set())}
    active = []
    for a in alarms:
        if not _is_active_alarm_dict(a):
            continue
        node = (a.get("NodeID") or "").strip().lower()
        if tokens and not any(tok in node for tok in tokens):
            continue
        active.append(a)
    active.sort(key=lambda a: (_sev_rank(a), _to_epoch(a.get("CreateTime"))), reverse=True)
    lines = []
    for a in active[:limit]:
        node = a.get("NodeID") or "UnknownNode"
        sev = a.get("Severity", "")
        atype = a.get("AlarmType", "")
        ctime = a.get("CreateTime", "")
        aid = a.get("AlarmID", "")
        status = a.get("AlarmStatus", "")
        desc = a.get("AlarmDescription", "")
        lines.append(f"- [{sev}] {node} • {atype} • {ctime} • ID={aid} • Status={status}" + (f" • {desc}" if desc else ""))
    header = f"Active alarms: {len(active)}"
    if active:
        header += f" (showing top {min(limit, len(active))})"
    return header + (("\n" + "\n".join(lines)) if lines else "")

# ---------------------------------------
# Customer churn utilities (ported & aligned with enhanced_churn_api.py)
# ---------------------------------------
CUSTOMERS_CSV_PATH = os.getenv("CUSTOMERS_CSV_PATH", "data/mobile_customers_usa_by_region.csv")


def load_customers_csv_local(path: str) -> Optional[pd.DataFrame]:
    """Load customers CSV and normalize required columns."""
    try:
        df = pd.read_csv(path)

        # Normalize to predictable names; keep PascalCase for downstream usage
        import re as _re
        df.columns = [_re.sub(r"[^\w]+", "_", str(c).strip()).lower() for c in df.columns]
        rename_map = {
            "customerid": "CustomerID",
            "region": "Region",
            "state": "State",
            "city": "City",
            "age": "Age",
            "gender": "Gender",
            "plantype": "PlanType",
            "networktype": "NetworkType",
            "tenuremonths": "TenureMonths",
            "arpu_inr": "ARPU_INR",
            "complaintslast90days": "ComplaintsLast90Days",
            "nps": "NPS",
        }
        out = {}
        for c in df.columns:
            out[rename_map.get(c, c)] = df[c]
        z = pd.DataFrame(out)

        # sanity defaults
        if "ComplaintsLast90Days" not in z.columns:
            z["ComplaintsLast90Days"] = 0
        if "NPS" not in z.columns:
            z["NPS"] = 0

        required = ["CustomerID", "Region", "TenureMonths", "ARPU_INR"]
        for r in required:
            if r not in z.columns:
                raise ValueError(f"Customers CSV missing required column: {r}")

        return z
    except Exception as e:
        logger.warning(f"[customers] failed to load '{path}': {e}")
        return None


def _det_rand_seed(key: str) -> int:
    return hash(key) % (2**32)


def enhance_customer_df(df_in: pd.DataFrame) -> pd.DataFrame:
    """Deterministic satisfaction & network features, churn prob, risk level, CLV, display name."""
    if df_in is None or df_in.empty:
        return pd.DataFrame()

    df = df_in.copy()

    # Deterministic per-customer synthetic fields
    def _sat(cid: str) -> int:
        import numpy as _np
        _np.random.seed(_det_rand_seed(f"{cid}_satisfaction"))
        return int(_np.random.choice(range(1, 11),
                      p=[0.05,0.05,0.10,0.10,0.15,0.15,0.15,0.15,0.05,0.05]))
    def _cdr(cid: str) -> float:
        import numpy as _np
        _np.random.seed(_det_rand_seed(f"{cid}_network"))
        return float(_np.random.uniform(0.5, 8.0))

    df["satisfaction_score"] = df["CustomerID"].apply(_sat)
    df["call_drop_rate"] = df["CustomerID"].apply(_cdr)

    # Friendly display name
    def _cname(cid: str) -> str:
        try:
            suffix = str(cid).replace("CUST", "").lstrip("0")
            return f"Customer_{suffix or '0'}"
        except Exception:
            return f"Customer_{cid}"
    df["customer_name"] = df["CustomerID"].apply(_cname)

    # Churn probability aligned with enhanced_churn_api.py
    def _churn_prob(row) -> float:
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

    df["churn_probability"] = df.apply(_churn_prob, axis=1)

    def _risk(p: float) -> str:
        if p >= 0.7: return "Critical"
        if p >= 0.5: return "High"
        if p >= 0.3: return "Medium"
        return "Low"
    df["risk_level"] = df["churn_probability"].apply(_risk)

    # Simplified CLV
    def _safe_float(x):
        try: return float(x)
        except Exception: return 0.0
    df["customer_lifetime_value"] = (
        df["ARPU_INR"].apply(_safe_float)
        * df["TenureMonths"].apply(_safe_float)
        * (1.0 - df["churn_probability"])
    )
    return df


def _customer_generate_analysis(row: pd.Series) -> str:
    churn_prob = float(row["churn_probability"]) * 100.0
    analysis = (
        f"Based on our analysis of {row['customer_name']}, we've identified a "
        f"{row['risk_level'].lower()} churn risk with a probability of {churn_prob:.1f}%.\n\n"
        "Key factors contributing to this assessment:\n"
        f"- Customer satisfaction score: {int(row['satisfaction_score'])}/10\n"
        f"- Tenure with us: {int(row['TenureMonths'])} months\n"
        f"- Recent complaints: {int(row.get('ComplaintsLast90Days', 0))}\n"
        f"- Network quality (call drop rate): {float(row['call_drop_rate']):.1f}%\n\n"
        f"The customer is currently on a {str(row.get('PlanType','')).lower()} plan worth "
        f"${float(row['ARPU_INR']):.0f}/month.\n\n"
    )
    if churn_prob >= 70:
        analysis += ("Our recommendation is to immediately engage with this customer through our premium retention "
                     "team to address their concerns and offer targeted retention strategies.")
    elif churn_prob >= 50:
        analysis += ("Our recommendation is to proactively engage with this customer to address their concerns and "
                     "offer targeted retention strategies.")
    elif churn_prob >= 30:
        analysis += ("Our recommendation is to monitor this customer closely and consider proactive engagement to "
                     "maintain satisfaction.")
    else:
        analysis += ("This customer shows good retention indicators. Continue providing excellent service to maintain "
                     "their loyalty.")
    return analysis


def _customer_pick_target(df: pd.DataFrame, customer_id: Optional[str], region: Optional[str]) -> pd.Series:
    if df is None or df.empty:
        raise ValueError("No customer data available.")
    if customer_id:
        sub = df[df["CustomerID"] == customer_id]
        if sub.empty:
            raise ValueError(f"Customer {customer_id} not found.")
        return sub.iloc[0]
    if region:
        sub = df[df["Region"] == region]
        if sub.empty:
            raise ValueError(f"No customers found in region {region}.")
        return sub.loc[sub["churn_probability"].idxmax()]
    hi = df[df["churn_probability"] >= 0.5]
    if not hi.empty:
        return hi.sample(n=1, random_state=42).iloc[0]
    return df.loc[df["churn_probability"].idxmax()]


def _customer_high_risk_table(df: pd.DataFrame, limit: int = 20) -> str:
    if df is None or df.empty:
        return "No customer data available."
    hi = (df[df["churn_probability"] >= 0.5]
          .sort_values("churn_probability", ascending=False)
          .head(limit))
    if hi.empty:
        return "No high-risk customers at the moment."
    lines = []
    for _, r in hi.iterrows():
        lines.append(
            f"- {r['CustomerID']} ({r['customer_name']}) • {r['Region']} • "
            f"Churn={r['churn_probability']*100:.1f}% [{r['risk_level']}] • "
            f"Sat={int(r['satisfaction_score'])}/10 • Tenure={int(r['TenureMonths'])}m • "
            f"ARPU=${float(r['ARPU_INR']):.0f}/mo • Complaints={int(r.get('ComplaintsLast90Days',0))}"
        )
    return f"High-risk customers: {len(hi)} (showing top {min(limit,len(hi))})\n" + "\n".join(lines)
# ---------------------------------------
# Agent State
# ---------------------------------------
# --- NEW: Signals passed back by agents for routing decisions ---
class Signals(TypedDict, total=False):
    fault: Dict[str, Any]
    performance: Dict[str, Any]
    inventory: Dict[str, Any]
    data_flow: Dict[str, Any]
    ticketing: Dict[str, Any]

# AgentState ...
class AgentState(TypedDict, total=False):
    # ... existing fields ...
    signals: Signals               # <--- NEW
    flow_decisions: List[Dict[str, Any]]  # <--- NEW (audit trail for dynamic planning)
    user_input: str
    intent: str
    result: str
    nodes: List[Dict[str, Any]]
    alarms: List[Dict[str, Any]]
    performance_records: List[Dict[str, Any]]
    inventory_records: List[Dict[str, Any]]
    target_nodes: List[str]
    fault_summary: str
    performance_summary: str
    inventory_summary: str
    data_flow_summary: str
    customer_summary: str
    alerts: List[str]
    inventory_actions: List[str]
    data_flow_actions: List[str]
    direct_answer: str
    yesno_alarm_query: bool
    events: List[Dict[str, Any]]
    tickets: List[Dict[str, Any]]
    ticketing_summary: str
    session_id: str
    # New: for flow analysis/abstraction
    _agent_trace: List[Dict[str, Any]]
    flow_analysis: str

    # --- NEW: Customer fields ---
    customer_records: List[Dict[str, Any]]
    customer_id: str
    customer_region: str
    customer_high_risk: bool
    customer_limit: int
    customer_needs_enrichment: bool


# Pending consent proposals per session
PENDING_TICKET_REQUESTS: Dict[str, Dict[str, Any]] = {}

# ---------------------------------------
# Target extraction helpers
# ---------------------------------------
# --- Customer→Inventory mapping helper (NEW) ---
from typing import Tuple, List, Dict, Any

# --- Customer→Inventory mapping helper (STRICT, no site_location) ---
def _resolve_targets_from_customer_context(state: AgentState) -> Tuple[List[str], str]:
    """
    Map a customer's context (customer_id or region/state) to network node targets
    using inventory records with STRICT priority and NO site_location matching:
      1) exact state match (customer's explicit state if present, else any state in customer's region),
      2) region fallback ONLY if the inventory row's state is consistent with that region.

    Returns (targets, reason).
    """
    cust_id = (state.get("customer_id") or "").strip()
    cust_region = (state.get("customer_region") or "").strip()

    customer_records: List[Dict[str, Any]] = state.get("customer_records") or []
    inv_records: List[Dict[str, Any]] = state.get("inventory_records") or []
    nodes: List[Dict[str, Any]] = state.get("nodes") or []

    # Limit results to nodes that exist in the orchestrator's node list (preserve display case)
    node_lower_to_disp = {
        (n.get("name") or "").strip().lower(): (n.get("name") or "").strip()
        for n in nodes if (n.get("name") or "").strip()
    }

    # Derive customer region/state from the customer row if a specific CustomerID was given
    region_name: Optional[str] = None
    customer_state_tokens: set = set()

    if cust_id and customer_records:
        for r in customer_records:
            if str(r.get("CustomerID") or "").strip().upper() == cust_id:
                region_name = (r.get("Region") or "").strip()
                st = (r.get("State") or "").strip()
                if st:
                    # Adds both abbreviation and full state name in lowercase
                    customer_state_tokens |= _normalize_state_all_tokens(st)
                break

    # If user explicitly passed a region, prefer it when we don't already have one from the record
    if cust_region and not region_name:
        region_name = cust_region.strip()

    # Tokens for all states in the customer region (if any)
    region_tokens: set = _states_for_region(region_name) if region_name else set()

    # Allowed tokens for exact state match:
    # - If customer's explicit state is known, use ONLY that (customer_state_tokens);
    # - else, use the full set of states for the customer's region.
    allowed_tokens: set = customer_state_tokens or region_tokens

    # Consistency check for region fallback: if inventory has a state, it must belong to the customer's region
    def _is_state_consistent_with_region(inv_state: str) -> bool:
        if not inv_state:
            return True  # no state to contradict region
        inv_state_tokens = _normalize_state_all_tokens(inv_state)
        return bool(inv_state_tokens & region_tokens) if region_tokens else True

    matched_nodes: set = set()

    for rec in inv_records:
        node = (rec.get("node") or rec.get("Nodename") or "").strip()
        if not node:
            continue

        inv_region = (rec.get("region") or rec.get("Region") or "").strip()
        inv_state  = (rec.get("state") or  rec.get("State")  or "").strip()

        ok_reason: Optional[str] = None

        # 1) Exact state match (highest priority)
        if allowed_tokens and inv_state:
            inv_state_tokens = _normalize_state_all_tokens(inv_state)
            if inv_state_tokens & allowed_tokens:
                ok_reason = f"state={inv_state}"

        # 2) Region fallback (only if state is consistent with that region)
        if not ok_reason and region_name and inv_region and inv_region.title() == region_name.title():
            if _is_state_consistent_with_region(inv_state):
                ok_reason = f"region={inv_region}"

        if ok_reason:
            disp = node_lower_to_disp.get(node.lower(), node)
            matched_nodes.add(disp)

    if matched_nodes:
        reason = f"mapped (strict/no-location) from {'customer ' + cust_id if cust_id else 'region ' + (region_name or '')}".strip()
        return sorted(matched_nodes), reason

    return [], "no mapping found (strict/no-location)"
# --- end helper ---

def _tokenize_node_mentions(text: str) -> List[str]:
    text = (text or "")
    pats = [
        r"\b[eE][nN][oO][dD][eE][bB][\-\_]?[\d]+\b",
        r"\b[Ss]ite[\-\_]?[\d]+\b",
        r"\b[Nn]ode[\-\_]?[\d]+\b",
        r"\b[A-Za-z]+[\-\_][0-9]+\b",
    ]
    out = set()
    for p in pats:
        out.update(re.findall(p, text))
    return list(out)


def _lower_name_set(names: Optional[List[str]]) -> set:
    return {(n or "").strip().lower() for n in (names or []) if (n or "").strip()}


def _contains_any(hay: str, needles_lower: set) -> bool:
    h = (hay or "").strip().lower()
    if not h or not needles_lower:
        return False
    return any(n in h for n in needles_lower)


def _filter_nodes_by_names(nodes: List[Dict[str, Any]], names_lower: set) -> List[Dict[str, Any]]:
    if not names_lower:
        return nodes
    out = []
    for n in nodes:
        name = (n.get("name") or "").strip()
        if _contains_any(name, names_lower):
            out.append(n)
    return out

# ---------------------------------------
# Tickets store readers (daemon-compatible)
# ---------------------------------------
TICKETS_OPEN_PATH = os.getenv("TICKETS_OPEN_PATH", "data/tickets_open.json")
TICKETS_CLOSED_PATH = os.getenv("TICKETS_CLOSED_PATH", "data/tickets_closed.jsonl")
_TT_ID_RE = re.compile(r"\bTT-[A-F0-9]{8}\b", re.I)


def _safe_load_json(path: str, default: Any):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception:
        return default


def _safe_tail_jsonl(path: str, max_lines: int = 2000) -> List[Dict[str, Any]]:
    from collections import deque
    items: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            dq = deque(f, maxlen=max_lines)
            for line in dq:
                s = line.strip()
                if not s:
                    continue
                try:
                    items.append(json.loads(s))
                except Exception:
                    continue
    except FileNotFoundError:
        pass
    except Exception:
        pass
    return items


def _load_open_tickets() -> List[Dict[str, Any]]:
    d = _safe_load_json(TICKETS_OPEN_PATH, {})
    rows: List[Dict[str, Any]] = []
    for key, t in (d or {}).items():
        row = dict(t or {})
        # Ensure status
        row["status"] = row.get("status") or "OPEN"
        # If correlation_id is blank but the dict key looks like a real id (not legacy TT-*)
        if not (row.get("correlation_id") or "").strip():
            if key and not str(key).startswith("TT-"):
                row["correlation_id"] = str(key).strip()
        rows.append(row)

    # Sort (desc) by severity rank, then node asc. No time fields in new schema.
    def _rank(v: str) -> int:
        return SEV_RANK_ORDER.get((v or "").strip().lower(), -1)
    rows.sort(key=lambda r: (_rank(r.get("severity")), (r.get("node") or "")), reverse=True)
    return rows



def _load_closed_tickets(tail: int = 2000, within_hours: Optional[int] = None) -> List[Dict[str, Any]]:
    rows = _safe_tail_jsonl(TICKETS_CLOSED_PATH, max_lines=tail)
    if within_hours:
        now = dt.datetime.now(dt.timezone.utc).timestamp()
        rows = [r for r in rows if (now - _to_epoch(r.get("closed_at"))) <= within_hours * 3600]
    rows.sort(key=lambda x: _to_epoch(x.get("closed_at") or x.get("last_seen_at") or x.get("created_at")), reverse=True)
    return rows


def _parse_ticket_query(text: str) -> Dict[str, Any]:
    t = (text or "").strip()
    tl = t.lower()
    out = {
        "want": "none",
        "corr_id": None,         # NEW: correlation_id target
        "ticket_id": None,       # legacy support
        "recent_hours": None,
        "severity": None,
        "alarm_type": None,
    }

    # NEW: correlation id mentions (corr=, correlation_id:, cid=)
    m = re.search(r"\b(?:corr|correlation_id|cid)\s*[:=]\s*([A-Za-z0-9._\-]+)\b", t, re.I)
    if m:
        out["corr_id"] = m.group(1).strip()
        out["want"] = "detail"
        return out

    # Legacy TT-XXXX id support (kept for backward compat)
    m = _TT_ID_RE.search(t)
    if m:
        out["ticket_id"] = m.group(0).upper()
        out["want"] = "detail"
        return out

    # Generic ticket queries
    if "ticket" in tl or "tt " in tl or " tt-" in tl:
        if "open" in tl:
            out["want"] = "open"
        elif ("close" in tl) or ("closed" in tl) or ("resolved" in tl):
            out["want"] = "closed"
        elif ("index" in tl) or ("mapping" in tl):
            out["want"] = "index"
        else:
            out["want"] = "open"

    m = re.search(r"(last|past)\s+(\d+)\s*(h|hr|hrs|hour|hours|d|day|days)\b", tl)
    if m:
        val = int(m.group(2)); unit = m.group(3)
        out["recent_hours"] = val if unit.startswith('h') else val*24

    for sev in ("critical","major","minor","warning"):
        if re.search(rf"\b{sev}\b", tl):
            out["severity"] = sev.title()
            break

    q = re.search(r"'(.*?)'|\"(.*?)\"", t)
    if q:
        out["alarm_type"] = (q.group(1) or q.group(2)).strip()
    else:
        q = re.search(r"(?:alarm\s*type|type)\s*[: ]+\s*([A-Za-z][A-Za-z0-9 _\-]{2,})", tl)
        if q:
            out["alarm_type"] = q.group(1).strip()

    return out


def _filter_tickets(tickets: List[Dict[str, Any]], *, targets_lower: set, severity: Optional[str], alarm_type: Optional[str]) -> List[Dict[str, Any]]:
    def ok(t: Dict[str, Any]) -> bool:
        if severity and (t.get("severity") or "").title() != severity:
            return False
        if alarm_type:
            at = (t.get("alarm_type") or "").lower()
            if alarm_type.lower() not in at:
                return False
        if targets_lower:
            node = (t.get("node") or "").lower()
            if not _contains_any(node, targets_lower):
                return False
        return True
    return [t for t in tickets if ok(t)]


def _format_tickets_list(tickets: List[Dict[str, Any]], header: str, limit: int = 50) -> str:
    if not tickets:
        return f"{header}: 0"
    lines = []
    for t in tickets[:limit]:
        line = (
            f"- [{t.get('severity','?')}] {t.get('node','?')} • {t.get('alarm_type','?')} "
            f"(AlarmID={t.get('alarm_id','?')}) • corr={t.get('correlation_id','?')} • status={t.get('status','?')}"
        )
        # Optional context: include description/IP if you like
        # desc = (t.get("description") or "").strip()
        # ip = (t.get("ip_address") or "").strip()
        # if desc or ip: line += f" • {desc}{(' • IP=' + ip) if ip else ''}"
        lines.append(line)
    head = f"{header}: {len(tickets)} (showing top {min(limit, len(tickets))})"
    return head + "\n" + "\n".join(lines)


def _ticket_by_id(ticket_id: str, open_list: List[Dict[str, Any]], closed_list: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for t in open_list:
        if (t.get("ticket_id") or "").upper() == ticket_id.upper():
            return t
    for t in closed_list:
        if (t.get("ticket_id") or "").upper() == ticket_id.upper():
            return t
    return None
def _ticket_by_corr_id(corr_id: str, open_list: List[Dict[str, Any]], closed_list: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    cid = (corr_id or "").strip()
    if not cid:
        return None
    for t in open_list:
        if (t.get("correlation_id") or "").strip() == cid:
            return t
    for t in closed_list:
        if (t.get("correlation_id") or "").strip() == cid:
            return t
    return None

def _tokenize_and_map_targets(state: AgentState) -> List[str]:
    user_msg = state.get("user_input", "") or ""
    session_id = state.get("session_id") or os.getenv("SESSION_ID", "cli_session")
    tokens = _tokenize_node_mentions(user_msg)
    if not tokens:
        for txt in _get_recent_human_texts(session_id, 10):
            prev = _tokenize_node_mentions(txt)
            if prev:
                tokens = prev
                break
    token_set = _lower_name_set(tokens)
    perf = state.get("performance_records", []) or []
    inv = state.get("inventory_records", []) or []
    from_perf_nodes = {str(r.get("node") or "").strip().lower() for r in perf if r.get("node")}
    from_inv_nodes = {str(r.get("node") or "").strip().lower() for r in inv if r.get("node")}
    csv_resolved = {n for n in from_perf_nodes.union(from_inv_nodes) if _contains_any(n, token_set)}

    # Site id → node (if provided in CSVs)
    perf_site = {str(r.get("site_id") or "").strip().lower(): str(r.get("node") or "").strip() for r in perf if r.get("site_id") and r.get("node")}
    inv_site = {str(r.get("site_id") or "").strip().lower(): str(r.get("node") or "").strip() for r in inv if r.get("site_id") and r.get("node")}
    for tok in list(token_set):
        for m in (perf_site, inv_site):
            for sk, nv in m.items():
                if tok in sk:
                    csv_resolved.add(nv.strip().lower())

    node_set = {str(n.get("name") or "").strip().lower() for n in state.get("nodes", [])}
    final_targets = [n for n in node_set if n in csv_resolved or _contains_any(n, token_set)]

    # Display casing
    disp_map = {(n.get("name") or "").strip().lower(): n.get("name") for n in state.get("nodes", [])}
    return sorted({disp_map.get(x, x) for x in final_targets})

# ---------------------------------------
# Fault agent (summarized)
# ---------------------------------------
def fault_management_agent(state: AgentState) -> AgentState:
    out: Dict[str, Any] = {}
    alarms = state.get("alarms", [])
    nodes = state.get("nodes", [])
    user_msg = state.get("user_input", "") or ""
    target_set = _lower_name_set(state.get("target_nodes"))

    # Update node status by max active severity
    per_node_max: Dict[str, int] = {}
    for a in alarms:
        if not _is_active_alarm_dict(a):
            continue
        n = (a.get("NodeID") or "").strip()
        if not n:
            continue
        per_node_max[n] = max(per_node_max.get(n, -1), _sev_rank(a))

    updated_nodes: List[Dict[str, Any]] = []
    for n in nodes:
        m = dict(n)
        name = (m.get("name") or "").strip()
        prior = (m.get("status") or "healthy").lower()
        mr = per_node_max.get(name, -1)
        if mr == 3:
            m["status"] = "critical"
        elif mr in (2, 1):
            if prior != "critical":
                m["status"] = "warning"
        else:
            m.setdefault("status", prior or "healthy")
        updated_nodes.append(m)
    try:
        save_nodes(updated_nodes)
    except Exception:
        pass

    def _direct_answer_for_targets() -> str:
        if not target_set:
            return ""
        node_to_alarms: Dict[str, List[Dict[str, Any]]] = {}
        for a in alarms:
            if not _is_active_alarm_dict(a):
                continue
            nid = (a.get("NodeID") or "").strip()
            if not nid:
                continue
            if not _contains_any(nid, target_set):
                continue
            node_to_alarms.setdefault(nid, []).append(a)
        lines = []
        for tok in sorted(target_set):
            group = []
            for k, v in node_to_alarms.items():
                if tok in k.lower():
                    group += v
            if group:
                group.sort(
                    key=lambda aa: (
                        _sev_rank(aa),
                        _to_epoch(aa.get("CreateTime") or aa.get("Timestamp") or aa.get("EventTime"))
                    ),
                    reverse=True
                )
                top = group[0]
                lines.append(f"Yes — {len(group)} active alarm(s) on {tok}: [{top.get('Severity','')}] {top.get('AlarmType','')} (ID={top.get('AlarmID','')})")
            else:
                lines.append(f"No — no active alarms on {tok}.")
        return "\n".join(lines)

    user_asked = _wants_alarms(user_msg)
    yesno_q = bool(re.search(r"\b(is there|are there|any|do we have)\b", user_msg, re.I))
    # --- Signals (non-breaking) ---
    try:
        # Count active alarms by severity (overall and on targets)
        target_set = _lower_name_set(state.get("target_nodes"))
        crit = maj = 0
        impacted_nodes = set()
        for a in alarms:
            if not _is_active_alarm_dict(a):
                continue
            sev = (a.get("Severity") or "").strip().lower()
            n = (a.get("NodeID") or "").strip()
            if sev == "critical": crit += 1
            if sev == "major": maj += 1
            if not target_set or _contains_any(n, target_set):
                impacted_nodes.add(n)
        sig = {
            "active_counts": {"critical": crit, "major": maj},
            "impacted_nodes": sorted(x for x in impacted_nodes if x),
            "has_target_impact": bool(impacted_nodes),
        }
        out["signals"] = {**(state.get("signals") or {}), "fault": sig}
    except Exception:
        pass


    if target_set:
        if user_asked and yesno_q:
            out["direct_answer"] = _direct_answer_for_targets()
            out["yesno_alarm_query"] = True
        # targeted list
        listing = format_active_alarms(alarms, 50, target_set) or "No active alarms for requested nodes."
        if USE_LLM_SUMMARY and listing and "No active alarms" not in listing:
            note = _llm_summary_safe("Summarize the following active alarms concisely in 2 bullet points (impact & next step):\n\n" + listing)
            if note:
                listing += "\n\n" + note
        out["fault_summary"] = listing
        return out

    # Global view fallback
    if user_asked and alarms:
        listing = format_active_alarms(alarms, 50)
        if USE_LLM_SUMMARY and listing:
            note = _llm_summary_safe("Summarize the following active alarms concisely in 2 bullets (mix & hotspots):\n\n" + listing)
            if note:
                listing += "\n\n" + note
        out["fault_summary"] = listing
        return out

    ctx = "\n".join([f"- {n.get('name')}: status={n.get('status','healthy')}" for n in updated_nodes])
    note = _llm_summary_safe("Provide a brief (<=4 lines) fault health note for these nodes (be specific):\n" + ctx)
    out["fault_summary"] = note or "Fault status updated based on active alarms."
    return out

# ---------------------------------------
# Performance agent (compact)
# ---------------------------------------
def performance_management_agent(state: AgentState) -> AgentState:
    records = state.get("performance_records", [])
    if not records:
        return {"performance_summary": "No performance data available."}
    df = pd.DataFrame(records)
    if "node" not in df.columns:
        return {"performance_summary": "Performance CSV missing 'node' column after normalization."}
    df = df.dropna(subset=["node"]).drop_duplicates(subset=["node"], keep="last")
    updated_nodes = [dict(n) for n in state.get("nodes", [])]
    idx = { (n.get("name") or "").strip().lower(): i for i, n in enumerate(updated_nodes) }
    target_set = _lower_name_set(state.get("target_nodes"))
    alerts_all: List[str] = []
    alerts_tgt: List[str] = []
    PERF = {
        "latency_ms_warning": float(os.getenv("LATENCY_MS_WARN", 60)),
        "cdr_percent_warning": float(os.getenv("CDR_PERCENT_WARN", 2.0)),
        "ho_success_min_ok": float(os.getenv("HO_SUCCESS_MIN_OK", 95)),
        "prb_util_percent_warning": float(os.getenv("PRB_UTIL_PERCENT_WARN", 85)),
    }
    for _, row in df.iterrows():
        key = str(row["node"]).strip().lower()
        i = idx.get(key)
        if i is None:
            continue
        m = dict(updated_nodes[i].get("metrics", {}) or {})
        def _f(v):
            try: return float(v)
            except Exception: return None
        m["throughput_mbps"] = _f(row.get("throughput_mbps"))
        m["latency"] = _f(row.get("latency_ms"))
        m["cdr_percent"] = _f(row.get("cdr_percent"))
        m["ho_succ_percent"] = _f(row.get("ho_succ_percent"))
        m["prb_util_percent"] = _f(row.get("prb_util_percent"))
        if m.get("prb_util_percent") is not None:
            m["utilization"] = m["prb_util_percent"] / 100.0
        updated_nodes[i]["metrics"] = m

        prior = (updated_nodes[i].get("status") or "healthy").lower()
        warn = False
        if m.get("latency") is not None and m["latency"] > PERF["latency_ms_warning"]: warn = True
        if m.get("cdr_percent") is not None and m["cdr_percent"] > PERF["cdr_percent_warning"]: warn = True
        if m.get("ho_succ_percent") is not None and m["ho_succ_percent"] < PERF["ho_success_min_ok"]: warn = True
        if m.get("prb_util_percent") is not None and m["prb_util_percent"] > PERF["prb_util_percent_warning"]: warn = True

        if warn and prior != "critical":
            msg = f"⚠️ Performance alert: {updated_nodes[i].get('name','Unknown')} exceeded KPI thresholds."
            alerts_all.append(msg)
            node_l = (updated_nodes[i].get('name','') or '').strip().lower()
            if node_l in target_set or _contains_any(node_l, target_set):
                alerts_tgt.append(msg)
            updated_nodes[i]["status"] = "warning"
        else:
            updated_nodes[i].setdefault("status", prior or "healthy")
    try:
        save_nodes(updated_nodes)
    except Exception:
        pass

    if target_set:
        sel = _filter_nodes_by_names(updated_nodes, target_set)
        details = "\n".join([
            f"- {n.get('name')}: latency={n.get('metrics',{}).get('latency','N/A')}ms, PRB={n.get('metrics',{}).get('prb_util_percent','N/A')}%, "
            f"CDR={n.get('metrics',{}).get('cdr_percent','N/A')}%, HO_Success={n.get('metrics',{}).get('ho_succ_percent','N/A')}%"
            for n in sel
        ])
        summary = "Performance details for requested nodes:\n" + (details or "No details")
        if USE_LLM_SUMMARY:
            note = _llm_summary_safe("Summarize these KPIs (2-4 bullets, highlight threshold breaches):\n\n" + summary)
            if note:
                summary += "\n\n" + note
        alerts_out = alerts_tgt
    else:
        degraded = [n.get("name","?") for n in updated_nodes if n.get("status") in ("warning","critical")]
        ok = [n.get("name","?") for n in updated_nodes if n.get("status") == "healthy"]
        base = (f"Performance check complete: {len(updated_nodes)} nodes.\n"
                f"- Degraded: {len(degraded)} ({', '.join(degraded[:10])}{'...' if len(degraded)>10 else ''})\n"
                f"- Healthy: {len(ok)}")
        summary = base
        if USE_LLM_SUMMARY:
            note = _llm_summary_safe("Summarize network performance in <=4 lines:\n" + base)
            if note:
                summary += "\n\n" + note
        alerts_out = alerts_all
    # --- Signals ---
    try:
        # Prefer nodes returned by this agent (if present), else fallback to existing state
        out_nodes = out.get("nodes") if ('out' in locals() and isinstance(out.get("nodes", None), list)) else (state.get("nodes") or [])
        degraded_nodes = [
            n.get("name", "?")
            for n in out_nodes
            if (n.get("status") in ("warning", "critical"))
        ]

        sig = {
            "alerts_count": len(alerts_out or []),
            "degraded_nodes": degraded_nodes[:200],
            "has_degradation": bool(degraded_nodes),
        }

        return {
            "nodes": updated_nodes,
            "alerts": (state.get("alerts", []) or []) + alerts_out,
            "performance_summary": summary,
            "signals": {**(state.get("signals") or {}), "performance": sig},
        }
    except Exception:
        # fallback to previous return if anything goes wrong
        return {
            "nodes": updated_nodes,
            "alerts": (state.get("alerts", []) or []) + alerts_out,
            "performance_summary": summary,
        }


# ---------------------------------------
# Inventory agent (compact)
# ---------------------------------------
def inventory_management_agent(state: AgentState) -> AgentState:
    records = state.get("inventory_records", [])
    if not records:
        return {"inventory_summary": "No inventory data available."}
    df = pd.DataFrame(records)
    if "node" not in df.columns:
        return {"inventory_summary": "Inventory CSV missing 'node' column after normalization."}
    df = df.dropna(subset=["node"]).drop_duplicates(subset=["node"], keep="last")
    inv_map = {str(r["node"]).strip().lower(): r.to_dict() for _, r in df.iterrows()}
    updated_nodes = [dict(n) for n in state.get("nodes", [])]
    target_set = _lower_name_set(state.get("target_nodes"))
    actions_all: List[str] = []
    actions_tgt: List[str] = []

    def _norm(v):
        try:
            import pandas as _pd
            if isinstance(v, float) and _pd.isna(v):
                return ""
        except Exception:
            pass
        return ("" if v is None else str(v)).strip()

    for i, n in enumerate(updated_nodes):
        name = (n.get("name") or "").strip(); name_l = name.lower()
        row = inv_map.get(name_l)
        if not row:
            continue
        inv = {
            "site_id": row.get("site_id"),
            "site_status": row.get("site_status"),
            "equipment_type": row.get("equipment_type"),
            "equipment_status": row.get("equipment_status"),
            "ip_address": row.get("ip_address"),
            "hostname": row.get("hostname"),
            "region": row.get("region"),
        }
        n["inventory"] = inv
        eq = _norm(inv.get("equipment_status")).lower(); st = _norm(inv.get("site_status")).lower()

        def add(msg: str):
            actions_all.append(msg)
            if name_l in target_set or _contains_any(name_l, target_set):
                actions_tgt.append(msg)

        if st and st != "active":
            add(f"🏷️ Site inactive: {name} (site_status={inv.get('site_status')}).")
        if eq and eq not in {"operational","active","ok"}:
            add(f"🔧 Equipment status for {name}: {inv.get('equipment_status')}. Schedule check.")
        if (n.get("status") or "healthy").lower() != "critical":
            n["status"] = "warning"
        updated_nodes[i] = n
    try:
        save_nodes(updated_nodes)
    except Exception:
        pass

    if target_set:
        sel = _filter_nodes_by_names(updated_nodes, target_set)
        details = "\n".join([
            f"- {n.get('name')}: site_status={n.get('inventory',{}).get('site_status','N/A')}, "
            f"equip_type={n.get('inventory',{}).get('equipment_type','N/A')}, "
            f"equip_status={n.get('inventory',{}).get('equipment_status','N/A')}, "
            f"site_id={n.get('inventory',{}).get('site_id','N/A')}, region={n.get('inventory',{}).get('region','N/A')}"
            for n in sel
        ])
        summary = "Inventory details for requested nodes:\n" + (details or "No details")
        if USE_LLM_SUMMARY:
            note = _llm_summary_safe("Summarize inventory status (2-3 bullets):\n\n" + summary)
            if note:
                summary += "\n\n" + note
        actions_out = actions_tgt
    else:
        base = f"Inventory merged for nodes. Actions: {len(actions_all)}."
        if USE_LLM_SUMMARY:
            note = _llm_summary_safe("Summarize inventory status/actions in <=3 lines:\n" + base)
            if note:
                base += "\n\n" + note
        summary = base
        actions_out = actions_all
    # --- Signals ---
    try:
        site_issues = [a for a in actions_out if "Site inactive" in a]
        equip_issues = [a for a in actions_out if "Equipment status" in a]
        sig = {
            "site_issues_count": len(site_issues),
            "equipment_issues_count": len(equip_issues),
            "has_inventory_issues": bool(site_issues or equip_issues),
        }
        return {"nodes": updated_nodes,
                "inventory_actions": (state.get("inventory_actions", []) or []) + actions_out,
                "inventory_summary": summary,
                "signals": {**(state.get("signals") or {}), "inventory": sig}}
    except Exception:
        return {"nodes": updated_nodes,
                "inventory_actions": (state.get("inventory_actions", []) or []) + actions_out,
                "inventory_summary": summary}


# ---------------------------------------
# Data flow agent (compact)
# ---------------------------------------
def data_flow_management_agent(state: AgentState) -> AgentState:
    updated_nodes = [dict(n) for n in state.get("nodes", [])]
    actions_all: List[str] = []
    actions_tgt: List[str] = []
    target_set = _lower_name_set(state.get("target_nodes"))
    PERF_WARN = float(os.getenv("PRB_UTIL_PERCENT_WARN", 85)) / 100.0

    for i, n in enumerate(updated_nodes):
        m = n.get("metrics", {}) or {}
        util = m.get("utilization")
        if util is None and m.get("prb_util_percent") is not None:
            try:
                util = float(m["prb_util_percent"]) / 100.0
            except Exception:
                util = None
        degraded = n.get("status") in ("warning","critical")
        high_util = isinstance(util, (int,float)) and util > PERF_WARN
        if degraded or high_util:
            n["data_flow"] = {"suggest_reroute": True, "timestamp": datetime.now(timezone.utc).isoformat()}
            msg = f"🔀 DataFlow: Suggest reroute/load-balance around {n.get('name','Unknown')} ."
            actions_all.append(msg)
            node_l = (n.get("name") or "").strip().lower()
            if node_l in target_set or _contains_any(node_l, target_set):
                actions_tgt.append(msg)
        updated_nodes[i] = n
    try:
        save_nodes(updated_nodes)
    except Exception:
        pass
    if target_set:
        base = f"Data flow assessment complete for requested nodes. Suggestions: {len(actions_tgt)}."
        if USE_LLM_SUMMARY and actions_tgt:
            note = _llm_summary_safe("Summarize these data-flow suggestions in 2 bullets:\n" + "\n".join(actions_tgt[:20]))
            if note:
                base += "\n\n" + note

        # --- Signals (NEW) ---
        sig = {
            "suggestions_count": len(actions_tgt),
            "has_suggestions": bool(actions_tgt),
        }
        return {
            "nodes": updated_nodes,
            "data_flow_actions": actions_tgt,
            "data_flow_summary": base,
            "signals": {**(state.get("signals") or {}), "data_flow": sig},  # <--- add to state
        }
    else:
        base = f"Data flow assessment complete. Suggestions: {len(actions_all)}."
        if USE_LLM_SUMMARY and actions_all:
            note = _llm_summary_safe("Summarize network-wide data-flow suggestions in 2 bullets:\n" + "\n".join(actions_all[:20]))
            if note:
                base += "\n\n" + note

        # --- Signals (NEW) ---
        sig = {
            "suggestions_count": len(actions_all),
            "has_suggestions": bool(actions_all),
        }
        return {
            "nodes": updated_nodes,
            "data_flow_actions": actions_all,
            "data_flow_summary": base,
            "signals": {**(state.get("signals") or {}), "data_flow": sig},  # <--- add to state
        }


# ---------------------------------------
# Customer query parsing
# ---------------------------------------
_CUST_ID_RE = re.compile(r"\bCUST\d{3,}\b", re.I)
def _parse_customer_query(text: str) -> Dict[str, Any]:
    t = (text or "").strip()
    tl = t.lower()
    out = {"customer_id": None, "region": None, "high_risk": False, "limit": None}

    # CustomerID like CUST000123
    m = _CUST_ID_RE.search(t)
    if m:
        out["customer_id"] = m.group(0).upper()

    # Region detection: "region: West" or "in West region"
    # --- Region detection (robust) ---
    # Accept only canonical region names used by _states_for_region(...)
    _REGION_CANON = {
        "west": "West",
        "midwest": "Midwest",
        "south": "South",
        "northeast": "Northeast",
    }

    def _canon_region(token: str) -> Optional[str]:
        if not token:
            return None
        t = token.lower()
        # normalize common variants
        if t in {"mid-west", "mid_west"}:
            t = "midwest"
        if t in {"north-east", "north_east"}:
            t = "northeast"
        return _REGION_CANON.get(t)

    # 1) Prefer: "in <region> region"
    m = re.search(r"\bin\s+([a-z\-]+)\s+region\b", tl, re.I)
    cand = _canon_region(m.group(1)) if m else None

    # 2) Fallback: "region[:=] <region>" or "region <region>"
    # (but avoid capturing conjunctions like 'and')
    if not cand:
        m = re.search(r"\bregion\b\s*(?:[:=]\s*)?([a-z\-]+)\b", tl, re.I)
        cand = _canon_region(m.group(1)) if m else None

    if cand:
        out["region"] = cand
    # --- end region detection ---

    if not m:
        m = re.search(r"\bin\s+([A-Za-z]+)\s+region\b", tl)
    if m:
        out["region"] = m.group(1).title()

    # Listing triggers: "high risk", "highest risk", "top 10", "all/whole customers"
    if re.search(r"\b(high\s*risk|at\s*risk|highest\s*risk|churners?)\b", tl):
        out["high_risk"] = True
    m = re.search(r"\btop\s+(\d{1,3})\b", tl)
    if m:
        out["high_risk"] = True
        out["limit"] = int(m.group(1))
    m = re.search(r"\blimit\s+(\d{1,3})\b", tl)
    if m and not out["limit"]:
        out["limit"] = int(m.group(1))
    if ("all customers" in tl or "whole customers" in tl) and not (out["customer_id"] or out["region"]):
        out["high_risk"] = True

    if out["limit"] is None:
        out["limit"] = 20

    # --- Count intent detection (NEW) ---
    # e.g., "how many customers in west region", "count of customers in West"
    if re.search(r"\b(how\s+many|count|number\s+of)\b", tl) and re.search(r"\bcustomers?\b", tl):
        out["want_count"] = True
    # --- end count detection ---

    return out

# ---------------------------------------
# Customer agent (churn / retention analysis)
# ---------------------------------------
def customer_service_agent(state: AgentState) -> AgentState:
    records = state.get("customer_records") or []
    if not records:
        return {"customer_summary": "No customer data available."}

    df = pd.DataFrame(records)
    # Ensure enriched fields exist (idempotent)
    required = {"satisfaction_score","call_drop_rate","churn_probability","risk_level","customer_lifetime_value","customer_name"}
    if not required.issubset(set(df.columns)):
        df = enhance_customer_df(df)

    q = {
        "customer_id": state.get("customer_id"),
        "region": state.get("customer_region"),
        "high_risk": bool(state.get("customer_high_risk", False)),
        "limit": int(state.get("customer_limit") or 20),
    }
    # NEW: support "how many customers in <region>" style queries
    want_count = bool(state.get("customer_want_count", False))
    if want_count and q["region"]:
        try:
            # Count customers in the requested region
            df_region = df[df["Region"] == q["region"]]
            count_total = int(df_region.shape[0])

            # Compose a concise summary that the LLM will preserve
            summary = f"Customers in {q['region']}: {count_total}. See Ticketing for nodes affecting them."

            return {"customer_summary": summary}
        except Exception as e:
            return {"customer_summary": f"Customer count failed: {e}"}

    try:
        # Single-customer detail (id or region) unless user explicitly asked for high-risk listing
        if q["customer_id"] or q["region"]:
            if not q["high_risk"]:
                row = _customer_pick_target(df, q["customer_id"], q["region"])
                analysis = _customer_generate_analysis(row)
                return {"customer_summary": analysis}

        # Listing view
        table = _customer_high_risk_table(df, q["limit"])
        if USE_LLM_SUMMARY and table and "No high-risk" not in table:
            note = _llm_summary_safe(
                "Summarize these high-risk customers in 2 bullets (pattern & next best action):\n\n" + table
            )
            if note:
                table += "\n\n" + note
        return {"customer_summary": table}
    except Exception as e:
        return {"customer_summary": f"Customer analysis failed: {e}"}

# ---------------------------------------
# Ticketing: legacy (event-driven) + query-driven reader for daemon stores
# ---------------------------------------
TICKETS_PATH = os.getenv("TICKETS_PATH", "data/tickets.jsonl")
TICKETS_INDEX_PATH = os.getenv("TICKETS_INDEX_PATH", "data/tickets_index.json")


def _ticket_key_from_event(e: Dict[str, Any]) -> str:
    return "\n".join([
        str(e.get("node") or e.get("NodeID") or "").strip(),
        str(e.get("alarm_id") or e.get("AlarmID") or "").strip(),
        str(e.get("alarm_type") or e.get("AlarmType") or "").strip(),
    ])


def _load_tickets_index() -> Dict[str, str]:
    try:
        os.makedirs(os.path.dirname(TICKETS_INDEX_PATH), exist_ok=True)
        if os.path.exists(TICKETS_INDEX_PATH):
            with open(TICKETS_INDEX_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_tickets_index(index: Dict[str, str]) -> None:
    try:
        with open(TICKETS_INDEX_PATH, "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2, ensure_ascii=False)
    except Exception:
        pass


def _append_ticket_row(row: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(TICKETS_PATH), exist_ok=True)
        with open(TICKETS_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    except Exception:
        pass


def create_or_get_ticket_for_event(e: Dict[str, Any]) -> Dict[str, Any]:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    idx = _load_tickets_index()
    key = _ticket_key_from_event(e) or f"{e.get('node','Unknown')}\n{e.get('alarm_id','?')}\n{e.get('alarm_type','?')}"
    if key in idx:
        return {"ticket_id": idx[key], "status": "existing", "source": "fault-monitor", "created_at": now,
                "node": e.get("node") or e.get("NodeID"), "alarm_id": e.get("alarm_id") or e.get("AlarmID"),
                "alarm_type": e.get("alarm_type") or e.get("AlarmType"), "severity": e.get("severity") or e.get("Severity")}
    tid = f"TT-{uuid.uuid4().hex[:8].upper()}"
    row = {"ticket_id": tid, "created_at": now, "status": "OPEN", "source": "fault-monitor",
           "node": e.get("node") or e.get("NodeID"), "alarm_id": e.get("alarm_id") or e.get("AlarmID"),
           "alarm_type": e.get("alarm_type") or e.get("AlarmType"), "severity": e.get("severity") or e.get("Severity"),
           "create_time": e.get("create_time") or e.get("CreateTime"), "raw": e}
    _append_ticket_row(row); idx[key] = tid; _save_tickets_index(idx); return {**row, "status": "created"}


def trouble_ticketing_agent(state: AgentState) -> AgentState:
    out: Dict[str, Any] = {}

    # Event-driven criticals (legacy path unchanged)
    events = state.get("events") or []
    if events:
        crits = []
        for e in events:
            sev = (e.get("severity") or e.get("Severity") or "").strip().title()
            et = (e.get("event") or "").strip().upper()
            if sev == "Critical" and et in {"ALARM_RAISED","CRITICAL_RAISED"}:
                crits.append(e)
        if crits:
            created: List[Dict[str, Any]] = [create_or_get_ticket_for_event(e) for e in crits]
            lines = [f"- [{t.get('severity','?')}] {t.get('node','?')} • {t.get('alarm_type','?')} (AlarmID={t.get('alarm_id','?')}) → Ticket {t.get('ticket_id')} ({t.get('status')})" for t in created]
            out["tickets"] = (state.get("tickets") or []) + created
            out["ticketing_summary"] = f"Ticketing: {len(created)} CRITICAL alarm(s) processed.\n" + ("\n".join(lines) if lines else "")

    # Query-driven reader
    user_msg = (state.get("user_input") or "").strip()
    q = _parse_ticket_query(user_msg)
    if q.get("want") != "none":
        # Always define these BEFORE any branch uses them
        open_tkts = _load_open_tickets()
        closed_tkts = _load_closed_tickets(tail=2000, within_hours=q.get("recent_hours"))

        targets_lower = _lower_name_set(state.get("target_nodes"))
        severity = q.get("severity")
        alarm_type = q.get("alarm_type")

        summary = ""
        result: List[Dict[str, Any]] = []

        # --- Detail lookups (prefer correlation_id) ---
        if q["want"] == "detail" and q.get("corr_id"):
            t = _ticket_by_corr_id(q["corr_id"], open_tkts, closed_tkts)
            if t:
                summary = _format_tickets_list([t], f"Ticket corr_id={q['corr_id']} • status={t.get('status','?')}", 1)
                result = [t]
            else:
                summary = f"Ticket corr_id={q['corr_id']} not found."
                result = []

        elif q["want"] == "detail" and q.get("ticket_id"):
            # Legacy TT-XXXX detail (kept for backward compatibility)
            t = _ticket_by_id(q["ticket_id"], open_tkts, closed_tkts)
            if t:
                summary = _format_tickets_list([t], f"Ticket (legacy id) • status={t.get('status','?')}", 1)
                result = [t]
            else:
                summary = f"Ticket {q['ticket_id']} not found."
                result = []

        # --- Lists ---
        elif q["want"] in {"open", "index"}:
            flt = _filter_tickets(open_tkts, targets_lower=targets_lower, severity=severity, alarm_type=alarm_type)
            summary = _format_tickets_list(flt, "Open tickets")
            result = flt

        elif q["want"] == "closed":
            flt = _filter_tickets(closed_tkts, targets_lower=targets_lower, severity=severity, alarm_type=alarm_type)
            hdr = "Recently closed tickets" if q.get("recent_hours") else "Closed tickets (tail)"
            summary = _format_tickets_list(flt, hdr)
            result = flt

        if USE_LLM_SUMMARY and summary and result:
            note = _llm_summary_safe(
                "Summarize these tickets in <=3 concise bullets (counts, nodes, next steps):\n\n" + summary
            )
            if note:
                summary += "\n\n" + note

        out["tickets"] = (state.get("tickets") or []) + result
        out["ticketing_summary"] = summary
        # --- Signals (non-blocking) ---
        try:
            open_count = len(_load_open_tickets() or [])
            created_count = len(out.get("tickets") or [])
            sig = {"open_count": open_count, "created_count": created_count}
            out["signals"] = {**(state.get("signals") or {}), "ticketing": sig}
        except Exception:
            pass

        return out


# --- TT events bus (append-only acks from tt_agent) ---
TT_EVENTS_LOG_PATH = os.getenv("TT_EVENTS_LOG", "data/tt_events.jsonl")


def _load_recent_tt_events(within_seconds: int = int(os.getenv("TT_EVENTS_WINDOW_SEC", "900")),
                           max_lines: int = 2000) -> List[Dict[str, Any]]:
    rows = _safe_tail_jsonl(TT_EVENTS_LOG_PATH, max_lines=max_lines)
    if not rows:
        return []
    now = dt.datetime.now(dt.timezone.utc).timestamp()
    out = []
    for r in rows:
        ts = _to_epoch(r.get("ts") or r.get("created_at") or r.get("timestamp"))
        if ts and (now - ts) <= within_seconds:
            out.append(r)
    out.sort(key=lambda x: _to_epoch(x.get("ts") or x.get("created_at") or x.get("timestamp")), reverse=True)
    return out
# ---------------------------------------
# Orchestrator
# ---------------------------------------
def _llm_summary_safe(prompt: str) -> str:
    try:
        return (llm.invoke(prompt).content or "").strip()
    except Exception:
        return ""


def route_by_intent(state: AgentState) -> str:
    intent = (state.get("intent") or "").strip().lower()
    if intent in ["fault","report"]:
        return "fault_agent"
    if intent == "performance":
        return "performance_agent"
    if intent == "inventory":
        return "inventory_agent"
    if intent == "data_flow":
        return "data_flow_agent"
    if intent == "customer":
        return "customer_agent"
    return "fault_agent"

# --- Agent capability manifest (for data-aware routing) ---
AGENT_MANIFEST = {
    "fault_agent": {
        "fn": fault_management_agent,
        "needs": ["alarms"],   # dataset keys it needs non-empty
        "writes": ["fault_summary", "signals.fault"],
    },
    "performance_agent": {
        "fn": performance_management_agent,
        "needs": ["performance_records", "nodes"],
        "writes": ["performance_summary", "signals.performance"],
    },
    "inventory_agent": {
        "fn": inventory_management_agent,
        "needs": ["inventory_records", "nodes"],
        "writes": ["inventory_summary", "signals.inventory"],
    },
    "data_flow_agent": {
        "fn": data_flow_management_agent,
        "needs": ["nodes"],
        "writes": ["data_flow_summary", "signals.data_flow"],
    },
    "customer_agent": {
        "fn": customer_service_agent,
        "needs": ["customer_records"],
        "writes": ["customer_summary"],
    },
    "ticketing_agent": {
        "fn": trouble_ticketing_agent,
        "needs": [],  # reads local files/daemon stores
        "writes": ["ticketing_summary", "tickets", "signals.ticketing"],
    },
}

def _has_data(state: AgentState, needs: list[str]) -> bool:
    for k in needs or []:
        v = state.get(k)
        if isinstance(v, list) and not v: return False
        if v is None: return False
    return True
# --- Ticket prompt: active alarm candidates (by severity gate & targets) ---
def _active_alarm_candidates(state: AgentState) -> List[Dict[str, Any]]:
    """
    Returns active alarm dicts suitable for consent prompt, filtered by:
      - active status,
      - current target nodes (if any),
      - ORCH_TT_PROMPT_SEVERITY gate:
          'major_only'   -> {Major}
          'major_plus'   -> {Major, Critical}
          'critical_plus'-> {Critical}
    Sorted by (severity rank desc, create time desc).
    """
    alarms = state.get("alarms", []) or []
    targets_lower = { (t or "").strip().lower() for t in (state.get("target_nodes") or []) }

    gate = os.getenv("ORCH_TT_PROMPT_SEVERITY", "major_only").strip().lower()
    if gate == "major_only":
        allowed = {"major"}
    elif gate == "major_plus":
        allowed = {"major", "critical"}
    elif gate == "critical_plus":
        allowed = {"critical"}
    else:
        allowed = {"major"}  # safe default

    dedup = {}
    for a in alarms:
        if not _is_active_alarm_dict(a):
            continue
        sev = (a.get("Severity") or "").strip().lower()
        if sev not in allowed:
            continue

        node = (a.get("NodeID") or "").strip()
        if not node:
            continue
        if targets_lower and (node.strip().lower() not in targets_lower):
            continue

        key = (node, str(a.get("AlarmID") or "").strip(), (a.get("AlarmType") or "").strip())
        if key in dedup:
            continue

        cand = {
            "node": node,
            "alarm_id": a.get("AlarmID"),
            "alarm_type": a.get("AlarmType"),
            "severity": a.get("Severity"),
            "create_time": a.get("CreateTime") or a.get("Timestamp") or a.get("EventTime"),
        }
        dedup[key] = cand

    # sort by severity rank (Critical>Major>...) then by time (desc)
    def _rank(sev: str) -> int:
        return SEV_RANK_ORDER.get((sev or "").strip().lower(), -1)

    out = list(dedup.values())
    out.sort(key=lambda c: (_rank(c.get("severity")), _to_epoch(c.get("create_time"))), reverse=True)
    return out
# --- end helper ---

def orchestrator_agent(state: AgentState) -> AgentState:
    """
    Orchestrates intent detection, target resolution, and executes agents via a reactive loop.
    - Starts with a minimal seed based on intent and mapping flags.
    - After each agent finishes, inspects state['signals'] and replans next steps.
    - Keeps consent-based TT creation and daemon-compatible ticket reading.
    - Emits rich flow telemetry (flow_started, agent_started/finished, flow_decision, etc.).
    """
    import uuid as _uuid
    import time
    import re
    import os

    s: AgentState = dict(state or {})
    user_msg = (s.get("user_input") or "").strip()
    session_id = s.get("session_id") or os.getenv("SESSION_ID", "cli_session")

    # Stable request id
    s["request_id"] = s.get("request_id") or f"req-{_uuid.uuid4().hex[:12]}"
    request_id = s["request_id"]

    # Log request + dataset sizes
    logger.info(
        "Request received\n session_id=%s\n request_id=%s\n text=%r",
        session_id, request_id, user_msg[:200],
        extra={"session_id": session_id, "request_id": request_id},
    )
    logger.info(
        "Datasets\n nodes=%d alarms=%d perf=%d inv=%d",
        len(s.get("nodes", [])),
        len(s.get("alarms", [])),
        len(s.get("performance_records", []) or []),
        len(s.get("inventory_records", []) or []),
        extra={"session_id": session_id, "request_id": request_id},
    )

    # Ensure correlation context for all following flow events
    try:
        cv_session_id.set(session_id)
        cv_request_id.set(request_id)
    except Exception:
        pass

    emit_flow_event(
        "flow_started",
        text_preview=user_msg[:240],
        dataset_sizes={
            "nodes": len(s.get("nodes", [])),
            "alarms": len(s.get("alarms", []) or []),
            "performance": len(s.get("performance_records", []) or []),
            "inventory": len(s.get("inventory_records", []) or []),
            "customers": len(s.get("customer_records", []) or []),
        },
    )

    # -------------------- Consent path (unchanged behavior) --------------------
    def _is_affirmative(text: str) -> bool:
        return (text or "").strip().lower() in {
            "y", "yes", "yeah", "yup", "please do", "create",
            "go ahead", "ok", "okay", "sure", "proceed"
        }

    if PENDING_TICKET_REQUESTS.get(session_id) and _is_affirmative(user_msg):
        logger.info(
            "User consent received; creating ticket(s) for conversation targets via batch API",
            extra={"session_id": session_id, "request_id": request_id},
        )
        created: List[Dict[str, Any]] = []
        try:
            targets = s.get("target_nodes") or []
            if not targets:
                targets = _tokenize_and_map_targets(s)

            if not targets:
                s["ticketing_summary"] = "No target nodes resolved from your request; no tickets created."
            else:
                try:
                    from tt_agent import set_conversation_targets  # type: ignore
                    ttl = int(os.getenv("CONV_TARGETS_TTL_SEC", "900"))
                    set_conversation_targets(targets, ttl_sec=ttl)
                except Exception as e:
                    logger.warning(
                        "Failed to set conversation targets: %s", e,
                        extra={"session_id": session_id, "request_id": request_id}
                    )

                from tt_agent import create_tickets_for_nodes  # type: ignore
                mode = os.getenv("ORCH_BATCH_SEVERITY_MODE", "major").strip().lower()
                res = create_tickets_for_nodes(targets, severity_mode=mode)
                created = res.get("created") or []
                existing = res.get("existing") or []

                blocks = []
                if existing:
                    blocks.append(_format_tickets_list(existing, "Existing tickets for requested nodes"))
                if created:
                    blocks.append(_format_tickets_list(created, "Newly created tickets"))
                if not blocks:
                    blocks.append("No new tickets created. No eligible alarms without an open ticket on your targets.")

                s["tickets"] = (s.get("tickets") or []) + created + existing
                s["ticketing_summary"] = "\n\n".join(blocks)

                emit_flow_event(
                    "ticket_batch_result",
                    created_count=len(created),
                    existing_count=len(existing),
                    sample_created=[redact(c) for c in created[:5]],
                    sample_existing=[redact(e) for e in existing[:5]],
                )
        except Exception as e:
            logger.exception(
                "Ticket batch creation failed: %s", e,
                extra={"session_id": session_id, "request_id": request_id}
            )
            s["ticketing_summary"] = "Ticket creation failed due to an internal error."
        finally:
            PENDING_TICKET_REQUESTS.pop(session_id, None)

    # -------------------- Customer query cues --------------------
    cq = _parse_customer_query(user_msg)
    s["customer_id"] = cq.get("customer_id")
    s["customer_region"] = cq.get("region")
    s["customer_high_risk"] = cq.get("high_risk", False)
    s["customer_limit"] = cq.get("limit") or 20
    s["customer_want_count"] = bool(cq.get("want_count", False))  # NEW


    # -------------------- Intent detection --------------------
    sys_preamble = (
        "You are an OSS assistant for telecom infrastructure. "
        "Classify the user's latest request into exactly one of: fault, performance, inventory, data_flow, customer, report. "
        "Respond with a single word only."
    )
    intent_raw = _invoke_with_history(session_id, user_msg, sys_preamble)
    intent = (intent_raw or "fault").strip().lower()
    if re.search(r"\b(health|status|overall|condition)\b", user_msg, re.I):
        intent = "report"
    if intent not in {"fault", "performance", "inventory", "data_flow", "customer", "report"}:
        intent = "fault"
    # Override to 'customer' if generic view but customer cues exist
    if intent in {"fault", "report"} and (cq.get("customer_id") or cq.get("region") or cq.get("high_risk")):
        intent = "customer"
    s["intent"] = intent
    emit_flow_event("intent_detected", intent=intent)

    # -------------------- Target resolution --------------------
    s["target_nodes"] = _tokenize_and_map_targets(s)
    emit_flow_event("targets_resolved", targets=(s.get("target_nodes") or [])[:100])

    # -------------------- Customer→Inventory mapping (scope targets) --------------------
    # Customer→Inventory mapping (scope targets)
    s["_mapping_used"] = False
    s["_mapping_reason"] = "n/a"
    # ✅ Also enable mapping for customer intent
    if s["intent"] in {"performance", "fault", "inventory", "data_flow", "report", "customer"} and (
        s.get("customer_id") or s.get("customer_region")
    ):
        if not s.get("target_nodes"):
            try:
                mapped, reason = _resolve_targets_from_customer_context(s)  # <--- now defined above
                if mapped:
                    s["target_nodes"] = mapped
                    s["_mapping_used"] = True
                    s["_mapping_reason"] = reason
            except Exception:
                pass
    if s.get("_mapping_used"):
        emit_flow_event("mapping_applied", reason=s.get("_mapping_reason"), mapped_targets=(s.get("target_nodes") or [])[:100])

    # Diagnostics and enrichment flags
    s["yesno_alarm_query"] = bool(re.search(r"\b(is there any|are there any|any active|any alarms\?|do we have)\b", user_msg, re.I))
    s["customer_needs_enrichment"] = (
        s["intent"] in {"fault", "performance", "inventory", "data_flow", "report", "customer"}
        and (bool(s.get("customer_id")) or bool(s.get("customer_region")))
    )

    # ==================== Reactive planner starts here ====================

    # Agent capability manifest (local to keep function self-contained)
    AGENT_MANIFEST = {
        "fault_agent": {
            "fn": fault_management_agent,
            "needs": ["alarms"],
        },
        "performance_agent": {
            "fn": performance_management_agent,
            "needs": ["performance_records", "nodes"],
        },
        "inventory_agent": {
            "fn": inventory_management_agent,
            "needs": ["inventory_records", "nodes"],
        },
        "data_flow_agent": {
            "fn": data_flow_management_agent,
            "needs": ["nodes"],
        },
        "customer_agent": {
            "fn": customer_service_agent,
            "needs": ["customer_records"],
        },
        "ticketing_agent": {
            "fn": trouble_ticketing_agent,
            "needs": [],
        },
    }

    def _has_data(local_state: AgentState, needs: List[str]) -> bool:
        for k in needs or []:
            v = local_state.get(k)
            if v is None:
                return False
            if isinstance(v, list) and len(v) == 0:
                return False
        return True

    # Seed queue from intent and mapping flags
    include_inv = os.getenv("INCLUDE_INVENTORY_ON_MAPPING", "true").strip().lower() in {"1", "true", "yes", "on"}
    queue: List[str] = []
    ran: set[str] = set()
    s.setdefault("flow_decisions", [])

    def enqueue(agent: str, reason: str) -> None:
        if agent in ran or agent in queue:
            return
        m = AGENT_MANIFEST.get(agent)
        if not m:
            return
        if not _has_data(s, m.get("needs", [])):
            return
        queue.append(agent)
        s["flow_decisions"].append({"action": "enqueue", "agent": agent, "reason": reason})
        emit_flow_event("flow_decision", action="enqueue", agent=agent, reason=reason)

    if s["intent"] == "customer":
        enqueue("customer_agent", "intent=customer")
    else:
        primary = {
            "fault": "fault_agent",
            "performance": "performance_agent",
            "inventory": "inventory_agent",
            "data_flow": "data_flow_agent",
            "report": "fault_agent",
        }.get(s["intent"], "fault_agent")
        enqueue(primary, f"intent={s['intent']}")
        # If mapping narrowed the scope, inventory view is helpful for any intent (including customer)
        if s.get("_mapping_used") and include_inv:
            enqueue("inventory_agent", "mapping_used")

    emit_flow_event("execution_seed", agents=list(queue))

    def can_consider_ticketing() -> bool:
        return s["intent"] != "customer"

    # Replanning rules look at s['signals'] written by agents
    def replan_after(last_agent: str) -> None:
        sig = s.get("signals") or {}
        f = (sig.get("fault") or {})
        p = (sig.get("performance") or {})
        inv = (sig.get("inventory") or {})
        df = (sig.get("data_flow") or {})

        # Fault-driven branches
        if last_agent == "fault_agent":
            if (f.get("active_counts", {}).get("critical", 0) > 0) and can_consider_ticketing():
                enqueue("ticketing_agent", "critical_faults")
            if (f.get("active_counts", {}).get("major", 0) > 0) and can_consider_ticketing():
                enqueue("ticketing_agent", "major_faults")
            if _has_data(s, AGENT_MANIFEST["performance_agent"]["needs"]):
                enqueue("performance_agent", "fault->performance")
            if _has_data(s, AGENT_MANIFEST["inventory_agent"]["needs"]):
                enqueue("inventory_agent", "fault->inventory")

        # Performance degradation suggests data-flow and possibly fault correlation
        if last_agent == "performance_agent":
            if p.get("has_degradation"):
                enqueue("data_flow_agent", "perf_degraded")
            if "fault_agent" not in ran:
                enqueue("fault_agent", "perf->fault_check")

        # Inventory issues may justify ticketing or perf check
        if last_agent == "inventory_agent":
            if inv.get("has_inventory_issues") and can_consider_ticketing():
                enqueue("ticketing_agent", "inventory_issues")
            if "performance_agent" not in ran and _has_data(s, AGENT_MANIFEST["performance_agent"]["needs"]):
                enqueue("performance_agent", "inventory->performance")

        # Data-flow suggestions -> consider ticket reading/summarization
        if last_agent == "data_flow_agent":
            if df.get("has_suggestions") and can_consider_ticketing():
                enqueue("ticketing_agent", "data_flow_suggestions")

        # Customer enrichment: if mapping narrowed targets, run network checks
        if last_agent == "customer_agent" and s.get("customer_needs_enrichment"):
            if _has_data(s, AGENT_MANIFEST["inventory_agent"]["needs"]):
                enqueue("inventory_agent", "customer->inventory")
            if _has_data(s, AGENT_MANIFEST["fault_agent"]["needs"]):
                enqueue("fault_agent", "customer->fault")
            if _has_data(s, AGENT_MANIFEST["performance_agent"]["needs"]):
                enqueue("performance_agent", "customer->performance")

    # Execute queue with replanning
    MAX_STEPS = int(os.getenv("ORCH_MAX_STEPS", "10"))
    steps = 0

    while queue and steps < MAX_STEPS:
        name = queue.pop(0)
        m = AGENT_MANIFEST.get(name)
        if not m or name in ran:
            continue

        t0 = time.time()
        emit_flow_event("agent_started", agent=name, agent_type=name)
        logger.info(f"Calling agent: {name}", extra={"agent": name, "session_id": session_id, "request_id": request_id})

        try:
            # ✅ Call the agent function and merge outputs into state
            out = m["fn"](s) or {}
            for k, v in out.items():
                if v is not None:
                    s[k] = v

            ms = int((time.time() - t0) * 1000)
            logger.info(
                "%s ✓ %dms\n keys=%s", name, ms, sorted(out.keys()),
                extra={
                    "session_id": session_id,
                    "request_id": request_id,
                    "agent": name,
                    "agent_type": name,
                    "status": "success"
                },
            )


            # Trace for FE/debugging
            s.setdefault("_agent_trace", []).append(
                {"agent": name, "elapsed_ms": ms, "produced": sorted(out.keys())}
            )

            # Mirror progress via WS sink (keep your original shape)
            try:
                import asyncio
                sink = _PROGRESS_SINKS.get(request_id)
                if sink:
                    asyncio.create_task(
                        sink({
                            "event": "progress",
                            "request_id": request_id,
                            "agent": name,
                            "elapsed_ms": ms,
                            "produced": sorted(out.keys()),
                        })
                    )
            except Exception:
                pass
            emit_flow_event(
            "agent_finished",
            agent=name,
            agent_type=name,  # <-- Add this for frontend filtering
            status="success",  # Optional, useful for frontend
            elapsed_ms=ms,
            produced=sorted(out.keys()),
            sizes={k: (len(v) if isinstance(v, (list, dict, str)) else None) for k, v in out.items()},
        )


        except Exception as e:
            logger.exception(
                "%s ✗ failed: %s", name, e,
                extra={"session_id": session_id, "request_id": request_id}
            )
            ms = int((time.time() - t0) * 1000)
            emit_flow_event("agent_failed", agent=name, elapsed_ms=ms, error=str(e))

        # Mark done and replan
        ran.add(name)
        steps += 1

        # Replan based on new signals/state
        try:
            replan_after(name)
        except Exception:
            pass
    
    # -------------------- TT events ingest (unchanged) --------------------
    try:
        events_recent = _load_recent_tt_events()
        if events_recent:
            targets_lower = _lower_name_set(s.get("target_nodes"))
            def _match_node(ev_node: str) -> bool:
                return (not targets_lower) or _contains_any(ev_node or "", targets_lower)

            created_on_targets = [e for e in events_recent
                                  if (str(e.get("event")).upper() == "TT_CREATED" and _match_node(e.get("node")))]
            if created_on_targets:
                lines = [
                    f"✅ Ticket created • [{(e.get('severity') or '?')}] {e.get('alarm_type','?')} • {e.get('node','?')} "
                    f"• corr={e.get('correlation_id','?')} • at {e.get('ts','')}"
                    for e in created_on_targets[:10]
                ]

                try:
                    note = _llm_summary_safe(
                        "Given the user's request and these new ticket events, "
                        "acknowledge creation with correlation ids in one concise line (<=25 words):\n\n"
                        f"User: {s.get('user_input','')}\n"
                        f"Targets: {', '.join(s.get('target_nodes') or [])}\n"
                        "Events:\n" + "\n".join(lines)
                    )

                except Exception:
                    note = ""
                s["alerts"] = (s.get("alerts", []) or []) + ([note] if note else []) + lines
                emit_flow_event("tt_events_ingested", count=len(events_recent), matched_on_targets=len(created_on_targets))
    except Exception as e:
        logger.warning("Ack ingest block error: %s", e,
                       extra={"session_id": session_id, "request_id": request_id})

    # -------------------- Open TT or propose consent (unchanged) --------------------
        # --- Open TT or propose consent (Major-only follow-up) ---
    try:
        # If a ticketing summary already exists from an earlier step, bubble it up now.
        if s.get("ticketing_summary"):
            return s

        # Helper: open tickets for current targets
        def _open_ticket_index_for_targets(local_state: AgentState) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
            try:
                open_all = _load_open_tickets()
            except Exception:
                open_all = []

            targets_lower = _lower_name_set(local_state.get("target_nodes"))
            filtered = _filter_tickets(open_all, targets_lower=targets_lower, severity=None, alarm_type=None)

            def _key(node: str, alarm_id: Any, alarm_type: str) -> str:
                return f"{(node or '').strip().lower()}\n{(str(alarm_id) if alarm_id is not None else '').strip().lower()}\n{(alarm_type or '').strip().lower()}"

            idx: Dict[str, Dict[str, Any]] = {}
            for t in filtered:
                idx[_key(t.get("node"), t.get("alarm_id"), t.get("alarm_type"))] = t
            return idx, filtered
       # --- Open tickets (customer-scoped if applicable) ---
        open_idx, open_list = _open_ticket_index_for_targets(s)

        intent  = (s.get("intent") or "").strip().lower()
        cid     = (s.get("customer_id") or "").strip()
        creg_in = (s.get("customer_region") or "").strip()
        targets = s.get("target_nodes") or []

        # Canonical regions allowed in UI/events (avoid printing garbage like "And")
        _CANON_REGIONS = {"West", "Midwest", "South", "Northeast"}
        creg = creg_in if creg_in in _CANON_REGIONS else ""

        # If user is in a customer flow but mapping produced no targets, do not show a global open-ticket list.
        if intent == "customer" and (cid or creg) and not targets:
            # No targets resolved for this customer/region → skip open-tickets surfacing to avoid misattribution
            pass
        elif open_list:
            # Build a header that reflects customer scope when applicable
            if intent == "customer":
                if cid:
                    header = f"Open tickets affecting {cid}"
                elif creg:
                    header = f"Open tickets in region {creg} (customer-scoped)"
                else:
                    header = "Open tickets for requested nodes"
            else:
                header = "Open tickets for requested nodes"

            s["ticketing_summary"] = _format_tickets_list(open_list, header)
            emit_flow_event(
                "open_tickets_found",
                count=len(open_list),
                scope=("customer" if intent == "customer" else "network"),
                customer_id=(cid or None),
                customer_region=(creg or None),  # <- sanitized; won't emit "And"
                targets=targets[:100],
            )
            return s

# --- end open tickets block ---


        # Propose follow-up ONLY when there is at least one Major alarm (no spam; ask once per session)
        cands = _active_alarm_candidates(s)  # already respects ORCH_TT_PROMPT_SEVERITY gate
        majors = [c for c in cands if (c.get("severity", "").strip().lower() == "major")]

        if majors and (session_id not in PENDING_TICKET_REQUESTS):
            PENDING_TICKET_REQUESTS[session_id] = {"batch": True}  # waiting for user's yes/no

            top = majors[0]
            ask = (
                f"Detected Major alarm on {top['node']}: {top['alarm_type']} (ID={top['alarm_id']}). "
                "Shall I create ticket(s) for the active **Major** alarms on your target(s)? (yes/no)"
            )
            s["alerts"] = (s.get("alerts", []) or []) + [ask]
            logger.info(
                "Proposed ticket batch (Major-only): %s",
                ask,
                extra={"session_id": session_id, "request_id": request_id},
            )
            emit_flow_event(
                "ticket_proposed",
                mode=ORCH_TT_PROMPT_SEVERITY,
                top_candidate={
                    "node": top["node"],
                    "alarm_type": top["alarm_type"],
                    "severity": top["severity"],
                    "alarm_id": top["alarm_id"],
                },
            )

    except Exception as e:
        logger.warning(
            "Proposal block error: %s",
            e,
            extra={"session_id": session_id, "request_id": request_id},
        )

    return s

# ---------------------------------------
# Finalize
# ---------------------------------------

def _select_sections(state: AgentState) -> List[str]:
    intent = (state.get("intent") or "").strip().lower()
    yesno = bool(state.get("yesno_alarm_query", False))
    has_customer = bool(state.get("customer_summary"))

    if yesno:
        sections = ["fault"]
    elif intent == "fault":
        sections = ["fault"] + (["customer"] if has_customer else [])
    elif intent == "performance":
        sections = ["performance"] + (["customer"] if has_customer else [])
    elif intent == "inventory":
        sections = ["inventory"] + (["customer"] if has_customer else [])
    elif intent == "data_flow":
        sections = ["data_flow"] + (["customer"] if has_customer else [])
    elif intent == "customer":
        sections = ["customer"]
    elif intent == "report":
        sections = ["fault","performance","inventory","data_flow"] + (["customer"] if has_customer else [])
    else:
        sections = ["fault","performance","inventory","data_flow"] + (["customer"] if has_customer else [])

    if state.get("ticketing_summary"):
        sections.append("ticketing")
    return sections


def _compose_overall_answer(state: AgentState, sections: List[str]) -> str:
    user_msg = state.get("user_input", "") or ""
    targets = state.get("target_nodes", []) or []
    if bool(state.get("yesno_alarm_query", False)) and state.get("direct_answer"):
        return state["direct_answer"]
    blobs = []
    name_map = {
        "fault": state.get("fault_summary", ""),
        "performance": state.get("performance_summary", ""),
        "inventory": state.get("inventory_summary", ""),
        "data_flow": state.get("data_flow_summary", ""),
        "customer": state.get("customer_summary", ""),
        "ticketing": state.get("ticketing_summary", ""),
    }
    for sct in sections:
        if name_map.get(sct):
            blobs.append(f"{sct.replace('_',' ').title()}:\n{name_map[sct]}")
    context = "\n\n".join(blobs).strip()
    if not USE_LLM_SUMMARY or not context:
        tgt = ", ".join(targets[:5]) if targets else "the network"
        intent = (state.get("intent") or "").strip().lower()
        return {
            "fault": f"Here is the fault status for {tgt}.",
            "performance": f"Here is the performance status for {tgt}.",
            "inventory": f"Here are the inventory details for {tgt}.",
            "data_flow": f"Here are the data flow suggestions for {tgt}.",
            "customer": f"Here is the customer analysis.",
        }.get(intent, f"Here is the requested status for {tgt}.")
    sys_pre = (
        "You are an OSS assistant. Answer the user's request directly and concisely. "
        "Use ONLY facts that are explicitly stated as 'Active' or current in the summaries. "
        "If the summaries do not show active issues, say so. Do NOT infer from historical/cleared items. "
        "Prefer bullets only if helpful, keep <=4 lines."
    )
    tgt_line = f"Target nodes: {', '.join(targets)}" if targets else "Target: network"
    block = f"User request:\n{user_msg}\n\n{tgt_line}\n\nAgent summaries:\n{context}\n\nNow produce a direct answer:"
    sid = state.get("session_id") or os.getenv("SESSION_ID", "cli_session")
    ans = _invoke_with_history(sid, block, sys_pre)
    return ans or "Summary generated."


def finalize_agent(state: AgentState) -> AgentState:
    """
    Final packaging for front-end.
    Returns a structured payload alongside the human-readable 'result':
    - summaries per section, alerts, tickets, trace, request/session ids,
    - tt_events_recent snapshot and sections_rendered (for FE tabs).
    - Emits sections_selected, answer_composed, flow_finished with correlation ids.
    """
    # Ensure correlation context is available to all flow events from finalize
    try:
        cv_session_id.set(state.get("session_id"))
        cv_request_id.set(state.get("request_id"))
    except Exception:
        pass

    sections = _select_sections(state)
    emit_flow_event("sections_selected", sections=sections)

    # Compose concise/semi-structured text (preserves original behavior)
    parts: List[str] = []
    if OVERALL_RESPONSE:
        overall = _compose_overall_answer(state, sections)
        if overall:
            parts.append(f"**Answer:** {overall}")
            # Emit answer preview event (without waiting for structure dict)
            try:
                answer_preview = parts[0][:240]
            except Exception:
                answer_preview = ""
            # Compute sections_rendered the same way as below
            if RESPONSE_STYLE == "sections":
                sections_rendered_preview = sections
            elif RESPONSE_STYLE == "concise":
                sections_rendered_preview = sections[:1]
            else:
                sections_rendered_preview = sections[:2]
            emit_flow_event(
                "answer_composed",
                style=RESPONSE_STYLE,
                sections_rendered=sections_rendered_preview,
                answer_preview=answer_preview,
            )

    section_texts = {
        "fault": state.get("fault_summary"),
        "performance": state.get("performance_summary"),
        "inventory": state.get("inventory_summary"),
        "data_flow": state.get("data_flow_summary"),
        "customer": state.get("customer_summary"),
        "ticketing": state.get("ticketing_summary"),
    }
    headers = {
        "fault": "### Fault",
        "performance": "### Performance",
        "inventory": "### Inventory",
        "data_flow": "### Data Flow",
        "customer": "### Customer",
        "ticketing": "### Trouble Ticketing",
    }

    # Respect RESPONSE_STYLE
    if RESPONSE_STYLE == "sections":
        to_render = sections
    elif RESPONSE_STYLE == "concise":
        to_render = sections[:1]
    else:
        to_render = sections[:2]

    for sct in to_render:
        txt = section_texts.get(sct)
        if txt:
            parts.append(f"{headers[sct]}\n{txt}")

    # --- Structured fields for FE ---
    structured = {
        "request_id": state.get("request_id"),
        "session_id": state.get("session_id"),
        "intent": state.get("intent"),
        "targets": state.get("target_nodes") or [],
        "alerts": state.get("alerts") or [],
        "tickets": state.get("tickets") or [],
        "trace": state.get("_agent_trace") or [],
        "summaries": {
            "fault": section_texts["fault"],
            "performance": section_texts["performance"],
            "inventory": section_texts["inventory"],
            "data_flow": section_texts["data_flow"],
            "customer": section_texts["customer"],
            "ticketing": section_texts["ticketing"],
        },
        "tt_events_recent": _load_recent_tt_events() or [],
        "sections_rendered": to_render,
    }

    # Final text blob (for chat bubble)
    # Include follow-up prompts at the end of the Answer, but only if any exist
    alerts = state.get("alerts") or []
    if alerts:
        parts.append("### Follow-up\n" + "\n".join(alerts))

    result_text = "\n\n".join([p for p in parts if p]) or "Flow complete."


    # Flow finished telemetry (now with session/request ids populated)
    emit_flow_event(
        "flow_finished",
        intent=state.get("intent"),
        targets=(state.get("target_nodes") or [])[:100],
        alerts=len(state.get("alerts") or []),
        tickets=len(state.get("tickets") or []),
        trace=state.get("_agent_trace") or [],
    )

    return {"result": result_text, **structured}

# ---------------------------------------
# Graph wiring
# ---------------------------------------
builder = StateGraph(AgentState)
builder.add_node("orchestrator", orchestrator_agent)
builder.add_node("finalize", finalize_agent)
builder.set_entry_point("orchestrator")
builder.add_edge("orchestrator", "finalize")
builder.add_edge("finalize", END)
graph = builder.compile()

# ---------------------------------------
# CLI
# ---------------------------------------
def _canonize_columns(df: pd.DataFrame) -> pd.DataFrame:
    import re as _re
    df = df.copy()
    df.columns = [_re.sub(r"[\s/]+", "_", str(c).strip().lower()).replace('%','percent').replace('-', '_') for c in df.columns]
    perf_map = {"network_element":"node","site_id":"site_id","throughput_mbps":"throughput_mbps","latency_ms":"latency_ms","call_drop_rate_percent":"cdr_percent","handover_success_rate_percent":"ho_succ_percent","prb_utilization_percent":"prb_util_percent","rrc_connection_attempts":"rrc_attempts","successful_rrc_connections":"rrc_success"}
    inv_map = {"nodename":"node","siteid":"site_id","site_location":"site_location","emsname":"ems_name","region":"region","state":"state","ipaddress":"ip_address","hostname":"hostname","maintenancepoint":"maintenance_point","sitetype":"site_type","sitename":"site_name","latitude":"latitude","longitude":"longitude","sitestatus":"site_status","facilityid":"facility_id","maintenancepointcode":"maintenance_point_code","equipmenttype":"equipment_type","equipmentstatus":"equipment_status","parentsite":"parent_site","parentequipment":"parent_equipment","parentsitefacilityid":"parent_site_facility_id"}
    rename_map = {**{k:v for k,v in perf_map.items() if k in df.columns}, **{k:v for k,v in inv_map.items() if k in df.columns}}
    if rename_map: df.rename(columns=rename_map, inplace=True)
    return df


def load_performance_csv_local(path: str) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(path); df = _canonize_columns(df)
        if "node" not in df.columns:
            raise ValueError("Performance CSV missing 'Network_Element' (mapped to 'node').")
        return df
    except Exception as e:
        print(f"[WARN] load_performance_csv_local: {e}"); return None


def load_inventory_csv_local(path: str) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(path); df = _canonize_columns(df)
        if "node" not in df.columns:
            raise ValueError("Inventory CSV missing 'Nodename' (mapped to 'node').")
        return df
    except Exception as e:
        print(f"[WARN] load_inventory_csv_local: {e}"); return None


def _union_nodes_by_name(base_nodes: List[Dict[str, Any]], perf: List[Dict[str, Any]], inv: List[Dict[str, Any]]):
    known = {(n.get("name") or "").strip().lower() for n in base_nodes}
    extra: List[Dict[str, Any]] = []
    for r in (perf or []):
        nm = str(r.get("node") or "").strip()
        if nm and nm.lower() not in known:
            extra.append({"name": nm, "status": "healthy", "metrics": {}, "alarm_summary": {"active_count": 0}})
            known.add(nm.lower())
    for r in (inv or []):
        nm = str(r.get("node") or "").strip()
        if nm and nm.lower() not in known:
            extra.append({"name": nm, "status": "healthy", "metrics": {}, "alarm_summary": {"active_count": 0}})
            known.add(nm.lower())
    return base_nodes + extra


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="OSS AI Agent: chat mode (default) or fault-monitor daemon via --daemon flag.")
    # FIXED: correct action for boolean flag
    parser.add_argument("--daemon", "-d", action="store_true", help="Run fault monitor daemon (delegated to tt_agent).")
    parser.add_argument("--alarms-csv", default=os.getenv("ALARMS_CSV_PATH", "data/ran_network_alarms.csv"),
                        help="Path to ran_network_alarms.csv (daemon mode only).")
    parser.add_argument("--poll", type=float, default=float(os.getenv("POLL_INTERVAL_SEC", "5")),
                        help="Poll interval in seconds for daemon.")
    parser.add_argument("--severity", choices=["any","major_plus","critical"], default=os.getenv("CREATE_TT_FOR_SEVERITY", "critical"),
                        help="Ticket creation severity filter (daemon): 'critical' (default), 'major_plus', or 'any'.")
    args = parser.parse_args()

    if args.daemon:
        # Launch external daemon inside tt_agent (unchanged)
        os.environ["ALARMS_CSV_PATH"] = args.alarms_csv
        os.environ["POLL_INTERVAL_SEC"] = str(args.poll)
        os.environ["CREATE_TT_FOR_SEVERITY"] = args.severity
        try:
            from importlib import import_module
            daemon = import_module("tt_agent")
            daemon.start_fault_daemon()
            sys.exit(0)  # graceful exit after daemon returns
        except KeyboardInterrupt:
            print("\n[Main] Daemon stopped by user.")
            sys.exit(0)
        except Exception as e:
            print(f"[Main] Failed to start daemon: {e}")
            sys.exit(1)

    # Chat mode
    SESSION_ID = os.getenv("SESSION_ID", "cli_session")
    print("💬 Chat mode. Type /reset to clear chat context, or 'exit' to quit.")

    while True:
        try:
            user_input = input("📥 Enter your OSS request: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n👋 Bye!"); break
        if not user_input:
            continue
        if user_input.lower() in {"exit","quit","q"}:
            print("👋 Bye!"); break
        if user_input.strip().lower() == "/reset":
            reset_chat_history(SESSION_ID); print("♻️ Chat history cleared."); continue

        alarms_csv_path = os.getenv("ALARMS_CSV_PATH", "data/ran_network_alarms.csv")
        performance_csv_path = os.getenv("PERFORMANCE_CSV_PATH", "data/ran_network_performance.csv")
        inventory_csv_path = os.getenv("INVENTORY_CSV_PATH", "data/ran_network_inventory.csv")
        customers_csv_path = os.getenv("CUSTOMERS_CSV_PATH", "data/mobile_customers_usa_by_region.csv")

        alarms_df = load_alarms_csv(alarms_csv_path)
        perf_df = load_performance_csv_local(performance_csv_path)
        inv_df = load_inventory_csv_local(inventory_csv_path)
        cust_df_raw = load_customers_csv_local(customers_csv_path)
        cust_df = enhance_customer_df(cust_df_raw) if cust_df_raw is not None else None

        derived_nodes = nodes_from_alarms(alarms_df)
        perf_records = perf_df.to_dict(orient="records") if perf_df is not None else []
        inv_records = inv_df.to_dict(orient="records") if inv_df is not None else []
        cust_records = cust_df.to_dict(orient="records") if cust_df is not None else []

        derived_nodes = _union_nodes_by_name(derived_nodes, perf_records, inv_records)

        initial_state: AgentState = {
            "user_input": user_input,
            "nodes": derived_nodes,
            "alarms": alarms_df.to_dict(orient="records") if alarms_df is not None else [],
            "performance_records": perf_records,
            "inventory_records": inv_records,
            "customer_records": cust_records,   # <-- customer dataset
            "session_id": SESSION_ID,
        }

        output = graph.invoke(initial_state)
        if output and isinstance(output, dict):
            print("\n✅ Result:\n", output.get("result", "No result found."))
