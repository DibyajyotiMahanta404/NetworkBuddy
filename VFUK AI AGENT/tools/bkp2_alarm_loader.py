# tools/alarm_loader.py
# -*- coding: utf-8 -*-
"""
Alarm loader that supports multiple CSV schemas (old & new) and normalizes them
to a canonical set of columns for downstream agents.

Canonical columns produced:
  AlarmID, NodeID, SiteName, AlarmType, AlarmDescription,
  Severity, AlarmStatus, CreateTime, EndTime
"""

from typing import List, Dict, Any
import pandas as pd
import numpy as np
import re

# ----------------------------
# Config / constants
# ----------------------------
def _canonize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        re.sub(r'[\s/]+', '_', str(c).strip().lower()).replace('%', 'percent').replace('-', '_')
        for c in df.columns
    ]
    perf_map = {
        "network_element": "node",
        "site_id": "site_id",
        "throughput_mbps": "throughput_mbps",
        "latency_ms": "latency_ms",
        "call_drop_rate_percent": "cdr_percent",
        "handover_success_rate_percent": "ho_succ_percent",
        "prb_utilization_percent": "prb_util_percent",
        "rrc_connection_attempts": "rrc_attempts",
        "successful_rrc_connections": "rrc_success",
    }
    inv_map = {
        "nodename": "node",
        "siteid": "site_id",
        "site_location": "site_location",
        "emsname": "ems_name",
        "region": "region",
        "state": "state",
        "ipaddress": "ip_address",
        "hostname": "hostname",
        "maintenancepoint": "maintenance_point",
        "sitetype": "site_type",
        "sitename": "site_name",
        "latitude": "latitude",
        "longitude": "longitude",
        "sitestatus": "site_status",
        "facilityid": "facility_id",
        "maintenancepointcode": "maintenance_point_code",
        "equipmenttype": "equipment_type",
        "equipmentstatus": "equipment_status",
        "parentsite": "parent_site",
        "parentequipment": "parent_equipment",
        "parentsitefacilityid": "parent_site_facility_id",
    }
    rename_map = {}
    for src, dst in {**perf_map, **inv_map}.items():
        if src in df.columns:
            rename_map[src] = dst
    if rename_map:
        df.rename(columns=rename_map, inplace=True)
    return df

def load_performance_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _canonize_columns(df)
    if "node" not in df.columns:
        raise ValueError("Performance CSV missing 'Network_Element' (mapped to 'node').")
    return df

def load_inventory_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = _canonize_columns(df)
    if "node" not in df.columns:
        raise ValueError("Inventory CSV missing 'Nodename' (mapped to 'node').")
    return df

# Severity ranks for comparisons
SEV_RANK = {"critical": 3, "major": 2, "minor": 1, "warning": 0}

# Column harmonization map: (new/old names) -> canonical
RENAME_TO_CANON = {
    # NEW schema -> Canonical
    "Alarm_ID": "AlarmID",
    "Network_Element": "NodeID",
    "Site_ID": "SiteName",
    "Alarm_Type": "AlarmType",
    "Description": "AlarmDescription",
    "Status": "AlarmStatus",
    "Timestamp": "CreateTime",
    "ClearTimeStamp": "EndTime",
    "TTStatus": "TicketStatus",
    "IP_Address": "IPAddress",

    # OLD schema -> Canonical (or pass-through)
    "AlarmID": "AlarmID",
    "NodeID": "NodeID",
    "CiName": "NodeID",              # if old data had CiName as the node identifier
    "SiteName": "SiteName",
    "AlarmType": "AlarmType",
    "AlarmDescription": "AlarmDescription",
    "Severity": "Severity",
    "EventType": "EventType",
    "CreateTime": "CreateTime",
    "EndTime": "EndTime",
    "ModifiedTime": "ModifiedTime",
    "AcknowledgeType": "AcknowledgeType",
    "ProbableCause": "ProbableCause",
    "Impact": "Impact",
    "CorrelationID": "CorrelationID",
    "TicketID": "TicketID",
    "AlarmStatus": "AlarmStatus",
}

# Ensure these exist (filled if missing), after harmonization
CANON_COLS = [
    "AlarmID", "NodeID", "SiteName", "AlarmType", "AlarmDescription",
    "Severity", "AlarmStatus", "CreateTime", "EndTime"
]

# ----------------------------
# Utilities
# ----------------------------

def _strip_df(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace for all object columns without using deprecated applymap."""
    obj_cols = df.select_dtypes(include=["object"]).columns
    for c in obj_cols:
        df[c] = df[c].astype(str).str.strip()
    return df

def _harmonize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to canonical names and ensure canonical columns exist."""
    rename_map = {c: RENAME_TO_CANON[c] for c in df.columns if c in RENAME_TO_CANON}
    df = df.rename(columns=rename_map)

    # Make sure canonical columns exist (fill with empty string for now)
    for col in CANON_COLS:
        if col not in df.columns:
            df[col] = ""

    return df

def _normalize_severity(val: Any) -> str:
    """Normalize Severity to Title-case strings: Critical/Major/Minor/Warning."""
    s = (str(val) if val is not None else "").strip().lower()
    if not s:
        return ""
    # Common normalizations
    if s in {"crit", "critical"}:
        return "Critical"
    if s in {"maj", "major"}:
        return "Major"
    if s in {"min", "minor"}:
        return "Minor"
    if s in {"warn", "warning"}:
        return "Warning"
    # Fallback to Title case
    return s.title()

def _normalize_status_value(val: Any) -> str:
    """
    Normalize AlarmStatus into 'Active'|'Cleared' or Title-case other values.
    Recognizes synonyms across old/new schemas.
    """
    s = (str(val) if val is not None else "").strip().lower()
    if s in {"cleared", "clear", "closed", "close"}:
        return "Cleared"
    if s in {"active", "open", "raised", "raise", "ongoing", "in progress", "inprogress"}:
        return "Active"
    return s.title() if s else ""

def _parse_dt_col(series: pd.Series) -> pd.Series:
    """
    Parse a datetime column robustly.
    - Handles ISO (YYYY-MM-DD HH:MM:SS) and dd-mm-YYYY HH:MM if present.
    - Returns pandas datetime (NaT where parsing fails).
    """
    # Replace empty-like values with NaN for parsing
    s = series.replace({"": np.nan, "None": np.nan, "NaT": np.nan})
    # dayfirst=True is safe for YYYY-MM-DD and correctly parses dd-mm-YYYY
    return pd.to_datetime(s, errors="coerce", dayfirst=True)

def _parse_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["CreateTime", "EndTime", "ModifiedTime"]:
        if col in df.columns:
            df[col] = _parse_dt_col(df[col])
    return df

def _is_open_alarm_row(row: pd.Series) -> bool:
    """
    Canonical open/closed decision:
    - If AlarmStatus explicitly in {Cleared, Close, Closed, Clear} => closed
    - Else if EndTime is NaT/None/empty => open
    - Else => closed
    """
    status = (row.get("AlarmStatus") or "")
    status_l = str(status).strip().lower()
    if status_l in {"cleared", "close", "closed", "clear"}:
        return False

    end = row.get("EndTime")
    return pd.isna(end)  # NaT/None => open

def _max_severity_rank(series: pd.Series) -> int:
    sev = series.astype(str).str.lower().map(SEV_RANK).dropna()
    return int(sev.max()) if not sev.empty else 0

def _status_from_active_alarms(g_active: pd.DataFrame) -> str:
    """
    Derive node status from open alarms:
    - 'critical' if any critical severity or types that imply outage (Down/Power Failure)
    - 'warning' if there are open alarms but not critical
    - 'healthy' if no open alarms
    """
    if g_active is None or g_active.empty:
        return "healthy"

    max_sev = _max_severity_rank(g_active.get("Severity", pd.Series(dtype=str)))

    at = g_active.get("AlarmType", pd.Series(dtype=str)).astype(str)
    has_down = at.str.contains("Down", case=False, na=False).any()
    has_power = at.str.contains("Power Failure", case=False, na=False).any()

    if max_sev >= SEV_RANK.get("critical", 3) or has_down or has_power:
        return "critical"
    return "warning"

# ----------------------------
# Public API
# ----------------------------

def load_alarms_csv(path: str) -> pd.DataFrame:
    """
    Load alarms CSV (old or new schema), harmonize to canonical columns,
    normalize Severity and AlarmStatus, and parse datetimes.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df = _strip_df(df)
    df = _harmonize_columns(df)

    # Normalize Severity
    if "Severity" in df.columns:
        df["Severity"] = df["Severity"].apply(_normalize_severity)

    # Normalize AlarmStatus if present; otherwise infer from EndTime
    if "AlarmStatus" in df.columns and df["AlarmStatus"].notna().any():
        df["AlarmStatus"] = df["AlarmStatus"].apply(_normalize_status_value)
    else:
        # Infer from EndTime (empty => Active; otherwise => Cleared)
        # Note: df['EndTime'] is still string here; parse after normalization
        end_is_empty = df["EndTime"].replace({"": np.nan, "None": np.nan, "NaT": np.nan}).isna()
        df["AlarmStatus"] = np.where(end_is_empty, "Active", "Cleared")

    # Parse datetime columns
    df = _parse_datetimes(df)

    # Fill canonical columns' NaNs with empty strings where appropriate (except datetime cols)
    for c in ["AlarmID", "NodeID", "SiteName", "AlarmType", "AlarmDescription", "Severity", "AlarmStatus"]:
        if c in df.columns:
            df[c] = df[c].fillna("")

    return df


def nodes_from_alarms(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Convert canonicalized alarms DataFrame to node list:
      - name: NodeID
      - status: healthy|warning|critical (from open alarms)
      - site: SiteName (if available)
      - alarm_summary: {active_count, max_severity, types, last_event_time}
      - metrics: {}  (not provided by CSV)
    """
    if df is None or df.empty:
        return []

    if "NodeID" not in df.columns:
        raise ValueError("Canonical NodeID column missing after harmonization; cannot derive nodes.")

    nodes: List[Dict[str, Any]] = []
    for node_id, g in df.groupby("NodeID", dropna=False):
        # Skip blank node ids
        sid = "" if (pd.isna(node_id) or str(node_id).strip() == "") else str(node_id).strip()
        if not sid:
            continue

        # Open alarms
        g_active = g[g.apply(_is_open_alarm_row, axis=1)].copy()

        status = _status_from_active_alarms(g_active)
        active_count = int(len(g_active))
        max_sev = _max_severity_rank(g_active.get("Severity", pd.Series(dtype=str)))
        types = sorted(
            g_active.get("AlarmType", pd.Series(dtype=str)).dropna().unique().tolist()
        ) if active_count else []
        last_event_dt = g_active.get("CreateTime").max() if active_count else None
        last_event = last_event_dt.isoformat() if pd.notna(last_event_dt) else None

        # Representative site (first non-empty)
        site_val = None
        if "SiteName" in g.columns:
            non_empty = g["SiteName"].replace({"": np.nan}).dropna()
            site_val = str(non_empty.iloc[0]) if not non_empty.empty else None

        nodes.append({
            "name": sid,
            "status": status,
            "site": site_val,
            "metrics": {},
            "alarm_summary": {
                "active_count": active_count,
                "max_severity": max_sev,
                "types": types,
                "last_event_time": last_event
            }
        })

    return nodes
