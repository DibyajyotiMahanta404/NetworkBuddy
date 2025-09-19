# agents_core.py
# -*- coding: utf-8 -*-
"""
Core agents and utilities:
- CSV loaders for Inventory / Customers (canonized to predictable columns)
- FaultAgent: groups Active alarms (CorrelationID-first)
- ImpactAgent: maps Node -> Site -> Region -> Customers, computes blast-radius
- NotificationAgent: crafts customer-friendly messages & writes a notifications queue
- ProcessFlow: unified step logger that emits the 6 logs you asked for

No LLM usage here; fully deterministic & idempotent.
"""

import os
import json
import math
import uuid
import datetime as dt
from typing import Dict, Any, List, Optional, Tuple, Set

import pandas as pd

# ---------------------------------------------------------------------------
# Canonical loaders (aligned with your CSV samples)
# Inventory CSV sample columns you shared:
# Nodename,SiteID,Site Location,EMSName,Region,State,IPAddress,Hostname,MaintenancePoint,
# SiteType,SiteName,Latitude,Longitude,SiteStatus,FacilityID,MaintenancePointCode,
# EquipmentType,EquipmentStatus,ParentSite,ParentEquipment,ParentSiteFacilityID
# ---------------------------------------------------------------------------

def load_inventory_csv(path: str) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(path)
        # normalize
        cols = {str(c).strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns}
        df.columns = [str(c).strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
        # rename to canonical
        rename = {
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
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        if "node" not in df.columns:
            raise ValueError("Inventory CSV missing Nodename->node")
        return df
    except Exception as e:
        print(f"[agents_core] inventory load failed: {e}")
        return None

# ---------------------------------------------------------------------------
# Customers CSV sample columns you shared:
# CustomerID,Region,State,City,CityTier,Gender,Age,PlanType,NetworkType,
# TenureMonths,CustomerSince,ARPU_INR,DataUsage_GB,VoiceMinutes,SMSCount,
# RechargeFreqPerMonth,PaymentMethod,HandsetBrand,OTTBundle,FamilyPlan,
# ComplaintsLast90Days,NPS,RoamingDays,Churn
# ---------------------------------------------------------------------------

def load_customers_csv(path: str) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(path)
        # Normalize names (lower snake); keep canonical PascalCase for key fields
        import re as _re
        df.columns = [_re.sub(r"[^\w]+", "_", str(c).strip()).lower() for c in df.columns]
        rename = {
            "customerid": "CustomerID",
            "region": "Region",
            "state": "State",
            "city": "City",
            "citytier": "CityTier",
            "age": "Age",
            "plantype": "PlanType",
            "networktype": "NetworkType",
            "tenuremonths": "TenureMonths",
            "customersince": "CustomerSince",
            "arpu_inr": "ARPU_INR",
            "complaintslast90days": "ComplaintsLast90Days",
            "nps": "NPS",
            "roamingdays": "RoamingDays",
            "churn": "Churn",
        }
        out = {}
        for c in df.columns:
            out[rename.get(c, c)] = df[c]
        z = pd.DataFrame(out)

        # fill missing expected columns
        for c in ["ComplaintsLast90Days", "NPS"]:
            if c not in z.columns:
                z[c] = 0
        return z
    except Exception as e:
        print(f"[agents_core] customers load failed: {e}")
        return None

# ---------------------------------------------------------------------------
# Enrichment: churn probability, risk level, display name, simple CLV
# (mirrors your main.py heuristics; deterministic & fast)
# ---------------------------------------------------------------------------

def enrich_customers(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    z = df.copy()
    def _seed(k: str) -> int:
        return hash(k) % (2**32)

    import numpy as _np
    def _satisfaction(cid: str) -> int:
        rng = _np.random.RandomState(_seed(f"{cid}_s"))
        return int(rng.choice(range(1, 11), p=[0.05,0.05,0.10,0.10,0.15,0.15,0.15,0.15,0.05,0.05]))

    def _call_drop(cid: str) -> float:
        rng = _np.random.RandomState(_seed(f"{cid}_n"))
        return float(rng.uniform(0.5, 8.0))

    z["satisfaction_score"] = z["CustomerID"].astype(str).map(_satisfaction)
    z["call_drop_rate"]    = z["CustomerID"].astype(str).map(_call_drop)

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

    z["churn_probability"] = z.apply(_churn_prob, axis=1)

    def _risk(p: float) -> str:
        if p >= 0.7: return "Critical"
        if p >= 0.5: return "High"
        if p >= 0.3: return "Medium"
        return "Low"

    z["risk_level"] = z["churn_probability"].apply(_risk)

    # Simplified CLV
    def _f(x): 
        try: return float(x)
        except: return 0.0
    z["customer_lifetime_value"] = _f(1) * z.get("ARPU_INR", 0).apply(_f) * z.get("TenureMonths", 0).apply(_f) * (1.0 - z["churn_probability"])

    # Friendly display
    def _name(cid: str) -> str:
        try:
            suffix = str(cid).replace("CUST", "").lstrip("0")
            return f"Customer_{suffix or '0'}"
        except:
            return f"Customer_{cid}"
    z["customer_name"] = z["CustomerID"].astype(str).map(_name)
    return z

# ---------------------------------------------------------------------------
# Fault Agent (Correlation-first grouping of Active alarms)
# We use your existing alarm loader in the orchestrator; here we just group.
# ---------------------------------------------------------------------------

SEV_RANK = {"critical": 3, "major": 2, "minor": 1, "warning": 0}
SEV_RANK_PERF = {"critical": 3, "major": 2, "minor": 1, "ok": 0}
PERF_IMPROVEMENT_DELTA = int(os.getenv("PERF_IMPROVEMENT_DELTA", "2"))  # score drop to call out improvement


class FaultAgent:
    @staticmethod
    def group_active_by_correlation(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        Produce a dict: key -> payload
          key: ["corr", <CorrelationID>] if present, else [NodeID, AlarmID, AlarmType] (JSON-encoded string)
          payload: node, alarm_id, alarm_type, severity, description, correlation_id, rows[]
        """
        out: Dict[str, Dict[str, Any]] = {}
        if df is None or df.empty or "AlarmStatus" not in df.columns:
            return out
        dd = df[df["AlarmStatus"].astype(str).str.strip().str.lower() == "active"].copy()
        if dd.empty:
            return out

        # unify CreateTime for tie-break
        dd["_t"] = pd.NaT
        for c in ("CreateTime", "Timestamp", "EventTime"):
            if c in dd.columns:
                dd["_t"] = pd.to_datetime(dd[c], errors="coerce", utc=True)
                break

        dd["__corr"] = dd.get("CorrelationID", "").astype(str).str.strip()
        has_corr = dd[dd["__corr"].str.len() > 0]
        no_corr  = dd[dd["__corr"].str.len() == 0]

        # with correlation id
        if not has_corr.empty:
            for corr_id, g in has_corr.groupby("__corr", dropna=False):
                g = g.copy()
                g["_rank"] = g["Severity"].map(lambda s: SEV_RANK.get(str(s).lower().strip(), -1))
                g.sort_values(by=["_rank", "_t"], ascending=[False, False], inplace=True, na_position="last")
                primary = g.iloc[0].to_dict()
                key = json.dumps(["corr", corr_id], ensure_ascii=False, separators=(",", ":"))
                out[key] = {
                    "node": (primary.get("NodeID") or "").strip(),
                    "alarm_id": (primary.get("AlarmID") or "").strip(),
                    "alarm_type": (primary.get("AlarmType") or "").strip(),
                    "severity": (primary.get("Severity") or "").strip(),
                    "description": (primary.get("AlarmDescription") or "").strip(),
                    "correlation_id": corr_id,
                    "rows": [r.to_dict() for _, r in g.iterrows()],
                }

        # without correlation id
        if not no_corr.empty:
            seen: Set[str] = set()
            for _, r in no_corr.iterrows():
                row = r.to_dict()
                key = json.dumps([
                    (row.get("NodeID") or "").strip(),
                    (row.get("AlarmID") or "").strip(),
                    (row.get("AlarmType") or "").strip()
                ], ensure_ascii=False, separators=(",", ":"))
                if key in seen:
                    continue
                out[key] = {
                    "node": (row.get("NodeID") or "").strip(),
                    "alarm_id": (row.get("AlarmID") or "").strip(),
                    "alarm_type": (row.get("AlarmType") or "").strip(),
                    "severity": (row.get("Severity") or "").strip(),
                    "description": (row.get("AlarmDescription") or "").strip(),
                    "correlation_id": "",
                    "rows": [row],
                }
                seen.add(key)

        return out

# --------------------------------------------------------------------------------------------------
# Performance CSV loader + PerformanceAgent (detects KPI degradations)
# --------------------------------------------------------------------------------------------------

def load_performance_csv(path: str) -> Optional[pd.DataFrame]:
    """
    Load & normalize performance CSV:
    Expected columns:
      Network_Element, Site_ID, Throughput_Mbps, Latency_ms, Call_Drop_Rate_%,
      Handover_Success_Rate_%, PRB_Utilization_%, RRC_Connection_Attempts, Successful_RRC_Connections
    """
    try:
        df = pd.read_csv(path)
        # normalize column names
        df.columns = [str(c).strip() for c in df.columns]
        # coerce numeric fields
        num_cols = [
            "Throughput_Mbps", "Latency_ms", "Call_Drop_Rate_%",
            "Handover_Success_Rate_%", "PRB_Utilization_%",
            "RRC_Connection_Attempts", "Successful_RRC_Connections",
        ]
        for c in num_cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        # derived KPI: RRC success rate %
        if {"RRC_Connection_Attempts", "Successful_RRC_Connections"}.issubset(df.columns):
            attempts = df["RRC_Connection_Attempts"].replace(0, pd.NA)
            df["RRC_Success_Rate_%"] = (df["Successful_RRC_Connections"] / attempts) * 100.0
            df["RRC_Success_Rate_%"] = df["RRC_Success_Rate_%"].fillna(0.0)
        else:
            df["RRC_Success_Rate_%"] = 100.0
        # canonical keys
        if "Network_Element" not in df.columns:
            raise ValueError("Performance CSV missing Network_Element")
        if "Site_ID" not in df.columns:
            df["Site_ID"] = ""
        return df
    except Exception as e:
        print(f"[agents_core] performance load failed: {e}")
        return None


class PerformanceAgent:
    """
    Detect performance degradations using simple thresholds and a composite severity score.
    Severity mapping (sum of points):
      0-1: none, 2-3: minor, 4-6: major, >=7: critical
    Thresholds are overrideable via env:
      PERF_TP_LOW (Mbps), PERF_LAT_HIGH (ms), PERF_CDR_HIGH (%),
      PERF_HO_SR_LOW (%), PERF_PRB_HIGH (%), PERF_RRC_SR_LOW (%)
    """

    # Defaults (can be overridden via environment)
    TP_LOW = float(os.getenv("PERF_TP_LOW", "100"))
    LAT_HIGH = float(os.getenv("PERF_LAT_HIGH", "80"))
    CDR_HIGH = float(os.getenv("PERF_CDR_HIGH", "1.5"))
    HO_SR_LOW = float(os.getenv("PERF_HO_SR_LOW", "95"))
    PRB_HIGH = float(os.getenv("PERF_PRB_HIGH", "85"))
    RRC_SR_LOW = float(os.getenv("PERF_RRC_SR_LOW", "90"))

    @classmethod
    def _score_row(cls, r: Dict[str, Any]) -> Tuple[int, List[str]]:
        """
        Return (score, reasons[]) based on rule triggers.
        Higher score => worse condition.
        """
        score = 0
        reasons: List[str] = []

        tp = float(r.get("Throughput_Mbps", 0.0) or 0.0)
        lat = float(r.get("Latency_ms", 0.0) or 0.0)
        cdr = float(r.get("Call_Drop_Rate_%", 0.0) or 0.0)
        ho = float(r.get("Handover_Success_Rate_%", 100.0) or 100.0)
        prb = float(r.get("PRB_Utilization_%", 0.0) or 0.0)
        rrc = float(r.get("RRC_Success_Rate_%", 100.0) or 100.0)

        # Throughput low
        if tp < cls.TP_LOW:
            if tp < cls.TP_LOW * 0.5:
                score += 3
                reasons.append(f"Throughput low: {tp:.1f} Mbps")
            else:
                score += 2
                reasons.append(f"Throughput below target: {tp:.1f} Mbps")

        # Latency high
        if lat > cls.LAT_HIGH:
            if lat > cls.LAT_HIGH * 1.5:
                score += 3
                reasons.append(f"Latency high: {lat:.1f} ms")
            else:
                score += 2
                reasons.append(f"Latency above target: {lat:.1f} ms")

        # Call drop rate high
        if cdr > cls.CDR_HIGH:
            if cdr > cls.CDR_HIGH * 2:
                score += 3
                reasons.append(f"Call drop high: {cdr:.2f}%")
            else:
                score += 2
                reasons.append(f"Call drop above target: {cdr:.2f}%")

        # Handover success low
        if ho < cls.HO_SR_LOW:
            if ho < cls.HO_SR_LOW - 5:
                score += 2
            else:
                score += 1
            reasons.append(f"Handover success low: {ho:.2f}%")

        # PRB very high (congestion)
        if prb > cls.PRB_HIGH:
            if prb > cls.PRB_HIGH + 10:
                score += 2
            else:
                score += 1
            reasons.append(f"PRB utilization high: {prb:.2f}%")

        # RRC success low
        if rrc < cls.RRC_SR_LOW:
            if rrc < cls.RRC_SR_LOW - 10:
                score += 3
            else:
                score += 2
            reasons.append(f"RRC success low: {rrc:.2f}%")

        return score, reasons

    @staticmethod
    def _sev_from_score(score: int) -> str:
        if score >= 7:
            return "critical"
        if score >= 4:
            return "major"
        if score >= 2:
            return "minor"
        return "ok"

    @classmethod
    def detect_degradation(cls, df: Optional[pd.DataFrame]) -> Dict[str, Dict[str, Any]]:
        """
        Returns dict: key -> payload
          key: JSON ["perf", Network_Element]  (unique per element)
          payload: {
             "node": <Network_Element>, "site_id": <Site_ID>, "severity": <str>,
             "score": <int>, "reasons": [..], "metrics": {...}
          }
        Only entries with severity != "ok" are returned.
        """
        out: Dict[str, Dict[str, Any]] = {}
        if df is None or df.empty:
            return out

        for _, row in df.iterrows():
            r = row.to_dict()
            ne = str(r.get("Network_Element") or "").strip()
            site_id = str(r.get("Site_ID") or "").strip()
            if not ne:
                continue

            score, reasons = cls._score_row(r)
            sev = cls._sev_from_score(score)
            if sev == "ok":
                continue

            key = json.dumps(["perf", ne], ensure_ascii=False, separators=(",", ":"))
            metrics = {
                "Throughput_Mbps": float(r.get("Throughput_Mbps", 0.0) or 0.0),
                "Latency_ms": float(r.get("Latency_ms", 0.0) or 0.0),
                "Call_Drop_Rate_%": float(r.get("Call_Drop_Rate_%", 0.0) or 0.0),
                "Handover_Success_Rate_%": float(r.get("Handover_Success_Rate_%", 0.0) or 0.0),
                "PRB_Utilization_%": float(r.get("PRB_Utilization_%", 0.0) or 0.0),
                "RRC_Success_Rate_%": float(r.get("RRC_Success_Rate_%", 100.0) or 100.0),
            }
            out[key] = {
                "node": ne,
                "site_id": site_id,
                "severity": sev,
                "score": score,
                "reasons": reasons,
                "metrics": metrics,
            }
        return out


# Impact Helper (Site/Region mapping + Customer enrichment + Blast Radius)
# --------------------------------------------------------------------------------------------------
class ImpactHelper:
    """
    Orchestrator-owned helper (not a standalone agent):
    - Map nodes -> site -> region (via inventory)
    - Pull customers by impacted regions
    - Compute blast-radius score (0..100)
    """
    RISK_W = {"Critical": 4, "High": 3, "Medium": 2, "Low": 1}

    @staticmethod
    def map_nodes_to_regions(nodes: List[str], inv: Optional[pd.DataFrame]) -> Tuple[List[str], Dict[str, Any]]:
        if inv is None or not nodes:
            return [], {"regions": [], "by_node": {}}
        inv = inv.copy()
        inv["node_l"] = inv["node"].astype(str).str.strip().str.lower()
        node_set = {n.strip().lower() for n in nodes}
        sub = inv[inv["node_l"].isin(node_set)]
        regions = sorted({str(x) for x in sub.get("region", pd.Series(dtype=str)).dropna().unique().tolist()})
        by_node = {}
        for _, r in sub.iterrows():
            by_node[str(r["node"])] = {
                "site_id": r.get("site_id"),
                "region": r.get("region"),
                "state": r.get("state"),
                "site_status": r.get("site_status"),
                "equipment_status": r.get("equipment_status"),
            }
        return regions, {"regions": regions, "by_node": by_node}

    @classmethod
    def customers_in_regions(cls, cust_enriched: Optional[pd.DataFrame], regions: List[str]) -> pd.DataFrame:
        if cust_enriched is None or not regions:
            return pd.DataFrame()
        return cust_enriched[cust_enriched["Region"].isin(regions)].copy()

    @classmethod
    def blast_radius(cls, customers: pd.DataFrame) -> Dict[str, Any]:
        if customers is None or customers.empty:
            return {"score": 0, "high_risk": 0, "total": 0, "by_region": []}
        total = int(len(customers))
        wsum = 0
        risk_counts = {"Critical": 0, "High": 0, "Medium": 0, "Low": 0}
        for _, r in customers.iterrows():
            risk = r.get("risk_level", "Low")
            risk_counts[risk] = risk_counts.get(risk, 0) + 1
            wsum += cls.RISK_W.get(risk, 1)
        # normalize to 0..100 (4 = max weight)
        score = int(round(100.0 * (wsum / (4.0 * max(1, total)))))
        by_region = (
            customers.groupby("Region")["customer_lifetime_value"].agg(["count", "sum"])
            .reset_index().rename(columns={"count": "customers", "sum": "clv_sum"})
            .to_dict(orient="records")
        )
        return {
            "score": score,
            "high_risk": int((customers["churn_probability"] >= 0.5).sum()),
            "total": total,
            "risk_counts": risk_counts,
            "by_region": by_region,
        }

# ---------------------------------------------------------------------------
# Notification Agent (customer-friendly messages)
# Writes to data/notifications.jsonl (append-only)
# ---------------------------------------------------------------------------

# Add this import near the top of agents_core.py
try:
    from gmail import send_mail as _gmail_send_mail
except Exception:
    _gmail_send_mail = None


class NotificationAgent:
    """
    NotificationAgent with optional LLM integration and email notifications.
    - If USE_LLM_NOTIFICATIONS=true (env), uses Azure OpenAI via LangChain to craft
      concise, customer-friendly messages.
    - Falls back to deterministic templates on any failure or when disabled.
    - Writes JSONL to data/notifications.jsonl (same as before).
    - If EMAIL_ON_NOTIFICATION=true (env), sends the payload's message via Gmail SMTP.
    """

    NOTIFS_LOG = os.getenv("NOTIFICATIONS_LOG", "data/notifications.jsonl")
    USE_LLM = os.getenv("USE_LLM_NOTIFICATIONS", "false").strip().lower() in {"1", "true", "yes", "on"}
    _LLM = None  # lazy init to avoid import overhead if disabled

    # Email feature flags
    EMAIL_ON_NOTIFICATION = os.getenv("EMAIL_ON_NOTIFICATION", "false").strip().lower() in {"1", "true", "yes", "on"}
    EMAIL_TO = os.getenv("TO_EMAIL", "")
    EMAIL_CC = os.getenv("CC_EMAILS", "")

    # ---------------- Basics ----------------
    @staticmethod
    def _safe_dir(path: str) -> None:
        d = os.path.dirname(path) or "."
        os.makedirs(d, exist_ok=True)

    # ---------------- Prompt helpers ----------------
    @staticmethod
    def _regions_phrase(regions: List[str]) -> str:
        if not regions:
            return "your area"
        if len(regions) == 1:
            return regions[0]
        if len(regions) == 2:
            return f"{regions[0]} and {regions[1]}"
        return f"{', '.join(regions[:-1])}, and {regions[-1]}"

    @classmethod
    def _incident_prompt(cls, node: str, regions: List[str], sev: str, alarm_type: str,
                         blast: Dict[str, Any]) -> Dict[str, str]:
        area = cls._regions_phrase(regions)
        score = blast.get("score", 0)
        high_risk = blast.get("high_risk", 0)
        total = blast.get("total", 0)
        system = (
            "You are a telecom provider's customer communications assistant. "
            "Write clear, empathetic, plain-language updates (2 sentences max). "
            "Avoid internal jargon, IDs, or blame. Offer reassurance and next steps. "
            "Do not expose node identifiers or correlation IDs. Refer to the issue broadly."
        )
        user = (
            f"Issue context:\n"
            f"- Area: {area}\n"
            f"- Severity: {sev}\n"
            f"- Type: {alarm_type}\n"
            f"- Estimated impact score (0-100): {score}\n"
            f"- High-risk customers in scope: {high_risk} (of {total})\n\n"
            f"Write a customer-facing incident update in <= 2 sentences. "
            f"Tone: calm, helpful, and proactive. "
            f"Do not reveal internal node/correlation identifiers."
        )
        return {"system": system, "user": user}

    @classmethod
    def _resolution_prompt(cls, node: str, regions: List[str], alarm_type: str) -> Dict[str, str]:
        area = cls._regions_phrase(regions)
        system = (
            "You are a telecom provider's customer communications assistant. "
            "Write clear, appreciative, plain-language resolution updates (2 sentences max). "
            "Avoid internal jargon or IDs. Thank customers for patience."
        )
        user = (
            f"Outage/incident has been resolved.\n"
            f"- Area: {area}\n"
            f"- Type: {alarm_type}\n\n"
            f"Write a customer-facing resolution update in <= 2 sentences."
        )
        return {"system": system, "user": user}

    # ---------------- LLM plumbing ----------------
    @classmethod
    def _lazy_llm(cls):
        """Lazily initialize AzureChatOpenAI (LangChain). Returns an LLM instance or None if unavailable."""
        if not cls.USE_LLM:
            return None
        if cls._LLM is not None:
            return cls._LLM
        try:
            try:
                from dotenv import load_dotenv
                load_dotenv()
            except Exception:
                pass
            from langchain_openai import AzureChatOpenAI
            deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")
            # Uses env: AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_VERSION
            cls._LLM = AzureChatOpenAI(
                deployment_name=deployment,
                temperature=0.0,
            )
            return cls._LLM
        except Exception as e:
            # Disable LLM gracefully
            cls._LLM = None
            cls.USE_LLM = False
            print(f"[NotificationAgent] LLM init disabled due to error: {e}")
            return None

    @classmethod
    def _gen_llm_text(cls, system_text: str, user_text: str) -> str:
        """Invoke the chat model with a simple system/user exchange. Returns text or empty string on failure."""
        llm = cls._lazy_llm()
        if llm is None:
            return ""
        try:
            from langchain.schema import SystemMessage, HumanMessage
            resp = llm.invoke([SystemMessage(content=system_text), HumanMessage(content=user_text)])
            text = (getattr(resp, "content", "") or "").strip()
            return text[:800]  # safety cap
        except Exception as e:
            print(f"[NotificationAgent] LLM generate error: {e}")
            return ""

    # ---------------- Email helpers ----------------
    @classmethod
    def _email_subject(cls, payload: dict) -> str:
        t = payload.get("type", "").lower()
        regions = payload.get("regions", [])
        area = cls._regions_phrase(regions)
        if t == "incident":
            return f"[{payload.get('severity','').upper()}] Network incident in {area}"
        if t == "resolution":
            return f"[RESOLVED] Issue in {area}"
        if t == "performance":
            return f"[{payload.get('severity','').upper()}] Performance degradation in {area}"
        if t == "performance_improvement":
            return f"[IMPROVING] Performance update for {area}"
        return f"Network notification for {area}"

    @classmethod
    def _send_email_if_enabled(cls, payload: dict) -> bool:
        if not (cls.EMAIL_ON_NOTIFICATION and _gmail_send_mail):
            return False
        try:
            subject = cls._email_subject(payload)
            body = str(payload.get("message", "")).strip()
            if not body:
                return False
            cc = [x.strip() for x in cls.EMAIL_CC.split(",") if x.strip()] if cls.EMAIL_CC else None
            _gmail_send_mail(subject, body, to=cls.EMAIL_TO or None, cc=cc)
            return True
        except Exception as e:
            # Non-fatal: log to console and continue
            print(f"[NotificationAgent] email send failed: {e}")
            return False

    # ---------------- Public methods ----------------
    @classmethod
    def send_performance(cls, node: str, site_id: str, regions: List[str], severity: str,
                         metrics: Dict[str, Any], affected_customers: int,
                         reasons: List[str], correlation_id: str = "") -> Dict[str, Any]:
        llm_used = False
        message = ""
        if cls.USE_LLM:
            area = cls._regions_phrase(regions)
            system = (
                "You are a telecom provider's customer communications assistant. "
                "Write clear, empathetic, short updates (<=2 sentences). "
                "Avoid internal IDs. Do not expose network element identifiers."
            )
            user = (
                f"Performance degradation detected.\n"
                f"- Area: {area}\n"
                f"- Severity: {severity}\n"
                f"- Affected customers (estimate): {affected_customers}\n"
                f"- KPIs: {json.dumps(metrics, ensure_ascii=False)}\n"
                f"- Reasons: {', '.join(reasons)}\n\n"
                f"Write a customer-facing update (<=2 sentences), suggesting we are working to restore quality."
            )
            message = cls._gen_llm_text(system, user)
            llm_used = bool(message)
        if not message:
            area = cls._regions_phrase(regions)
            message = (
                f"We’re addressing a network performance issue in {area} "
                f"(severity {severity}). Estimated {affected_customers} customers may notice slower speeds or "
                f"higher latency. Our teams are optimizing the network and will update you soon."
            )
        payload = {
            "type": "performance",
            "node": node,
            "site_id": site_id,
            "regions": regions,
            "severity": severity,
            "metrics": metrics,
            "affected_customers": affected_customers,
            "reasons": reasons,
            "correlation_id": correlation_id,
            "message": message,
            "llm_used": llm_used,
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        cls._safe_dir(cls.NOTIFS_LOG)
        with open(cls.NOTIFS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        payload["email_sent"] = cls._send_email_if_enabled(payload)
        return payload

    @classmethod
    def send_performance_improvement(cls, node: str, site_id: str, regions: List[str], severity: str,
                                     old_severity: str, metrics: Dict[str, Any], affected_customers: int,
                                     reasons: List[str], old_score: int = 0, new_score: int = 0,
                                     correlation_id: str = "") -> Dict[str, Any]:
        llm_used = False
        message = ""
        if cls.USE_LLM:
            area = cls._regions_phrase(regions)
            system = (
                "You are a telecom provider's customer communications assistant. "
                "Write brief, positive, plain-language updates (<=2 sentences). "
                "Explain that service quality is improving but work continues. "
                "Avoid internal IDs and technical jargon."
            )
            user = (
                f"Network performance is trending better.\n"
                f"- Area: {area}\n"
                f"- Severity: improving from {old_severity} to {severity}\n"
                f"- Affected customers (estimate): {affected_customomers}\n"
                f"- KPIs now: {json.dumps(metrics, ensure_ascii=False)}\n"
                f"- Reasons: {', '.join(reasons)}\n"
                f"- Score: {old_score} → {new_score}\n\n"
                f"Write a customer-facing improvement update (<=2 sentences) that is encouraging and clear."
            )
            message = cls._gen_llm_text(system, user)
            llm_used = bool(message)
        if not message:
            area = cls._regions_phrase(regions)
            message = (
                f"Service quality in {area} is improving (now {severity}). "
                f"An estimated {affected_customers} customers may still notice some impact while we complete optimizations."
            )
        payload = {
            "type": "performance_improvement",
            "node": node,
            "site_id": site_id,
            "regions": regions,
            "severity": severity,
            "old_severity": old_severity,
            "metrics": metrics,
            "affected_customers": affected_customers,
            "reasons": reasons,
            "old_score": old_score,
            "new_score": new_score,
            "correlation_id": correlation_id,
            "message": message,
            "llm_used": llm_used,
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        cls._safe_dir(cls.NOTIFS_LOG)
        with open(cls.NOTIFS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        payload["email_sent"] = cls._send_email_if_enabled(payload)
        return payload

    @classmethod
    def send_incident(cls, node: str, regions: List[str], sev: str, alarm_type: str,
                      blast: Dict[str, Any], correlation_id: str = "") -> Dict[str, Any]:
        llm_used = False
        message = ""
        if cls.USE_LLM:
            p = cls._incident_prompt(node, regions, sev, alarm_type, blast)
            message = cls._gen_llm_text(p["system"], p["user"])
            llm_used = bool(message)
        if not message:
            message = (
                f"We’re investigating a network issue affecting {', '.join(regions) or 'your area'} "
                f"(Severity {sev}). Estimated impact: {blast.get('score',0)}% blast radius; "
                f"{blast.get('high_risk',0)} high‑risk customers in scope. "
                f"Our teams have initiated self‑healing and will update you shortly."
            )
        payload = {
            "type": "incident",
            "node": node,
            "regions": regions,
            "severity": sev,
            "alarm_type": alarm_type,
            "blast_radius": blast,
            "correlation_id": correlation_id,  # internal
            "message": message,
            "llm_used": llm_used,
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        cls._safe_dir(cls.NOTIFS_LOG)
        with open(cls.NOTIFS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        payload["email_sent"] = cls._send_email_if_enabled(payload)
        return payload

    @classmethod
    def send_resolution(cls, node: str, regions: List[str], alarm_type: str, correlation_id: str = "") -> Dict[str, Any]:
        llm_used = False
        message = ""
        if cls.USE_LLM:
            p = cls._resolution_prompt(node, regions, alarm_type)
            message = cls._gen_llm_text(p["system"], p["user"])
            llm_used = bool(message)
        if not message:
            message = (
                f"Good news — the network issue in {', '.join(regions) or 'the affected area'} "
                f"({alarm_type}) has been resolved. Services should be back to normal. "
                f"Thanks for your patience."
            )
        payload = {
            "type": "resolution",
            "node": node,
            "regions": regions,
            "alarm_type": alarm_type,
            "correlation_id": correlation_id,
            "message": message,
            "llm_used": llm_used,
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        cls._safe_dir(cls.NOTIFS_LOG)
        with open(cls.NOTIFS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        payload["email_sent"] = cls._send_email_if_enabled(payload)
        return payload

# ---------------------------------------------------------------------------
# ProcessFlow logger — emits numbered steps you asked for
# Writes a JSONL stream; also emits a simple text line in same object
# ---------------------------------------------------------------------------

# ---- REPLACE ProcessFlow class in agents_core.py ----
import time
import tempfile

class ProcessFlow:
    """
    Unified process/agent logger.
    - log(step_no, message, **fields): your numbered flow logs (Log 1..6)
    - agent(agent, action, **fields): emits AGENT=... ACTION=... lines before steps
    Both write to PROCESS_FLOW_LOG (JSONL) and mirror a compact line to console.
    Robust against Windows/OneDrive file locks: retries + fallback.
    """

    # Expand %ENVVARS% and ~ in the path, default to project ./data
    _cfg_path = os.getenv("PROCESS_FLOW_LOG", "data/process_flow.jsonl")
    FLOW_LOG = os.path.expanduser(os.path.expandvars(_cfg_path))

    TO_CONSOLE = os.getenv("PROCESS_FLOW_TO_CONSOLE", "true").strip().lower() in {"1", "true", "yes", "on"}

    # Retry settings for transient locks (e.g., OneDrive/AV)
    _RETRIES = int(os.getenv("PROCESS_FLOW_RETRIES", "5"))          # attempts
    _BACKOFF_SEC = float(os.getenv("PROCESS_FLOW_BACKOFF_SEC", "0.25"))  # base backoff seconds

    @staticmethod
    def _safe_dir(path: str) -> None:
        d = os.path.dirname(path) or "."
        os.makedirs(d, exist_ok=True)

    @classmethod
    def _fallback_path(cls) -> str:
        """Fallback to a temp folder if the primary path keeps failing."""
        d = os.path.join(tempfile.gettempdir(), "vfuk_ai_agent")
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, "process_flow.jsonl")

    @classmethod
    def _try_open_append(cls, path: str):
        """Open file for append with retries; return file handle or raise."""
        cls._safe_dir(path)
        last_err = None
        for i in range(cls._RETRIES):
            try:
                return open(path, "a", encoding="utf-8")
            except PermissionError as e:
                last_err = e
                # Exponential-ish backoff
                time.sleep(cls._BACKOFF_SEC * (i + 1))
            except FileNotFoundError:
                # Directory may have been removed between attempts
                cls._safe_dir(path)
                time.sleep(0.05)
        if last_err:
            raise last_err
        raise PermissionError(f"Could not open {path} for append")

    @classmethod
    def _write(cls, obj: dict):
        # First try primary path with retries
        try:
            with cls._try_open_append(cls.FLOW_LOG) as f:
                f.write(json.dumps(obj, ensure_ascii=False, default=str) + "\n")
                return
        except PermissionError as e:
            # Fall back to a temp location to keep the daemon alive
            fb = cls._fallback_path()
            try:
                with cls._try_open_append(fb) as f:
                    f.write(json.dumps(obj, ensure_ascii=False, default=str) + "\n")
                print(f"[ProcessFlow] WARN: Permission denied on '{cls.FLOW_LOG}'. "
                      f"Wrote to fallback '{fb}'. err={e}")
            except Exception as e2:
                # Last resort: emit to console only
                print(f"[ProcessFlow] ERROR: Failed to write both primary '{cls.FLOW_LOG}' and fallback. err={e2}")
        except Exception as e:
            print(f"[ProcessFlow] ERROR: Unexpected write error: {e}")

    @classmethod
    def agent(cls, agent: str, action: str, **fields):
        payload = {
            "kind": "agent",
            "agent": agent,
            "action": action,
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            **fields
        }
        cls._write(payload)
        if cls.TO_CONSOLE:
            key = fields.get("key", "")
            node = fields.get("node", "")
            print(f'{payload["ts"]} AGENT={agent:<16} ACTION={action:<24} key={key} node={node}')
        return payload

    @classmethod
    def log(cls, step_no: int, message: str, **fields):
        payload = {
            "kind": "step",
            "step": step_no,
            "text": f"Log {step_no} - {message}",
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            **fields
        }
        cls._write(payload)
        if cls.TO_CONSOLE:
            key = fields.get("key", "")
            node = fields.get("node", "")
            print(f'{payload["ts"]} {payload["text"]:<45} key={key} node={node}')
        return payload
# ---- END REPLACE ----
