# -*- coding: utf-8 -*-
"""
ran_realtime_simulator_autosave.py

Real-time RAN simulator with autosave + live reload + robust CSV handling.

What's included
---------------
- Autosave to CSV:
  Writes back to data/ran_network_performance.csv and data/ran_network_alarms.csv
  after each change (atomic writes using .tmp + replace).

- Live reload:
  If you manually edit either CSV while it runs, it detects the file mtime change
  and reloads the freshest state.

- Robust header parsing:
  Normalizes CSV headers (case/space/underscore variants) so we never miss fields.
  Guarantees we never emit alarms with empty Network_Element (fallback derived).

- Performance notifications every tick:
  Emits a PERF_NOTIFICATION every perf interval (default 30s), including:
    action: "DEGRADE" | "IMPROVE" | "RESET_HEALTHY"
    health_change: true/false
  so you can reliably detect performance degradation and improvement events.

CSV files (must exist):
  - data/ran_network_alarms.csv
  - data/ran_network_performance.csv

Run
---
python ran_realtime_simulator_autosave.py \
  --data-dir data \
  --perf-interval 30 \
  --alarm-interval 60 \
  --autosave-perf --autosave-alarms --watch-csv
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import random
import re
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ISO_EVT_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"  # JSON event timestamps
CSV_TS_FMT = "%Y-%m-%d %H:%M:%S"       # CSV timestamps to match seed files


# ------------------------------ Data Models ---------------------------------
class Severity(str, Enum):
    Warning = "Warning"
    Minor = "Minor"
    Major = "Major"
    Critical = "Critical"


@dataclass
class Alarm:
    alarm_id: str
    severity: Severity
    network_element: str
    site_id: str
    alarm_type: str
    description: str
    status: str  # "Active" | "Cleared"
    ip_address: str
    timestamp: datetime
    clear_timestamp: Optional[datetime] = None
    tt_status: Optional[str] = None

    def to_event(self) -> Dict:
        return {
            "type": "ALARM_EVENT",
            "timestamp": now_iso(),
            "alarm": {
                "Alarm_ID": self.alarm_id,
                "Severity": self.severity.value,
                "Network_Element": self.network_element,
                "Site_ID": self.site_id,
                "Alarm_Type": self.alarm_type,
                "Description": self.description,
                "Status": self.status,
                "IP_Address": self.ip_address,
                "TimeStamp": self.timestamp.astimezone(timezone.utc).strftime(ISO_EVT_FMT),
                "ClearTimeStamp": self.clear_timestamp.astimezone(timezone.utc).strftime(ISO_EVT_FMT) if self.clear_timestamp else None,
                "TTStatus": self.tt_status,
            },
        }


@dataclass
class NodePerf:
    network_element: str
    site_id: str
    throughput_mbps: float
    latency_ms: float
    cdr_percent: float
    ho_sr_percent: float
    prb_util_percent: float
    rrc_attempts: int
    rrc_success: int

    last_health: Optional[str] = None

    def rrc_sr_percent(self) -> float:
        return (100.0 * self.rrc_success / self.rrc_attempts) if self.rrc_attempts > 0 else 0.0


@dataclass
class Thresholds:
    PERF_TP_LOW: float = 100.0      # Mbps
    PERF_LAT_HIGH: float = 80.0     # ms
    PERF_CDR_HIGH: float = 1.5      # %
    PERF_HO_SR_LOW: float = 95.0    # %
    PERF_PRB_HIGH: float = 85.0     # %
    PERF_RRC_SR_LOW: float = 90.0   # %


# ------------------------------ Utilities -----------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO_EVT_FMT)

def _norm_key(k: str) -> str:
    """Normalize CSV header keys (strip, lowercase, remove non-alnum)."""
    return re.sub(r'[^a-z0-9]', '', (k or '').strip().lower())

def get_val(row: Dict[str, str], *candidates: str, default: str = "") -> str:
    """Return the first matching value from row by trying several header variants."""
    norm = {_norm_key(k): v for k, v in row.items()}
    for c in candidates:
        v = norm.get(_norm_key(c))
        if v is not None:
            return v
    return default

def parse_float_val(row: Dict[str, str], default: float, *names: str) -> float:
    s = get_val(row, *names, default=None)
    if s is None or s == "":
        return default
    try:
        return float(s)
    except Exception:
        return default

def parse_int_val(row: Dict[str, str], default: int, *names: str) -> int:
    s = get_val(row, *names, default=None)
    if s is None or s == "":
        return default
    try:
        return int(float(s))
    except Exception:
        return default


# ------------------------------ Loaders/Savers -------------------------------
PERF_HEADER = [
    "Network_Element",
    "Site_ID",
    "Throughput_Mbps",
    "Latency_ms",
    "Call_Drop_Rate_%",
    "Handover_Success_Rate_%",
    "PRB_Utilization_%",
    "RRC_Connection_Attempts",
    "Successful_RRC_Connections",
]

ALARM_HEADER = [
    "Timestamp",
    "Alarm_ID",
    "Severity",
    "Network_Element",
    "Site_ID",
    "Alarm_Type",
    "Description",
    "Status",
    "IP_Address",
    "ClearTimeStamp",
    "TTStatus",
]

def load_performance_csv(path: Path) -> List[NodePerf]:
    nodes: List[NodePerf] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            node = NodePerf(
                network_element=get_val(row, "Network_Element", "Network Element", "NE"),
                site_id=get_val(row, "Site_ID", "Site Id", "Site"),
                throughput_mbps=parse_float_val(row, 0.0, "Throughput_Mbps", "Throughput Mbps", "TP"),
                latency_ms=parse_float_val(row, 0.0, "Latency_ms", "Latency ms", "Latency"),
                cdr_percent=parse_float_val(row, 0.0, "Call_Drop_Rate_%", "Call Drop Rate %", "CDR_%", "CDR"),
                ho_sr_percent=parse_float_val(row, 0.0, "Handover_Success_Rate_%", "Handover Success Rate %", "HO_SR_%"),
                prb_util_percent=parse_float_val(row, 0.0, "PRB_Utilization_%", "PRB Utilization %", "PRB_%"),
                rrc_attempts=parse_int_val(row, 0, "RRC_Connection_Attempts", "RRC Connection Attempts", "RRC_Attempts"),
                rrc_success=parse_int_val(row, 0, "Successful_RRC_Connections", "Successful RRC Connections", "RRC_Success"),
            )
            nodes.append(node)
    return nodes

def save_performance_csv(path: Path, nodes: List[NodePerf]):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PERF_HEADER)
        w.writeheader()
        for n in nodes:
            w.writerow({
                "Network_Element": n.network_element,
                "Site_ID": n.site_id,
                "Throughput_Mbps": f"{n.throughput_mbps:.2f}",
                "Latency_ms": f"{n.latency_ms:.2f}",
                "Call_Drop_Rate_%": f"{n.cdr_percent:.2f}",
                "Handover_Success_Rate_%": f"{n.ho_sr_percent:.2f}",
                "PRB_Utilization_%": f"{n.prb_util_percent:.2f}",
                "RRC_Connection_Attempts": n.rrc_attempts,
                "Successful_RRC_Connections": n.rrc_success,
            })
    os.replace(tmp, path)

def load_alarms_csv(path: Path) -> List[Alarm]:
    alarms: List[Alarm] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            def _parse(ts: Optional[str]) -> Optional[datetime]:
                if not ts:
                    return None
                try:
                    return datetime.strptime(ts, CSV_TS_FMT)
                except Exception:
                    return None

            ts = _parse(get_val(row, "Timestamp")) or datetime.now()
            clr = _parse(get_val(row, "ClearTimeStamp"))
            sev = get_val(row, "Severity", default="Warning")
            if sev not in (s.value for s in Severity):
                sev = "Warning"
            alarm = Alarm(
                alarm_id=get_val(row, "Alarm_ID", "Alarm ID"),
                severity=Severity(sev),
                network_element=get_val(row, "Network_Element", "Network Element"),
                site_id=get_val(row, "Site_ID", "Site Id", "Site"),
                alarm_type=get_val(row, "Alarm_Type", "Alarm Type"),
                description=get_val(row, "Description"),
                status=get_val(row, "Status", default="Active"),
                ip_address=get_val(row, "IP_Address", "IP Address", default="0.0.0.0"),
                timestamp=ts,
                clear_timestamp=clr,
                tt_status=get_val(row, "TTStatus", "TT Status"),
            )
            alarms.append(alarm)
    return alarms

def save_alarms_csv(path: Path, alarms: List[Alarm]):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ALARM_HEADER)
        w.writeheader()
        for a in alarms:
            w.writerow({
                "Timestamp": a.timestamp.strftime(CSV_TS_FMT),
                "Alarm_ID": a.alarm_id,
                "Severity": a.severity.value,
                "Network_Element": a.network_element,
                "Site_ID": a.site_id,
                "Alarm_Type": a.alarm_type,
                "Description": a.description,
                "Status": a.status,
                "IP_Address": a.ip_address,
                "ClearTimeStamp": a.clear_timestamp.strftime(CSV_TS_FMT) if a.clear_timestamp else "",
                "TTStatus": a.tt_status or "",
            })
    os.replace(tmp, path)


# -------------------------- Health Evaluation --------------------------------
def evaluate_health(node: NodePerf, th: Thresholds) -> Tuple[str, Dict[str, Tuple[str, float, float]]]:
    violations: Dict[str, Tuple[str, float, float]] = {}

    if node.throughput_mbps < th.PERF_TP_LOW:
        violations["Throughput_Mbps"] = ("LOW", node.throughput_mbps, th.PERF_TP_LOW)
    if node.latency_ms > th.PERF_LAT_HIGH:
        violations["Latency_ms"] = ("HIGH", node.latency_ms, th.PERF_LAT_HIGH)
    if node.cdr_percent > th.PERF_CDR_HIGH:
        violations["Call_Drop_Rate_%"] = ("HIGH", node.cdr_percent, th.PERF_CDR_HIGH)
    if node.ho_sr_percent < th.PERF_HO_SR_LOW:
        violations["Handover_Success_Rate_%"] = ("LOW", node.ho_sr_percent, th.PERF_HO_SR_LOW)
    if node.prb_util_percent > th.PERF_PRB_HIGH:
        violations["PRB_Utilization_%"] = ("HIGH", node.prb_util_percent, th.PERF_PRB_HIGH)
    rrc_sr = node.rrc_sr_percent()
    if rrc_sr < th.PERF_RRC_SR_LOW:
        violations["RRC_SR_%"] = ("LOW", rrc_sr, th.PERF_RRC_SR_LOW)

    if not violations:
        return "HEALTHY", violations

    severe = (
        node.throughput_mbps < max(20.0, th.PERF_TP_LOW * 0.5)
        or node.latency_ms > max(150.0, th.PERF_LAT_HIGH * 1.5)
        or node.cdr_percent > max(5.0, th.PERF_CDR_HIGH * 2)
        or node.ho_sr_percent < min(85.0, th.PERF_HO_SR_LOW - 10)
        or node.prb_util_percent > min(95.0, th.PERF_PRB_HIGH + 10)
        or rrc_sr < min(80.0, th.PERF_RRC_SR_LOW - 10)
    )
    if severe or len(violations) >= 3:
        return "CRITICAL", violations
    return "DEGRADED", violations


# --------------------------- Notification Agent ------------------------------
class NotificationAgent:
    def notify_perf_change(
        self,
        node: NodePerf,
        prev: Optional[str],
        curr: str,
        violations: Dict[str, Tuple[str, float, float]],
        action: str,
        health_change: bool,
    ):
        kind = "HEALTHY" if curr == "HEALTHY" else (
            "IMPROVEMENT" if prev and _health_rank(curr) < _health_rank(prev) else "DEGRADATION"
        )
        payload = {
            "type": "PERF_NOTIFICATION",
            "timestamp": now_iso(),
            "action": action,
            "health_change": health_change,
            "change": kind,
            "node": node.network_element,
            "site": node.site_id,
            "current_health": curr,
            "previous_health": prev,
            "kpis": {
                "Throughput_Mbps": round(node.throughput_mbps, 2),
                "Latency_ms": round(node.latency_ms, 2),
                "CDR_%": round(node.cdr_percent, 2),
                "HO_SR_%": round(node.ho_sr_percent, 2),
                "PRB_%": round(node.prb_util_percent, 2),
                "RRC_SR_%": round(node.rrc_sr_percent(), 2),
            },
            "violations": {k: {"dir": v[0], "value": round(v[1], 2), "threshold": v[2]} for k, v in violations.items()},
        }
        print(json.dumps(payload))

def _health_rank(h: str) -> int:
    return {"HEALTHY": 0, "DEGRADED": 1, "CRITICAL": 2}.get(h, 1)


# --------------------------- Mutation Helpers --------------------------------
ALARM_TYPES = [
    "Power Failure",
    "License Expiry",
    "Cell Down",
    "Link Down",
    "Temperature High",
]

IP_OCTETS = list(range(1, 255))

def random_ip() -> str:
    return f"192.168.{random.choice(IP_OCTETS)}.{random.choice(IP_OCTETS)}"

def mutate_performance(node: NodePerf, th: Thresholds) -> str:
    action = random.choices(population=["DEGRADE", "IMPROVE", "RESET_HEALTHY"], weights=[0.5, 0.4, 0.1], k=1)[0]

    metrics = ["TP", "LAT", "CDR", "HO", "PRB", "RRC"]
    k = random.choice([1, 1, 2, 2, 3])
    chosen = random.sample(metrics, k)

    def clamp(v, lo, hi):
        return max(lo, min(hi, v))

    for m in chosen:
        if m == "TP":
            if action == "DEGRADE":
                node.throughput_mbps = clamp(node.throughput_mbps * random.uniform(0.4, 0.9), 1.0, 1000.0)
            elif action == "IMPROVE":
                node.throughput_mbps = clamp(max(node.throughput_mbps, th.PERF_TP_LOW * random.uniform(1.05, 1.5)), 1.0, 2000.0)
            else:
                node.throughput_mbps = max(th.PERF_TP_LOW * 1.3, 200.0)
        elif m == "LAT":
            if action == "DEGRADE":
                node.latency_ms = clamp(node.latency_ms + random.uniform(15, 120), 1.0, 400.0)
            elif action == "IMPROVE":
                node.latency_ms = clamp(node.latency_ms - random.uniform(10, 60), 1.0, 400.0)
            else:
                node.latency_ms = min(th.PERF_LAT_HIGH * 0.5, 30.0)
        elif m == "CDR":
            if action == "DEGRADE":
                node.cdr_percent = clamp(node.cdr_percent + random.uniform(0.5, 3.0), 0.0, 20.0)
            elif action == "IMPROVE":
                node.cdr_percent = clamp(node.cdr_percent - random.uniform(0.3, 2.0), 0.0, 20.0)
            else:
                node.cdr_percent = min(th.PERF_CDR_HIGH * 0.4, 0.5)
        elif m == "HO":
            if action == "DEGRADE":
                node.ho_sr_percent = clamp(node.ho_sr_percent - random.uniform(2, 12), 50.0, 100.0)
            elif action == "IMPROVE":
                node.ho_sr_percent = clamp(node.ho_sr_percent + random.uniform(2, 8), 50.0, 100.0)
            else:
                node.ho_sr_percent = max(98.0, th.PERF_HO_SR_LOW + 3)
        elif m == "PRB":
            if action == "DEGRADE":
                node.prb_util_percent = clamp(node.prb_util_percent + random.uniform(5, 20), 1.0, 100.0)
            elif action == "IMPROVE":
                node.prb_util_percent = clamp(node.prb_util_percent - random.uniform(5, 15), 1.0, 100.0)
            else:
                node.prb_util_percent = min(60.0, th.PERF_PRB_HIGH - 20)
        elif m == "RRC":
            if action == "DEGRADE":
                target = random.uniform(60.0, max(85.0, th.PERF_RRC_SR_LOW - 1))
            elif action == "IMPROVE":
                target = random.uniform(th.PERF_RRC_SR_LOW + 2, 99.0)
            else:
                target = max(98.0, th.PERF_RRC_SR_LOW + 5)
            attempts = max(100, int(node.rrc_attempts * random.uniform(0.8, 1.2)))
            success = int(attempts * (target / 100.0))
            node.rrc_attempts = attempts
            node.rrc_success = min(success, attempts)

    return action


# ------------------------------- Simulator -----------------------------------
@dataclass
class Simulator:
    data_dir: Path
    perf_interval: float
    alarm_interval: float
    thresholds: Thresholds
    autosave_perf: bool = True
    autosave_alarms: bool = True
    watch_csv: bool = True
    seed: Optional[int] = None

    # runtime state
    nodes: List[NodePerf] = field(default_factory=list)
    alarms: List[Alarm] = field(default_factory=list)
    active_alarms_idx: List[int] = field(default_factory=list)
    perf_csv_mtime: float = 0.0
    alarm_csv_mtime: float = 0.0

    next_alarm_id: int = 20000

    # paths
    perf_csv: Path = field(init=False)
    alarm_csv: Path = field(init=False)

    def __post_init__(self):
        self.perf_csv = self.data_dir / "ran_network_performance.csv"
        self.alarm_csv = self.data_dir / "ran_network_alarms.csv"

    def load(self):
        if not self.perf_csv.exists():
            raise FileNotFoundError(f"Performance CSV not found: {self.perf_csv}")
        if not self.alarm_csv.exists():
            raise FileNotFoundError(f"Alarms CSV not found: {self.alarm_csv}")

        self.nodes = load_performance_csv(self.perf_csv)
        self.alarms = load_alarms_csv(self.alarm_csv)
        self.active_alarms_idx = [i for i, a in enumerate(self.alarms) if a.status.strip().lower() == "active"]

        # Seed last health for nodes
        for n in self.nodes:
            h, _ = evaluate_health(n, self.thresholds)
            n.last_health = h

        self._snap_mtimes()

    def _snap_mtimes(self):
        try:
            self.perf_csv_mtime = self.perf_csv.stat().st_mtime
        except Exception:
            self.perf_csv_mtime = 0.0
        try:
            self.alarm_csv_mtime = self.alarm_csv.stat().st_mtime
        except Exception:
            self.alarm_csv_mtime = 0.0

    def _gen_alarm_id(self) -> str:
        aid = f"SIM{self.next_alarm_id}"
        self.next_alarm_id += 1
        return aid

    async def start(self):
        if self.seed is not None:
            random.seed(self.seed)
        self.load()
        print(json.dumps({
            "type": "SIMULATOR_START",
            "timestamp": now_iso(),
            "nodes": len(self.nodes),
            "initial_active_alarms": len(self.active_alarms_idx),
            "perf_interval_sec": self.perf_interval,
            "alarm_interval_sec": self.alarm_interval,
            "autosave_perf": self.autosave_perf,
            "autosave_alarms": self.autosave_alarms,
            "watch_csv": self.watch_csv,
        }))

        stop_event = asyncio.Event()

        def _handle_sig(*_):
            stop_event.set()
        try:
            loop = asyncio.get_running_loop()
            for s in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(s, _handle_sig)
                except NotImplementedError:
                    signal.signal(s, lambda *_: stop_event.set())
        except RuntimeError:
            pass

        perf_task = asyncio.create_task(self._perf_loop(stop_event))
        alarm_task = asyncio.create_task(self._alarm_loop(stop_event))

        await stop_event.wait()
        for t in (perf_task, alarm_task):
            t.cancel()
        await asyncio.gather(perf_task, alarm_task, return_exceptions=True)
        print(json.dumps({"type": "SIMULATOR_STOP", "timestamp": now_iso()}))

    async def _maybe_reload_csv(self):
        if not self.watch_csv:
            return
        # Reload perf if mtime changed
        try:
            m = self.perf_csv.stat().st_mtime
            if m != self.perf_csv_mtime:
                self.nodes = load_performance_csv(self.perf_csv)
                for n in self.nodes:
                    h, _ = evaluate_health(n, self.thresholds)
                    n.last_health = h
                self.perf_csv_mtime = m
                print(json.dumps({"type": "RELOAD", "resource": "performance_csv", "timestamp": now_iso(), "nodes": len(self.nodes)}))
        except Exception as e:
            print(json.dumps({"type": "ERROR", "source": "reload_perf", "error": str(e), "timestamp": now_iso()}), file=sys.stderr)
        # Reload alarms if mtime changed
        try:
            m = self.alarm_csv.stat().st_mtime
            if m != self.alarm_csv_mtime:
                self.alarms = load_alarms_csv(self.alarm_csv)
                self.active_alarms_idx = [i for i, a in enumerate(self.alarms) if a.status.strip().lower() == "active"]
                self.alarm_csv_mtime = m
                print(json.dumps({"type": "RELOAD", "resource": "alarms_csv", "timestamp": now_iso(), "active_alarms": len(self.active_alarms_idx)}))
        except Exception as e:
            print(json.dumps({"type": "ERROR", "source": "reload_alarm", "error": str(e), "timestamp": now_iso()}), file=sys.stderr)

    async def _perf_loop(self, stop_event: asyncio.Event):
        agent = NotificationAgent()
        while not stop_event.is_set():
            try:
                await self._maybe_reload_csv()
                node = random.choice(self.nodes)
                action = mutate_performance(node, self.thresholds)
                curr, violations = evaluate_health(node, self.thresholds)
                prev = node.last_health
                health_change = (prev != curr)

                # ALWAYS notify each tick
                agent.notify_perf_change(node, prev, curr, violations, action, health_change)
                node.last_health = curr

                if self.autosave_perf:
                    save_performance_csv(self.perf_csv, self.nodes)
                    self._snap_mtimes()
                await asyncio.wait_for(asyncio.sleep(self.perf_interval), timeout=self.perf_interval + 2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(json.dumps({"type": "ERROR", "source": "perf_loop", "error": str(e), "timestamp": now_iso()}), file=sys.stderr)
                await asyncio.sleep(self.perf_interval)

    async def _alarm_loop(self, stop_event: asyncio.Event):
        toggle = True
        while not stop_event.is_set():
            try:
                await self._maybe_reload_csv()
                # Create or clear alternately (ensures one event every alarm_interval)
                if toggle or not self.active_alarms_idx:
                    self._create_alarm()
                else:
                    self._clear_random_alarm()

                # Optionally change severity
                if self.active_alarms_idx and random.random() < 0.3:
                    self._change_severity_random_alarm()

                if self.autosave_alarms:
                    save_alarms_csv(self.alarm_csv, self.alarms)
                    self._snap_mtimes()

                toggle = not toggle
                await asyncio.wait_for(asyncio.sleep(self.alarm_interval), timeout=self.alarm_interval + 2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(json.dumps({"type": "ERROR", "source": "alarm_loop", "error": str(e), "timestamp": now_iso()}), file=sys.stderr)
                await asyncio.sleep(self.alarm_interval)

    def _create_alarm(self):
        node = random.choice(self.nodes)

        # Guarantee Network_Element and Site_ID are not empty
        ne = (node.network_element or
              next((n.network_element for n in self.nodes if n.site_id == node.site_id and n.network_element), None) or
              (f"NE_{node.site_id}" if node.site_id else "NE_UNKNOWN"))
        site = (node.site_id or
                next((n.site_id for n in self.nodes if n.network_element == ne and n.site_id), None) or
                "UNKNOWN")

        alarm = Alarm(
            alarm_id=self._gen_alarm_id(),
            severity=random.choice(list(Severity)),
            network_element=ne,
            site_id=site,
            alarm_type=random.choice(ALARM_TYPES),
            description="Simulated event",
            status="Active",
            ip_address=f"192.168.{random.randint(1,254)}.{random.randint(1,254)}",
            timestamp=datetime.now(),
        )
        self.alarms.append(alarm)
        self.active_alarms_idx.append(len(self.alarms) - 1)
        print(json.dumps(alarm.to_event()))

    def _clear_random_alarm(self):
        if not self.active_alarms_idx:
            return
        idx_pos = random.randrange(len(self.active_alarms_idx))
        aidx = self.active_alarms_idx.pop(idx_pos)
        alarm = self.alarms[aidx]
        alarm.status = "Cleared"
        alarm.clear_timestamp = datetime.now()
        print(json.dumps(alarm.to_event()))

    def _change_severity_random_alarm(self):
        if not self.active_alarms_idx:
            return
        aidx = random.choice(self.active_alarms_idx)
        alarm = self.alarms[aidx]
        severities = [Severity.Warning, Severity.Minor, Severity.Major, Severity.Critical]
        cur = severities.index(alarm.severity)
        if random.random() < 0.6 and cur < len(severities) - 1:
            alarm.severity = severities[cur + 1]
        elif cur > 0:
            alarm.severity = severities[cur - 1]
        evt = alarm.to_event()
        evt["alarm"]["SeverityChange"] = True
        print(json.dumps(evt))


# ------------------------------- CLI ----------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Real-time RAN alarm & performance simulator with autosave and live reload")
    p.add_argument("--data-dir", type=Path, default=Path("data"), help="Folder with ran_network_alarms.csv and ran_network_performance.csv")
    p.add_argument("--perf-interval", type=float, default=30.0, help="Seconds between performance changes (default 30)")
    p.add_argument("--alarm-interval", type=float, default=60.0, help="Seconds between alarm create/clear events (default 60)")
    p.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    p.add_argument("--autosave-perf", action="store_true", help="Autosave performance CSV after each change")
    p.add_argument("--no-autosave-perf", dest="autosave_perf", action="store_false")
    p.set_defaults(autosave_perf=True)
    p.add_argument("--autosave-alarms", action="store_true", help="Autosave alarms CSV after each change")
    p.add_argument("--no-autosave-alarms", dest="autosave_alarms", action="store_false")
    p.set_defaults(autosave_alarms=True)
    p.add_argument("--watch-csv", action="store_true", help="Reload CSVs if modified manually while running")
    p.add_argument("--no-watch-csv", dest="watch_csv", action="store_false")
    p.set_defaults(watch_csv=True)

    # Threshold overrides
    p.add_argument("--tp-low", type=float, default=100.0)
    p.add_argument("--lat-high", type=float, default=80.0)
    p.add_argument("--cdr-high", type=float, default=1.5)
    p.add_argument("--ho-sr-low", type=float, default=95.0)
    p.add_argument("--prb-high", type=float, default=85.0)
    p.add_argument("--rrc-sr-low", type=float, default=90.0)
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    th = Thresholds(
        PERF_TP_LOW=args.tp_low,
        PERF_LAT_HIGH=args.lat_high,
        PERF_CDR_HIGH=args.cdr_high,
        PERF_HO_SR_LOW=args.ho_sr_low,
        PERF_PRB_HIGH=args.prb_high,
        PERF_RRC_SR_LOW=args.rrc_sr_low,
    )
    sim = Simulator(
        data_dir=args.data_dir,
        perf_interval=args.perf_interval,
        alarm_interval=args.alarm_interval,
        thresholds=th,
        autosave_perf=args.autosave_perf,
        autosave_alarms=args.autosave_alarms,
        watch_csv=args.watch_csv,
        seed=args.seed,
    )
    try:
        asyncio.run(sim.start())
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
