#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reset orchestrator logs/state safely.

Usage examples:
  python scripts/reset_logs.py --dry-run
  python scripts/reset_logs.py --yes
  python scripts/reset_logs.py --all --yes
"""

import os
import sys
import json
import argparse
from typing import List, Tuple

def _env_or_default(name: str, default: str) -> str:
    return os.getenv(name, default)

def _collect_targets(include_tickets: bool) -> List[Tuple[str, str]]:
    """Return list of (label, path) pairs to delete."""
    targets = [
        ("Orchestrator state", _env_or_default("BG_ORCH_STATE", "data/bg_state.json")),
        ("Process flow log", _env_or_default("PROCESS_FLOW_LOG", "data/process_flow.jsonl")),
        ("Notifications log", _env_or_default("NOTIFICATIONS_LOG", "data/notifications.jsonl")),
        ("BG Orchestrator events", _env_or_default("BG_ORCH_EVENTS_LOG", "data/bg_orch_events.jsonl")),
        ("TT events", _env_or_default("TT_EVENTS_LOG", "data/tt_events.jsonl")),
    ]
    if include_tickets:
        targets += [
            ("Open tickets", _env_or_default("OPEN_TICKETS_PATH", "data/tickets_open.json")),
            ("Tickets index", _env_or_default("TICKETS_INDEX_PATH", "data/tickets_index.json")),
            ("Closed tickets audit", _env_or_default("CLOSED_TICKETS_LOG", "data/tickets_closed.jsonl")),
        ]
    return targets

def _fmt_exists(path: str) -> str:
    return "exists" if os.path.exists(path) else "missing"

def main():
    p = argparse.ArgumentParser(description="Reset orchestrator logs/state.")
    p.add_argument("--all", action="store_true", help="Also clear ticket stores (open/index/closed).")
    p.add_argument("--dry-run", action="store_true", help="Show what would be deleted, do not delete.")
    p.add_argument("--yes", action="store_true", help="Do not prompt for confirmation.")
    args = p.parse_args()

    targets = _collect_targets(include_tickets=args.all)

    print("Reset plan (environment overrides respected):")
    plan = []
    for label, path in targets:
        plan.append({"label": label, "path": path, "status": _fmt_exists(path)})
    print(json.dumps(plan, indent=2, ensure_ascii=False))

    if args.dry_run:
        print("\n[DRY-RUN] No files will be deleted.")
        return 0

    if not args.yes:
        confirm = input("\nThis will DELETE the files above if they exist. Type 'yes' to continue: ").strip().lower()
        if confirm != "yes":
            print("Aborted. Nothing deleted.")
            return 1

    deleted = []
    skipped = []
    for label, path in targets:
        try:
            if os.path.exists(path):
                os.remove(path)
                deleted.append((label, path))
            else:
                skipped.append((label, path))
        except Exception as e:
            print(f"[WARN] Failed to delete {path}: {e}", file=sys.stderr)

    print("\nDeleted:")
    for lbl, pth in deleted:
        print(f"  - {lbl}: {pth}")

    if skipped:
        print("\nSkipped (missing):")
        for lbl, pth in skipped:
            print(f"  - {lbl}: {pth}")

    print("\nDone.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
