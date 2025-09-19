#!/usr/bin/env python3
"""
Menu-driven tester for OSS AI Agent endpoints.

Endpoints covered:
  1) WS Chat (final only)                -> ws://<host>:<port>/ws/chat
  2) WS Chat (full payload)             -> ws://<host>:<port>/ws/chat
  3) WS TT Events (live stream)         -> ws://<host>:<port>/ws/events/tt
  4) WS Orchestrator Logs (live stream) -> ws://<host>:<port>/ws/logs/orchestrator
  5) REST: /tickets/open                -> http://<host>:<port>/tickets/open
  6) REST: /tickets/closed              -> http://<host>:<port>/tickets/closed?recent_hours=24

Usage:
  python ws_test.py
"""

import asyncio
import json
import os
import sys
from typing import List, Optional

try:
    import websockets  # pip install websockets
except ImportError:
    print("Missing dependency: websockets. Install: pip install websockets")
    sys.exit(1)

# REST is optional; we handle absence gracefully
try:
    import requests  # pip install requests
except Exception:
    requests = None

DEFAULT_HOST = os.getenv("OSS_API_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("OSS_API_PORT", "8080"))

def ws_base(host: str, port: int) -> str:
    return f"ws://{host}:{port}"

def http_base(host: str, port: int) -> str:
    return f"http://{host}:{port}"

def pretty(obj) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return str(obj)

async def ws_chat_final_only(host: str, port: int):
    url = f"{ws_base(host, port)}/ws/chat"
    print(f"\nConnecting to {url} ...")
    user_input = input("Enter your prompt: ").strip() or "Show alarms for ENodeB-1023"
    session_id = input("Enter session_id (default: web_session): ").strip() or "web_session"

    try:
        async with websockets.connect(url) as ws:
            await ws.send(json.dumps({"session_id": session_id, "user_input": user_input}))
            while True:
                msg = await ws.recv()
                data = json.loads(msg)
                if data.get("event") == "final":
                    payload = data.get("payload") or {}
                    result = payload.get("result") or "No result in payload."
                    print("\n=== FINAL RESULT ===\n")
                    print(result)
                    print("\n(request_id:", payload.get("request_id"), ")\n")
                    break
    except KeyboardInterrupt:
        print("\nCancelled by user.")
    except Exception as e:
        print(f"Error: {e}")

async def ws_chat_full_payload(host: str, port: int):
    url = f"{ws_base(host, port)}/ws/chat"
    print(f"\nConnecting to {url} ...")
    user_input = input("Enter your prompt: ").strip() or "Show alarms for ENodeB-1023"
    session_id = input("Enter session_id (default: web_session): ").strip() or "web_session"

    try:
        async with websockets.connect(url) as ws:
            await ws.send(json.dumps({"session_id": session_id, "user_input": user_input}))
            while True:
                msg = await ws.recv()
                data = json.loads(msg)
                if data.get("event") == "final":
                    payload = data.get("payload") or {}
                    print("\n=== FINAL PAYLOAD ===\n")
                    print(pretty(payload))
                    print()
                    break
    except KeyboardInterrupt:
        print("\nCancelled by user.")
    except Exception as e:
        print(f"Error: {e}")

async def ws_events_tt(host: str, port: int):
    url = f"{ws_base(host, port)}/ws/events/tt"
    print(f"\nConnecting to {url} ...")
    nodes_raw = input("Filter nodes (comma-separated, optional; leave blank for all): ").strip()
    nodes: Optional[List[str]] = None
    if nodes_raw:
        nodes = [x.strip() for x in nodes_raw.split(",") if x.strip()]

    try:
        async with websockets.connect(url) as ws:
            # optional filter message
            if nodes:
                await ws.send(json.dumps({"nodes": nodes}))
            print("\n=== STREAMING TT EVENTS (Ctrl+C to stop) ===\n")
            while True:
                line = await ws.recv()
                # server sends JSONL line or JSON; print as-is
                try:
                    obj = json.loads(line)
                    print(pretty(obj))
                except Exception:
                    print(line)
    except KeyboardInterrupt:
        print("\nStopped.")
    except Exception as e:
        print(f"Error: {e}")

async def ws_logs_orchestrator(host: str, port: int):
    url = f"{ws_base(host, port)}/ws/logs/orchestrator"
    print(f"\nConnecting to {url} ...")
    session_id = input("Filter by session_id (optional): ").strip()
    request_id = input("Filter by request_id (optional): ").strip()

    try:
        async with websockets.connect(url) as ws:
            # optional first message with filters
            filt = {}
            if session_id:
                filt["session_id"] = session_id
            if request_id:
                filt["request_id"] = request_id
            if filt:
                await ws.send(json.dumps(filt))

            print("\n=== STREAMING ORCHESTRATOR LOGS (Ctrl+C to stop) ===\n")
            while True:
                line = await ws.recv()
                try:
                    obj = json.loads(line)
                    print(pretty(obj))
                except Exception:
                    print(line)
    except KeyboardInterrupt:
        print("\nStopped.")
    except Exception as e:
        print(f"Error: {e}")

def rest_tickets_open(host: str, port: int):
    if requests is None:
        print("The 'requests' library is not installed. Install with: pip install requests")
        return
    url = f"{http_base(host, port)}/tickets/open"
    print(f"\nGET {url}")
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        print(pretty(r.json()))
    except Exception as e:
        print(f"Error: {e}")

def rest_tickets_closed(host: str, port: int):
    if requests is None:
        print("The 'requests' library is not installed. Install with: pip install requests")
        return
    recent = input("Recent hours (e.g., 24; blank for none): ").strip()
    params = {}
    if recent:
        params["recent_hours"] = recent
    url = f"{http_base(host, port)}/tickets/closed"
    print(f"\nGET {url} params={params or '{}'}")
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        print(pretty(r.json()))
    except Exception as e:
        print(f"Error: {e}")

def run_menu():
    host = DEFAULT_HOST
    port = DEFAULT_PORT

    while True:
        print("\n==============================")
        print(" OSS AI Agent — Test Menu")
        print("==============================")
        print(f" Server: {host}:{port}")
        print(" 1) WS Chat (final result only)")
        print(" 2) WS Chat (full payload)")
        print(" 3) WS TT Events (live)")
        print(" 4) WS Orchestrator Logs (live)")
        print(" 5) REST: /tickets/open")
        print(" 6) REST: /tickets/closed")
        print(" 7) Change server host/port")
        print(" 0) Exit")
        choice = input("Select an option: ").strip()

        if choice == "1":
            asyncio.run(ws_chat_final_only(host, port))
        elif choice == "2":
            asyncio.run(ws_chat_full_payload(host, port))
        elif choice == "3":
            asyncio.run(ws_events_tt(host, port))
        elif choice == "4":
            asyncio.run(ws_logs_orchestrator(host, port))
        elif choice == "5":
            rest_tickets_open(host, port)
        elif choice == "6":
            rest_tickets_closed(host, port)
        elif choice == "7":
            new_host = input(f"Host (current: {host}): ").strip() or host
            try:
                new_port = int(input(f"Port (current: {port}): ").strip() or str(port))
            except Exception:
                print("Invalid port, keeping previous.")
                new_port = port
            host, port = new_host, new_port
        elif choice == "0":
            print("Bye!")
            break
        else:
            print("Invalid choice, try again.")

if __name__ == "__main__":
    try:
        run_menu()
    except KeyboardInterrupt:
        print("\nBye!")
