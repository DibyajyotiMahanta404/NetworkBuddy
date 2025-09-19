import React, { useEffect, useRef, useState } from "react";
import AgentDiagram from "@/components/AgentDiagram"; // default export from your diagram file
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Send } from "lucide-react";

/* -------------------- Types -------------------- */
type NodeStatus = "idle" | "queued" | "running" | "success" | "failed";

type FlowNode = {
  id: string;
  label: string;
  type: "orchestrator" | "agent";
  status?: NodeStatus;
  meta?: Record<string, any>;
};

type FlowEdge = {
  id: string;
  from: string;
  to: string;
  status?: "enqueued" | "started" | "finished" | "failed";
  reason?: string;
  elapsed_ms?: number;
  produced?: string[];
};

type FlowState = {
  requestId: string | null;
  nodes: FlowNode[];
  edges: FlowEdge[];
};

/* -------------------- Constants -------------------- */
const AGENT_LABELS: Record<string, string> = {
  orchestrator: "Orchestrator",
  fault_agent: "Fault",
  performance_agent: "Performance",
  inventory_agent: "Inventory",
  data_flow_agent: "Data Flow",
  customer_agent: "Customer",
  ticketing_agent: "Ticketing",
};

const STATIC_AGENTS = [
  "fault_agent",
  "performance_agent",
  "inventory_agent",
  "data_flow_agent",
  "customer_agent",
  "ticketing_agent",
];

// Plain-text log fallbacks (if any non-JSON lines come through)
const AGENT_START_RX = /Calling agent:\s*([a-zA-Z_]+)\s*$/;
const AGENT_DONE_RX = /^([a-zA-Z_]+)\s*✓\s*(\d+)ms\b.*(?:keys=)?(.*)?$/;

/* -------------------- Stable session id -------------------- */
const SESSION_STORAGE_KEY = "oss_session_id";

/** Returns a stable session id. Creates one and stores in localStorage if absent. */
function getStableSessionId(): string {
  try {
    const existing = localStorage.getItem(SESSION_STORAGE_KEY);
    if (existing && existing.trim()) return existing;
    const fresh = "web_session_" + Math.random().toString(36).slice(2, 10);
    localStorage.setItem(SESSION_STORAGE_KEY, fresh);
    return fresh;
  } catch {
    // Fallback if localStorage is blocked
    return "web_session_" + Math.random().toString(36).slice(2, 10);
  }
}

/* -------------------- Static node manifest -------------------- */
function buildStaticNodes(): Map<string, FlowNode> {
  const map = new Map<string, FlowNode>();
  map.set("orchestrator", {
    id: "orchestrator",
    label: AGENT_LABELS["orchestrator"],
    type: "orchestrator",
    status: "idle",
  });
  for (const id of STATIC_AGENTS) {
    map.set(id, {
      id,
      label: AGENT_LABELS[id] || id.replace(/_/g, " "),
      type: "agent",
      status: "idle",
    });
  }
  return map;
}

const Index: React.FC = () => {
  const [networkQuery, setNetworkQuery] = useState("");
  const [isProcessing, setIsProcessing] = useState(false);
  const [logs, setLogs] = useState<any[]>([]);
  const [isWsConnected, setIsWsConnected] = useState(false);

  // Stable session id (so main.py can reuse chat history)
  const sessionIdRef = useRef<string>(getStableSessionId());

  /* -------------------- Flow graph state (static nodes, dynamic edges) -------------------- */
  const [flow, setFlow] = useState<FlowState>({
    requestId: null,
    nodes: Array.from(buildStaticNodes().values()),
    edges: [],
  });

  // Mutable store (reduces re-renders on bursts)
  const storeRef = useRef<{
    requestId: string | null;
    nodes: Map<string, FlowNode>;
    edges: Map<string, FlowEdge>;
    finishTimer: any | null;
  }>({
    requestId: null,
    nodes: buildStaticNodes(),
    edges: new Map(),
    finishTimer: null,
  });

  /* -------------------- Flow helpers -------------------- */
  const publishFlow = () => {
    setFlow({
      requestId: storeRef.current.requestId,
      nodes: Array.from(storeRef.current.nodes.values()),
      edges: Array.from(storeRef.current.edges.values()),
    });
  };

  const setAllStatuses = (status: NodeStatus) => {
    for (const [id, n] of storeRef.current.nodes) {
      storeRef.current.nodes.set(id, { ...n, status });
    }
  };

  const setNodeStatus = (id: string, status: NodeStatus) => {
    const n = storeRef.current.nodes.get(id);
    if (!n) return;
    storeRef.current.nodes.set(id, { ...n, status });
  };

  const clearEdges = () => {
    storeRef.current.edges.clear();
  };

  const upsertEdge = (from: string, to: string, status?: FlowEdge["status"], reason?: string) => {
    const rid = storeRef.current.requestId || "req";
    const id = `${rid}:${from}->${to}`;
    const prev = storeRef.current.edges.get(id);
    const merged: FlowEdge = {
      id,
      from,
      to,
      status: status ?? prev?.status ?? "started",
      reason: reason ?? prev?.reason,
      elapsed_ms: prev?.elapsed_ms,
      produced: prev?.produced ?? [],
    };
    storeRef.current.edges.set(id, merged);
  };

  const finishAndReset = (delayMs = 900) => {
    if (storeRef.current.finishTimer) {
      clearTimeout(storeRef.current.finishTimer);
      storeRef.current.finishTimer = null;
    }
    storeRef.current.finishTimer = setTimeout(() => {
      setAllStatuses("idle");
      clearEdges();
      publishFlow();
    }, delayMs);
  };

  const resetForRequest = (requestId: string | null) => {
    storeRef.current.requestId = requestId;
    setAllStatuses("idle");
    clearEdges();
    setNodeStatus("orchestrator", "running"); // show planning in progress
    publishFlow();
  };

  /* -------------------- Structured flow events from backend -------------------- */
  const applyFlowEvent = (evt: any) => {
    if (!evt || !evt.event) return;

    if (evt.event === "flow_started") {
      resetForRequest(evt.request_id || null);
      return;
    }

    if (!storeRef.current.requestId && evt.request_id) {
      storeRef.current.requestId = evt.request_id;
    }

    switch (evt.event) {
      case "flow_decision": {
        const agent = String(evt.agent || "").trim();
        if (!agent) break;
        setNodeStatus(agent, "queued");
        upsertEdge("orchestrator", agent, "enqueued", String(evt.reason || ""));
        publishFlow();
        break;
      }
      case "execution_seed": {
        const agents = (evt.agents || []) as string[];
        for (const a of agents) {
          setNodeStatus(a, "queued");
          upsertEdge("orchestrator", a, "enqueued", "seed");
        }
        publishFlow();
        break;
      }
      case "agent_started": {
        const agent = String(evt.agent || "").trim();
        setNodeStatus(agent, "running");
        upsertEdge("orchestrator", agent, "started");
        publishFlow();
        break;
      }
      case "agent_finished": {
        const agent = String(evt.agent || "").trim();
        setNodeStatus(agent, "success");
        const id = `${storeRef.current.requestId || "req"}:orchestrator->${agent}`;
        const e = storeRef.current.edges.get(id);
        storeRef.current.edges.set(id, {
          ...(e || { id, from: "orchestrator", to: agent }),
          status: "finished",
          elapsed_ms: evt.elapsed_ms,
          produced: Array.isArray(evt.produced) ? evt.produced : [],
        });
        publishFlow();
        break;
      }
      case "agent_failed": {
        const agent = String(evt.agent || "").trim();
        setNodeStatus(agent, "failed");
        upsertEdge("orchestrator", agent, "failed");
        publishFlow();
        break;
      }
      case "flow_finished": {
        setNodeStatus("orchestrator", "success");
        publishFlow();
        finishAndReset(1200);
        setIsProcessing(false);
        break;
      }
      default:
        break;
    }
  };

  /* -------------------- Fallback plain log parsing -------------------- */
  const applyPlainLog = (line: string) => {
    const start = line.match(AGENT_START_RX);
    if (start) {
      const agent = start[1].trim();
      setNodeStatus(agent, "running");
      upsertEdge("orchestrator", agent, "started");
      publishFlow();
      return;
    }
    const done = line.match(AGENT_DONE_RX);
    if (done) {
      const agent = done[1].trim();
      const ms = Number(done[2]);
      let produced: string[] = [];
      try {
        const rest = done[3] || "";
        const s = rest.indexOf("[");
        const e = rest.lastIndexOf("]");
        if (s >= 0 && e > s) {
          produced = rest.slice(s + 1, e).split(",").map((x) => x.trim()).filter(Boolean);
        }
      } catch {}
      setNodeStatus(agent, "success");
      const id = `${storeRef.current.requestId || "req"}:orchestrator->${agent}`;
      storeRef.current.edges.set(id, {
        id,
        from: "orchestrator",
        to: agent,
        status: "finished",
        elapsed_ms: ms,
        produced,
      });
      publishFlow();
    }
  };

  /* -------------------- WebSocket plumbing -------------------- */
  const wsRef = useRef<WebSocket | null>(null);

  const connectWS = () => {
    if (wsRef.current && (wsRef.current.readyState === WebSocket.OPEN || wsRef.current.readyState === WebSocket.CONNECTING)) return;

    const ws = new WebSocket("ws://localhost:8000/ws/chat");
    wsRef.current = ws;

    ws.onopen = () => setIsWsConnected(true);
    ws.onclose = () => setIsWsConnected(false);
    ws.onerror = () => setIsWsConnected(false);

    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);

        // Human-readable logs for the activity area
        if (msg.event === "log") {
          setLogs((prev) => [
            ...prev,
            {
              id: Date.now().toString(),
              timestamp: new Date().toLocaleTimeString(),
              level: (msg.level as string) || "info",
              source: msg.source || "Server",
              message: msg.line,
            },
          ]);
          // Also allow fallback progression from plain lines
          if (typeof msg.line === "string") {
            applyPlainLog(msg.line);
          }
          return;
        }

        // Structured flow events
        if (msg.event === "flow" && msg.data) {
          applyFlowEvent(msg.data);
          return;
        }

        // Server-side normalized agent_flow (not strictly needed with logger tap, kept for safety)
        if (msg.event === "agent_flow" && msg.data) {
          const d = msg.data;
          if (d.kind === "started") {
            applyPlainLog(`Calling agent: ${d.agent}`);
          } else if (d.kind === "finished") {
            const keysPart = d.produced?.length ? ` keys=[${d.produced.join(", ")}]` : "";
            applyPlainLog(`${d.agent} ✓ ${d.elapsed_ms}ms${keysPart}`);
          }
          return;
        }

        // Final
        if (msg.event === "final") {
          setLogs((prev) => [
            ...prev,
            {
              id: Date.now().toString(),
              timestamp: new Date().toLocaleTimeString(),
              level: "success",
              source: "Orchestrator",
              message: "Flow complete",
            },
          ]);
          setIsProcessing(false);
          return;
        }

        // Error
        if (msg.event === "error") {
          setLogs((prev) => [
            ...prev,
            {
              id: Date.now().toString(),
              timestamp: new Date().toLocaleTimeString(),
              level: "error",
              source: "System",
              message: msg.message || "WebSocket error",
            },
          ]);
          setIsProcessing(false);
          return;
        }
      } catch {
        /* ignore non-JSON frames */
      }
    };
  };

  useEffect(() => {
    connectWS();
    return () => {
      try {
        wsRef.current?.close();
      } catch {}
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /* -------------------- Send query over WS -------------------- */
  const handleSendQuery = async () => {
    const q = networkQuery.trim();
    if (!q) return;

    setLogs((prev) => [
      ...prev,
      {
        id: Date.now().toString(),
        timestamp: new Date().toLocaleTimeString(),
        level: "info",
        source: "User",
        message: `Query: ${q}`,
      },
    ]);

    setIsProcessing(true);
    // Prepare a fresh canvas; real request_id will arrive in flow_started
    resetForRequest(null);

    if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
      connectWS();
      await new Promise((r) => setTimeout(r, 150));
    }

    try {
      wsRef.current?.send(
        JSON.stringify({
          session_id: sessionIdRef.current, // <-- stable session id for chat memory
          user_input: q,
        })
      );
    } catch {
      setLogs((prev) => [
        ...prev,
        {
          id: Date.now().toString(),
          timestamp: new Date().toLocaleTimeString(),
          level: "error",
          source: "System",
          message: "Failed to send query over WebSocket.",
        },
      ]);
      setIsProcessing(false);
    }

    setNetworkQuery("");
  };

  /* -------------------- Render -------------------- */
  return (
    <div className="p-4 space-y-6">
      {/* Header */}
      <header className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-semibold">AI Agent Portal</h2>
          <p className="text-sm text-muted-foreground">Monitor and manage autonomous agents in real time</p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant={isWsConnected ? "default" : "secondary"}>WS: {isWsConnected ? "Connected" : "Disconnected"}</Badge>
          <Badge variant={isProcessing ? "default" : "secondary"}>{isProcessing ? "Processing…" : "Idle"}</Badge>
          {/* Debug (optional): <Badge variant="secondary">Session: {sessionIdRef.current}</Badge> */}
        </div>
      </header>

      {/* Agent Flow (static nodes, animated edges) */}
      <section>
        <h3 className="text-xl font-semibold">Agent Flow</h3>
        <p className="text-xs text-muted-foreground mb-2">
          All agents are shown; edges animate when Orchestrator connects to them during a flow.
        </p>

        {/* IMPORTANT: parent must have a fixed height for React Flow */}
        <div className="relative w-full h-[560px] rounded-lg border bg-white/40">
          <AgentDiagram nodes={flow.nodes} edges={flow.edges} />
        </div>
      </section>

      {/* Prompt */}
      <section>
        <h3 className="text-xl font-semibold">AI Assistant Agent</h3>
        <p className="text-xs text-muted-foreground mb-2">Ask about alarms, performance, inventory, data flow, customers, tickets…</p>
        <div className="flex items-start gap-2">
          <Textarea
            value={networkQuery}
            onChange={(e) => setNetworkQuery(e.target.value)}
            className="flex-1 min-h-[80px] resize-none"
            disabled={isProcessing}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                handleSendQuery();
              }
            }}
            placeholder="e.g., Show high-risk customers in the West region"
          />
          <Button onClick={handleSendQuery} disabled={isProcessing}>
            {isProcessing ? "Processing..." : <Send className="w-4 h-4" />}
          </Button>
        </div>
      </section>

      {/* Recent activity (compact) */}
      <section>
        <h3 className="text-xl font-semibold">Activity</h3>
        <div className="max-h-[260px] overflow-auto text-sm pr-1">
          {logs.map((log, i) => (
            <div key={i} className="text-muted-foreground">
              [{log.timestamp}] {log.source}: {log.message}
            </div>
          ))}
          {!logs.length && <div className="text-xs text-muted-foreground">No logs yet. Send a query to see activity.</div>}
        </div>
      </section>
    </div>
  );
};

export default Index;
