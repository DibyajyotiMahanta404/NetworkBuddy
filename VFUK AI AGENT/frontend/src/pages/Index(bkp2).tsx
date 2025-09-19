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

// Log parsing regex
const AGENT_START_RX = /Calling agent:\s*([a-zA-Z_]+)\s*$/;
const AGENT_DONE_RX = /^([a-zA-Z_]+)\s*✓\s*(\d+)ms\b.*(?:keys=)?(.*)?$/;
// Optional: second line with keys=['x','y']
const KEYS_NEXTLINE_RX = /^\s*keys\s*=\s*\[([^\]]*)\]/;

// Special command to reset server/CLI
const RESET_COMMAND = "/reset";

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
      label: AGENT_LABELS[id] ?? id.replace(/_/g, " "),
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

  // Assistant result (right column)
  const [assistantText, setAssistantText] = useState<string>("");
  const [isStreaming, setIsStreaming] = useState<boolean>(false);

  // Stable session id (so main.py can reuse chat history)
  const sessionIdRef = useRef<string>(getStableSessionId());

  /* -------- Flow graph state (static nodes, dynamic edges) -------- */
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
    lastAgentForKeys?: string | null;
  }>({
    requestId: null,
    nodes: buildStaticNodes(),
    edges: new Map(),
    finishTimer: null,
    lastAgentForKeys: null,
  });

  // WebSocket + reconnect control
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimerRef = useRef<any>(null);

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

  const upsertEdge = (
    from: string,
    to: string,
    status?: FlowEdge["status"],
    reason?: string
  ) => {
    const rid = storeRef.current.requestId ?? "req";
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

  // ✅ Keep the final graph visible after completion (do not clear edges).
  // Only clear edges when a NEW run starts (resetForRequest).
  const finishAndReset = (delayMs = 900, { preserveEdges = true } = {}) => {
    if (storeRef.current.finishTimer) {
      clearTimeout(storeRef.current.finishTimer);
      storeRef.current.finishTimer = null;
    }

    storeRef.current.finishTimer = setTimeout(() => {
      // Safety: keep static nodes available
      if (storeRef.current.nodes.size === 0) {
        storeRef.current.nodes = buildStaticNodes();
      }
      setAllStatuses("idle");
      if (!preserveEdges) {
        clearEdges();
      }
      publishFlow();
    }, delayMs);
  };

  const resetForRequest = (requestId: string | null) => {
    // Defensive: ensure static nodes exist before starting the next run
    if (storeRef.current.nodes.size === 0) {
      storeRef.current.nodes = buildStaticNodes();
    }
    storeRef.current.requestId = requestId;
    setAllStatuses("idle");
    clearEdges();
    setNodeStatus("orchestrator", "running"); // show planning in progress
    publishFlow();
  };

  /* -------------------- Result helpers -------------------- */
  const handleAnswerEvent = (evt: any) => {
    const text =
      evt.answer ?? evt.answer_preview ?? evt.content ?? evt.text ?? "";
    if (text) {
      setAssistantText(text);
      setIsStreaming(false);
    }
  };

  const handleInlineResultBlock = (block: string) => {
    // If the frame contains "✅ Result:", capture everything after it
    const idx = block.indexOf("✅ Result:");
    if (idx >= 0) {
      const rest = block.slice(idx + "✅ Result:".length).trim();
      if (rest) {
        setAssistantText(rest);
        setIsStreaming(false);
      }
    }
  };

  /* -------- Structured flow events from backend -------- */
  const applyFlowEvent = (evt: any) => {
    if (!evt || !evt.event) return;

    if (evt.event === "flow_started") {
      resetForRequest(evt.request_id ?? null);
      return;
    }
    if (!storeRef.current.requestId && evt.request_id) {
      storeRef.current.requestId = evt.request_id;
    }

    switch (evt.event) {
      case "flow_decision": {
        const agent = String(evt.agent ?? "").trim();
        if (!agent) break;
        setNodeStatus(agent, "queued");
        upsertEdge("orchestrator", agent, "enqueued", String(evt.reason ?? ""));
        publishFlow();
        break;
      }
      case "execution_seed": {
        const agents = (evt.agents ?? []) as string[];
        for (const a of agents) {
          setNodeStatus(a, "queued");
          upsertEdge("orchestrator", a, "enqueued", "seed");
        }
        publishFlow();
        break;
      }
      case "agent_started": {
        const agent = String(evt.agent ?? "").trim();
        setNodeStatus(agent, "running");
        upsertEdge("orchestrator", agent, "started");
        publishFlow();
        break;
      }
      case "agent_finished": {
        const agent = String(evt.agent ?? "").trim();
        setNodeStatus(agent, "success");
        const id = `${storeRef.current.requestId ?? "req"}:orchestrator->${agent}`;
        const e = storeRef.current.edges.get(id);
        storeRef.current.edges.set(id, {
          ...(e ?? { id, from: "orchestrator", to: agent }),
          status: "finished",
          elapsed_ms: evt.elapsed_ms,
          produced: Array.isArray(evt.produced) ? evt.produced : [],
        });
        publishFlow();
        break;
      }
      case "agent_failed": {
        const agent = String(evt.agent ?? "").trim();
        setNodeStatus(agent, "failed");
        upsertEdge("orchestrator", agent, "failed");
        publishFlow();
        break;
      }
      case "answer_composed": {
        handleAnswerEvent(evt);
        break;
      }
      case "flow_finished": {
        setNodeStatus("orchestrator", "success");
        publishFlow();
        // Keep last run visible; edges cleared only when the next run starts.
        finishAndReset(1200, { preserveEdges: true });
        setIsProcessing(false);
        setIsStreaming(false);
        break;
      }
      default:
        // Ignore non-visual orchestration logs
        break;
    }
  };

  /* -------------------- Fallback plain log parsing -------------------- */
  const applyPlainLog = (line: string) => {
    // Agents start
    const start = line.match(AGENT_START_RX);
    if (start) {
      const agent = start[1].trim();
      setNodeStatus(agent, "running");
      upsertEdge("orchestrator", agent, "started");
      publishFlow();
      storeRef.current.lastAgentForKeys = agent;
      return;
    }

    // Agents done (same line)
    const done = line.match(AGENT_DONE_RX);
    if (done) {
      const agent = done[1].trim();
      const ms = Number(done[2]);
      setNodeStatus(agent, "success");
      const id = `${storeRef.current.requestId ?? "req"}:orchestrator->${agent}`;
      const prev = storeRef.current.edges.get(id);
      storeRef.current.edges.set(id, {
        ...(prev ?? { id, from: "orchestrator", to: agent }),
        status: "finished",
        elapsed_ms: ms,
        produced: prev?.produced ?? [],
      });
      publishFlow();
      storeRef.current.lastAgentForKeys = agent;
      return;
    }

    // Optional: keys on next line
    const k = line.match(KEYS_NEXTLINE_RX);
    if (k && storeRef.current.lastAgentForKeys) {
      const agent = storeRef.current.lastAgentForKeys;
      const raw = k[1] ?? "";
      const produced = raw
        .split(",")
        .map((x) => x.replace(/['"]/g, "").trim())
        .filter(Boolean);
      const id = `${storeRef.current.requestId ?? "req"}:orchestrator->${agent}`;
      const prev = storeRef.current.edges.get(id);
      storeRef.current.edges.set(id, {
        ...(prev ?? { id, from: "orchestrator", to: agent }),
        status: "finished",
        elapsed_ms: prev?.elapsed_ms,
        produced: produced.length ? produced : prev?.produced ?? [],
      });
      publishFlow();
      return;
    }
  };

  /* -------------------- WS parsing helpers -------------------- */
  const tryParseEventFromLine = (line: string): any | null => {
    const trimmed = line.trim();
    // Full-line JSON
    if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
      try {
        return JSON.parse(trimmed);
      } catch {}
    }
    // Embedded {...} in a line
    const start = line.indexOf("{");
    const end = line.lastIndexOf("}");
    if (start >= 0 && end > start) {
      const slice = line.slice(start, end + 1);
      try {
        return JSON.parse(slice);
      } catch {}
    }
    return null;
  };

  const processRawText = (raw: string) => {
    if (!raw) return;

    // Log to Activity
    const lines = raw.split(/\r?\n/).filter((l) => l.length > 0);
    for (const line of lines) {
      setLogs((prev) => [
        ...prev,
        {
          id: Date.now().toString() + Math.random().toString(36).slice(2, 6),
          timestamp: new Date().toLocaleTimeString(),
          level: "info",
          source: "Server",
          message: line,
        },
      ]);

      // Parse embedded JSON events first
      const evt = tryParseEventFromLine(line);
      if (evt?.event) {
        if (evt.event === "answer_composed") {
          handleAnswerEvent(evt);
        } else {
          applyFlowEvent(evt);
        }
        continue;
      }

      // Plain markers (agent start / finish)
      applyPlainLog(line);
    }

    // Inline "✅ Result:" capture (for multi-line frames)
    handleInlineResultBlock(raw);
  };

  const processWsFrame = (data: any) => {
    if (typeof data === "string") {
      // Try structured JSON frame first
      try {
        const msg = JSON.parse(data);

        // Known wrappers
        if (msg.event === "log") {
          setLogs((prev) => [
            ...prev,
            {
              id: Date.now().toString(),
              timestamp: new Date().toLocaleTimeString(),
              level: msg.level ?? "info",
              source: msg.source ?? "Server",
              message: msg.line ?? "",
            },
          ]);
          if (typeof msg.line === "string") applyPlainLog(msg.line);
          return;
        }
        if (msg.event === "flow" && msg.data) {
          applyFlowEvent(msg.data);
          return;
        }
        if (msg.event === "agent_flow" && msg.data) {
          const d = msg.data;
          if (d.kind === "started") {
            applyPlainLog(`Calling agent: ${d.agent}`);
          } else if (d.kind === "finished") {
            const keysPart = d.produced?.length
              ? ` keys=[${d.produced.join(", ")}]`
              : "";
            applyPlainLog(`${d.agent} ✓ ${d.elapsed_ms}ms${keysPart}`);
          }
          return;
        }

        // Streaming / final assistant messages
        if (
          msg.event === "chunk" ||
          msg.event === "delta" ||
          msg.event === "assistant_chunk"
        ) {
          const text = msg.text ?? msg.delta ?? "";
          if (text) {
            setIsStreaming(true);
            setAssistantText((t) => (t ? t + text : text));
          }
          return;
        }
        if (
          msg.event === "result" ||
          msg.event === "answer" ||
          msg.event === "assistant_final" ||
          msg.event === "final"
        ) {
          const text = msg.text ?? msg.answer ?? msg.content ?? "";
          if (text) setAssistantText(text);
          setIsStreaming(false);
          setIsProcessing(false);
          return;
        }

        // Bare JSON events (like your logs): { "event": "flow_started", ... }
        if (msg.event) {
          if (msg.event === "answer_composed") {
            handleAnswerEvent(msg);
          } else {
            applyFlowEvent(msg);
          }
          return;
        }

        // Fallback to raw processor
        processRawText(data);
      } catch {
        // Not JSON -> raw text with timestamps / embedded JSON
        processRawText(data);
      }
    } else {
      // Non-string frames (Blob/ArrayBuffer)
      try {
        const textPromise =
          typeof data === "object" && "text" in (data as any)
            ? (data as any).text()
            : Promise.resolve("");
        Promise.resolve(textPromise).then((t) => processRawText(String(t)));
      } catch {
        /* ignore */
      }
    }
  };

  /* -------------------- WebSocket & reconnect -------------------- */
  const scheduleReconnect = () => {
    // Avoid multiple timers
    if (reconnectTimerRef.current) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
    const attempt = reconnectAttemptsRef.current;
    const delay = Math.min(1000 * 2 ** attempt, 10000); // 1s,2s,4s,... max 10s
    reconnectTimerRef.current = setTimeout(() => {
      reconnectAttemptsRef.current += 1;
      connectWS();
    }, delay);
  };

  const connectWS = () => {
    if (
      wsRef.current &&
      (wsRef.current.readyState === WebSocket.OPEN ||
        wsRef.current.readyState === WebSocket.CONNECTING)
    )
      return;

    const ws = new WebSocket("ws://localhost:8000/ws/chat");
    wsRef.current = ws;

    ws.onopen = () => {
      setIsWsConnected(true);
      // Reset backoff on successful connection
      reconnectAttemptsRef.current = 0;
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };
    ws.onclose = () => {
      setIsWsConnected(false);
      scheduleReconnect(); // server may have exited after /reset
    };
    ws.onerror = () => {
      setIsWsConnected(false);
      scheduleReconnect();
    };
    ws.onmessage = (ev) => {
      processWsFrame(ev.data);
    };
  };

  useEffect(() => {
    connectWS();
    return () => {
      try {
        wsRef.current?.close();
      } catch { /* empty */ }
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /* -------------------- Reset helpers -------------------- */
  const wipeUiState = () => {
    // Clear result panel & streaming state
    setAssistantText("");
    setIsStreaming(false);

    // Clear activity logs (comment out if you want to keep prior logs)
    setLogs([]);

    // Reset flow to just static nodes, no edges
    storeRef.current.requestId = null;
    if (storeRef.current.nodes.size === 0) {
      storeRef.current.nodes = buildStaticNodes();
    }
    setAllStatuses("idle");
    clearEdges();
    publishFlow();

    setIsProcessing(false);
  };

  const resetSessionId = () => {
    try {
      localStorage.removeItem(SESSION_STORAGE_KEY);
    } catch {}
    sessionIdRef.current = getStableSessionId();
  };

  const handleResetCommand = async () => {
    // Optional confirmation; comment out if you prefer silent reset
    // const ok = window.confirm("Reset conversation and restart session?");
    // if (!ok) return;

    // Log to activity for transparency
    setLogs((prev) => [
      ...prev,
      {
        id: Date.now().toString(),
        timestamp: new Date().toLocaleTimeString(),
        level: "info",
        source: "Client",
        message: "Reset requested by user",
      },
    ]);

    // Immediate UI feedback
    wipeUiState();
    resetSessionId();

    // Best-effort send /reset to server (it will wipe history & exit)
    try {
      if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
        connectWS();
        await new Promise((r) => setTimeout(r, 150));
      }
      wsRef.current?.send(
        JSON.stringify({
          session_id: sessionIdRef.current,
          user_input: RESET_COMMAND,
        })
      );
    } catch {
      /* ignore — server may already be shutting down */
    }
  };

  /* -------------------- Send query over WS -------------------- */
  const handleSendQuery = async () => {
    const q = networkQuery.trim();
    if (!q) return;

    // 🔴 Intercept /reset early
    if (q.toLowerCase().startsWith(RESET_COMMAND)) {
      setNetworkQuery("");
      await handleResetCommand();
      return;
    }

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
    setIsStreaming(false);
    setAssistantText("");

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
      setIsStreaming(false);
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
        </div>
        <div className="flex items-center gap-2">
          <Badge variant={isWsConnected ? "default" : "secondary"}>
            WS: {isWsConnected ? "Connected" : "Disconnected"}
          </Badge>
          <Badge variant={isProcessing ? "default" : "secondary"}>
            {isProcessing ? "Processing…" : "Idle"}
          </Badge>
        </div>
      </header>

      {/* Main grid: Left(Flow+Activity) | Right(Prompt+Result) */}
      <main className="grid grid-cols-12 gap-6">
        {/* LEFT: Flow (top-left) */}
        <section className="col-span-12 md:col-span-7 order-1">
          <h3 className="text-xl font-semibold">Agent Flow</h3>
          <p className="text-xs text-muted-foreground mb-2">
            Edges animate when Orchestrator connects to agents.
          </p>
          {/* IMPORTANT: fixed (smaller) height for React Flow */}
          <div className="relative w-full h-[380px] md:h-[440px] rounded-lg border bg-white/50 backdrop-blur-sm">
            <AgentDiagram nodes={flow.nodes} edges={flow.edges} />
          </div>
        </section>

        {/* RIGHT: Prompt (top-right) -> pin to columns 8-12 on md+ */}
        <section className="col-span-12 md:col-span-5 md:col-start-8 order-2 space-y-2">
          <h3 className="text-xl font-semibold">AI Assistant</h3>
          <p className="text-xs text-muted-foreground">
            Ask about alarms, performance, inventory, data flow, customers, tickets…
          </p>
          <div className="rounded-lg border bg-white/50 backdrop-blur-sm p-3">
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
                placeholder="Type a query or /reset to start fresh"
              />
              <Button onClick={handleSendQuery} disabled={isProcessing}>
                {isProcessing ? "Processing..." : <Send className="w-4 h-4" />}
              </Button>
            </div>
            <div className="mt-2 text-[11px] text-muted-foreground">
              Press <kbd className="px-1 py-[1px] border rounded">Enter</kbd> to send •{" "}
              <kbd className="px-1 py-[1px] border rounded">Shift</kbd>+
              <kbd className="px-1 py-[1px] border rounded">Enter</kbd> for a new line
            </div>
          </div>
        </section>

        {/* RIGHT: Result (under Prompt, same right column) -> pin to columns 8-12 on md+ */}
        <section className="col-span-12 md:col-span-5 md:col-start-8 order-3 space-y-2">
          <div className="flex items-baseline justify-between">
            <h3 className="text-xl font-semibold">Result</h3>
            <div className="flex items-center gap-2">
              <Button
                variant="secondary"
                size="sm"
                onClick={() => navigator.clipboard?.writeText(assistantText || "")}
                disabled={!assistantText}
              >
                Copy
              </Button>
              <Button
                variant="secondary"
                size="sm"
                onClick={() => setAssistantText("")}
                disabled={!assistantText}
              >
                Clear
              </Button>
            </div>
          </div>
          <div
            className="rounded-lg border bg-white/50 backdrop-blur-sm min-h-[160px] max-h-[360px] overflow-auto p-4"
            aria-live="polite"
          >
            {assistantText ? (
              <div className="text-sm leading-6 whitespace-pre-wrap">{assistantText}</div>
            ) : (
              <div className="text-sm text-muted-foreground">Your answers will appear here.</div>
            )}
            {isStreaming && (
              <div className="mt-2 text-xs text-muted-foreground">Generating…</div>
            )}

            {/* Optional: artifacts */}
            {flow.edges.some((e) => (e.produced ?? []).length > 0) && (
              <div className="mt-4">
                <div className="text-xs font-medium text-muted-foreground">
                  Generated artifacts:
                </div>
                <ul className="mt-1 text-xs text-muted-foreground list-disc pl-4">
                  {Array.from(new Set(flow.edges.flatMap((e) => e.produced ?? []))).map(
                    (k) => (
                      <li key={k}>{k}</li>
                    )
                  )}
                </ul>
              </div>
            )}
          </div>
        </section>

        {/* LEFT: Activity (under Flow, left column) */}
        <section className="col-span-12 md:col-span-7 order-4">
          <h3 className="text-xl font-semibold">Activity</h3>
          <div className="rounded-lg border bg-white/50 backdrop-blur-sm max-h-[240px] overflow-auto">
            <div className="p-3 text-sm">
              {logs.map((log, i) => (
                <div key={i} className="text-muted-foreground">
                  [{log.timestamp}] {log.source}: {log.message}
                </div>
              ))}
              {!logs.length && (
                <div className="text-xs text-muted-foreground">
                  No logs yet. Send a query to see activity.
                </div>
              )}
            </div>
          </div>
        </section>
      </main>
    </div>
  );
};

export default Index;
