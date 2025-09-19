import React, { useEffect, useRef, useState } from "react";
import AgentDiagram from "@/components/AgentDiagram"; // default export from your diagram file
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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

/* -------- Stable session id -------- */
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

/* -------- Static node manifest -------- */
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

  /* ---- Flow graph state (static nodes, dynamic edges) ---- */
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

  // NEW: ensure a node exists for a given id (covers unknown agents from server)
  const ensureNode = (id: string) => {
    if (!id) return;
    if (!storeRef.current.nodes.has(id)) {
      const label = AGENT_LABELS[id] ?? id.replace(/_/g, " ");
      const type: FlowNode["type"] = id === "orchestrator" ? "orchestrator" : "agent";
      storeRef.current.nodes.set(id, {
        id,
        label,
        type,
        status: "idle",
      });
    }
  };

  // CHANGED: publishFlow now filters edges whose endpoints are missing
  const publishFlow = () => {
    const nodesArr = Array.from(storeRef.current.nodes.values());
    const nodeIds = new Set(nodesArr.map((n) => n.id));
    const edgesArr = Array.from(storeRef.current.edges.values()).filter(
      (e) => nodeIds.has(e.from) && nodeIds.has(e.to)
    );
    setFlow({
      requestId: storeRef.current.requestId,
      nodes: nodesArr,
      edges: edgesArr,
    });
  };

  const setAllStatuses = (status: NodeStatus) => {
    for (const [id, n] of storeRef.current.nodes) {
      storeRef.current.nodes.set(id, { ...n, status });
    }
  };

  // CHANGED: ensure node before setting status
  const setNodeStatus = (id: string, status: NodeStatus) => {
    if (!id) return;
    ensureNode(id); // NEW
    const n = storeRef.current.nodes.get(id);
    if (!n) return;
    storeRef.current.nodes.set(id, { ...n, status });
  };

  // CHANGED: log clears for debugging
  const clearEdges = () => {
    console.debug("[FLOW] clearEdges()", {
      reason: "called",
      requestId: storeRef.current.requestId,
      edgesBefore: storeRef.current.edges.size,
    });
    storeRef.current.edges.clear();
  };

  // CHANGED: ensure endpoints exist before upserting edge
  const upsertEdge = (
    from: string,
    to: string,
    status?: FlowEdge["status"],
    reason?: string
  ) => {
    ensureNode(from); // NEW
    ensureNode(to);   // NEW
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

  // CHANGED: guard against duplicate / overlapping starts
  const resetForRequest = (requestId: string | null) => {
    // ignore duplicate starts for the same request id
    if (storeRef.current.requestId && requestId && storeRef.current.requestId === requestId) {
      console.debug("[FLOW] duplicate flow_started ignored", { requestId });
      return;
    }

    // Defensive: ensure static nodes exist before starting the next run
    if (storeRef.current.nodes.size === 0) {
      storeRef.current.nodes = buildStaticNodes();
    }

    console.debug("[FLOW] resetForRequest()", {
      from: storeRef.current.requestId,
      to: requestId,
    });

    storeRef.current.requestId = requestId;
    setAllStatuses("idle");
    clearEdges();
    setNodeStatus("orchestrator", "running"); // show planning in progress
    publishFlow();
  };

  /* -------------------- Result helpers -------------------- */
  const handleAnswerEvent = (evt: any) => {
    const text = evt.answer ?? evt.answer_preview ?? evt.content ?? evt.text ?? "";
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

    // NEW: debug incoming events
    console.debug("[FLOW] evt", evt);

    // NEW: drop stale events that don't match active request (unless starting)
    if (evt.request_id && storeRef.current.requestId && evt.event !== "flow_started") {
      if (evt.request_id !== storeRef.current.requestId) {
        console.debug("[FLOW] dropping stale event", {
          active: storeRef.current.requestId,
          incoming: evt.request_id,
          event: evt.event,
        });
        return;
      }
    }

    // NEW: guard "flow_started" against overlapping runs
    if (evt.event === "flow_started") {
      const incomingId = evt.request_id ?? null;
      if (
        storeRef.current.requestId &&
        incomingId &&
        incomingId !== storeRef.current.requestId &&
        isProcessing // if UI thinks we are in the middle of a run, ignore stray start
      ) {
        console.debug("[FLOW] ignoring overlapping flow_started", {
          active: storeRef.current.requestId,
          incoming: incomingId,
        });
        return;
      }
      resetForRequest(incomingId);
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
        // NEW: ensure endpoints exist
        ensureNode("orchestrator");
        ensureNode(agent);
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

  /* -------- Fallback plain log parsing -------- */
  const applyPlainLog = (line: string) => {
    // Agents start
    const start = line.match(AGENT_START_RX);
    if (start) {
      const agent = start[1].trim();
      setNodeStatus(agent, "running");      // ensureNode is called inside
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
      const raw = (k[1] ?? "");
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

  /* -------- WS parsing helpers -------- */
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
            const keysPart = d.produced?.length ? ` keys=[${d.produced.join(", ")}]` : "";
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

  /* -------- WebSocket & reconnect -------- */
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
      } catch {}
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /* -------- Reset helpers -------- */
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

  /* -------- Send query over WS -------- */
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
    <div className="min-h-screen flex flex-col p-4 space-y-4 bg-gradient-to-br from-slate-900 via-blue-900 to-slate-800">
      {/* Header */}
      <header className="flex items-center justify-between">
        <div>
          <h2 className="text-3xl font-bold bg-gradient-to-r from-blue-400 via-cyan-400 to-blue-500 bg-clip-text text-transparent drop-shadow-lg">
            AI Agent Portal
          </h2>
        </div>
        <div className="flex items-center gap-2">
          <Badge
            variant={isWsConnected ? "default" : "secondary"}
            className={
              isWsConnected
                ? "bg-blue-500/20 text-cyan-300 border-blue-400/50 shadow-lg shadow-blue-500/25"
                : "bg-slate-700/50 text-slate-300 border-slate-600/50"
            }
          >
            WS: {isWsConnected ? "Connected" : "Disconnected"}
          </Badge>
          <Badge
            variant={isProcessing ? "default" : "secondary"}
            className={
              isProcessing
                ? "bg-blue-500/20 text-cyan-300 border-blue-400/50 shadow-lg shadow-blue-500/25"
                : "bg-slate-700/50 text-slate-300 border-slate-600/50"
            }
          >
            {isProcessing ? "Processing…" : "Idle"}
          </Badge>
        </div>
      </header>

      {/* Main Content - Conservative Height Layout */}
      <main className="flex-1 flex flex-col gap-4 min-h-[calc(100vh-180px)]">
        {/* Top Section - Better Height Management */}
        <section className="flex flex-col lg:flex-row gap-3 h-[460px]">
          {/* LEFT: Agent Flow Diagram */}
          <Card className="flex-1 lg:flex-[2] bg-slate-800/40 backdrop-blur-xl border-blue-500/30 shadow-2xl shadow-blue-500/20">
            <CardHeader className="pb-2 px-4 pt-4">
              <CardTitle className="text-xl font-semibold text-cyan-300 drop-shadow-md">
                Agent Flow
              </CardTitle>
              <p className="text-sm text-blue-200/80">
                Edges animate when Orchestrator connects to agents.
              </p>
            </CardHeader>
            <CardContent className="px-4 pb-4">
              {/* Conservative height container for React Flow */}
              <div className="relative w-full h-[380px] rounded-lg border border-blue-400/40 bg-slate-900/60 shadow-inner shadow-blue-500/10">
                <AgentDiagram nodes={flow.nodes} edges={flow.edges} />
              </div>
            </CardContent>
          </Card>

          {/* RIGHT: Prompt + Result (Flex Column) */}
          <div className="flex-1 lg:flex-[1] flex flex-col gap-3 min-w-0">
            {/* Prompt Section */}
            <Card className="bg-slate-800/40 backdrop-blur-xl border-blue-500/30 shadow-2xl shadow-blue-500/20">
              <CardHeader className="pb-1 px-3 pt-3">
                <CardTitle className="text-xl font-semibold text-cyan-300 drop-shadow-md">
                  AI Assistant
                </CardTitle>
                <p className="text-sm text-blue-200/80">
                  Ask about alarms, performance, inventory, data flow, customers, tickets…
                </p>
              </CardHeader>
              <CardContent className="px-3 pb-3 space-y-2">
                <div className="flex items-start gap-2">
                  <Textarea
                    value={networkQuery}
                    onChange={(e) => setNetworkQuery(e.target.value)}
                    className="flex-1 min-h-[80px] resize-none bg-slate-900/60 border-blue-400/40 text-blue-100 placeholder-blue-300/50 focus:border-cyan-400/60 focus:ring-cyan-400/30"
                    disabled={isProcessing}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" && !e.shiftKey) {
                        e.preventDefault();
                        handleSendQuery();
                      }
                    }}
                    placeholder="Type a query or /reset to start fresh"
                  />
                  <Button
                    onClick={handleSendQuery}
                    disabled={isProcessing}
                    className="bg-gradient-to-r from-blue-600 to-cyan-600 hover:from-blue-500 hover:to-cyan-500 text-white border-0 shadow-lg shadow-blue-500/40 hover:shadow-cyan-400/50 transition-all duration-300 transform hover:scale-105 active:scale-95 disabled:opacity-50 disabled:transform-none"
                  >
                    {isProcessing ? "Processing..." : <Send className="w-4 h-4" />}
                  </Button>
                </div>
                <div className="text-xs text-blue-300/70">
                  Press <kbd className="px-1 py-[1px] border border-blue-400/40 bg-slate-700/50 rounded text-cyan-300">Enter</kbd> to send •{" "}
                  <kbd className="px-1 py-[1px] border border-blue-400/40 bg-slate-700/50 rounded text-cyan-300">Shift</kbd>+
                  <kbd className="px-1 py-[1px] border border-blue-400/40 bg-slate-700/50 rounded text-cyan-300">Enter</kbd> for a new line
                </div>
              </CardContent>
            </Card>

            {/* Result Section */}
            <Card className="flex-1 bg-slate-800/40 backdrop-blur-xl border-blue-500/30 shadow-2xl shadow-blue-500/20">
              <CardHeader className="pb-1 px-3 pt-3">
                <div className="flex items-baseline justify-between">
                  <CardTitle className="text-xl font-semibold text-cyan-300 drop-shadow-md">
                    Result
                  </CardTitle>
                  <div className="flex items-center gap-2">
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={() => navigator.clipboard?.writeText(assistantText || "")}
                      disabled={!assistantText}
                      className="bg-gradient-to-r from-emerald-600/80 to-green-600/80 hover:from-emerald-500/90 hover:to-green-500/90 text-white border-emerald-400/50 shadow-lg shadow-emerald-500/30 hover:shadow-emerald-400/50 transition-all duration-300 transform hover:scale-105 active:scale-95 disabled:opacity-40 disabled:transform-none"
                    >
                      Copy
                    </Button>
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={() => setAssistantText("")}
                      disabled={!assistantText}
                      className="bg-gradient-to-r from-rose-600/80 to-red-600/80 hover:from-rose-500/90 hover:to-red-500/90 text-white border-rose-400/50 shadow-lg shadow-rose-500/30 hover:shadow-rose-400/50 transition-all duration-300 transform hover:scale-105 active:scale-95 disabled:opacity-40 disabled:transform-none"
                    >
                      Clear
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent className="px-3 pb-3">
                <div
                  className="rounded-lg border border-blue-400/40 bg-slate-900/60 min-h-[180px] max-h-[260px] overflow-auto p-3 shadow-inner"
                  aria-live="polite"
                >
                  {assistantText ? (
                    <div className="text-sm leading-6 whitespace-pre-wrap text-blue-100">{assistantText}</div>
                  ) : (
                    <div className="text-sm text-blue-300/60">Your answers will appear here.</div>
                  )}
                  {isStreaming && (
                    <div className="mt-2 text-xs text-cyan-400/80">Generating…</div>
                  )}
                  {/* Optional: artifacts */}
                  {flow.edges.some((e) => (e.produced ?? []).length > 0) && (
                    <div className="mt-4">
                      <div className="text-xs font-medium text-cyan-300/90">
                        Generated artifacts:
                      </div>
                      <ul className="mt-1 text-xs text-blue-200/70 list-disc pl-4">
                        {Array.from(new Set(flow.edges.flatMap((e) => e.produced ?? []))).map(
                          (k) => (
                            <li key={k}>{k}</li>
                          )
                        )}
                      </ul>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          </div>
        </section>

        {/* Bottom Section - Logs */}
        <section className="flex flex-col">
          <Card className="bg-slate-800/40 backdrop-blur-xl border-blue-500/30 shadow-2xl shadow-blue-500/20">
            <CardHeader className="pb-1 px-3 pt-3">
              <CardTitle className="text-xl font-semibold text-cyan-300 drop-shadow-md">
                Activity
              </CardTitle>
            </CardHeader>
            <CardContent className="px-3 pb-3">
              <div className="rounded-lg border border-blue-400/40 bg-gradient-to-br from-slate-800/80 via-blue-900/60 to-slate-800/80 h-[160px] overflow-auto shadow-inner backdrop-blur-sm">
                <div className="p-3 text-sm space-y-1">
                  {logs.map((log, i) => (
                    <div
                      key={i}
                      className="text-blue-200/80 hover:text-cyan-300/90 transition-colors duration-200 hover:bg-blue-500/20 rounded px-2 py-1"
                    >
                      <span className="text-cyan-400/70">[{log.timestamp}]</span>
                      <span className="text-blue-300/90 ml-2">{log.source}:</span>
                      <span className="text-blue-100 ml-2">{log.message}</span>
                    </div>
                  ))}
                  {!logs.length && (
                    <div className="text-sm text-blue-300/60 text-center py-8">
                      No logs yet. Send a query to see activity.
                    </div>
                  )}
                </div>
              </div>
            </CardContent>
          </Card>
        </section>
      </main>
    </div>
  );
};

export default Index;
