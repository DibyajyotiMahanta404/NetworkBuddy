import React, { useEffect, useMemo, useRef } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  MarkerType,
  type Node,
  type Edge,
  type ReactFlowInstance,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

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

type Props = {
  nodes: FlowNode[];
  edges: FlowEdge[];
};

/* ---- Palette ---- */
const statusColors = {
  orchestrator: "#7c3aed", // violet-600 for ring accent
  idle: "#cbd5e1",         // slate-300
  queued: "#60a5fa",       // blue-400
  running: "#f59e0b",      // amber-500
  success: "#10b981",      // emerald-500
  failed: "#ef4444",       // red-500
};

const edgeColors = {
  enqueued: "#60a5fa", // blue-400
  started: "#f59e0b",  // amber-500
  finished: "#10b981", // green-500
  failed: "#ef4444",   // red-500
};

/* ---- Anim helpers ---- */
const glow = (c: string) => ({
  boxShadow: `0 0 0 2px ${c}22, 0 0 0 8px ${c}16, 0 0 18px 3px ${c}55`,
  transition: "box-shadow 120ms ease, border-color 120ms ease",
});

/* ---- Layout: orchestrator center, all agents in a circle ---- */
function circularLayout(flowNodes: FlowNode[]) {
  // Determine who is orchestrator and the agents
  const centerId = "orchestrator";
  const agents = flowNodes.filter((n) => n.id !== centerId);
  const R = 260; // radius
  const positions: Record<string, { x: number; y: number }> = {};
  positions[centerId] = { x: 0, y: 0 };

  const n = agents.length || 1;
  const step = (2 * Math.PI) / n;
  const startAngle = -Math.PI / 2; // start at 12 o'clock

  agents.forEach((node, i) => {
    const angle = startAngle + i * step;
    positions[node.id] = {
      x: Math.cos(angle) * R,
      y: Math.sin(angle) * R,
    };
  });

  return positions;
}

export default function AgentDiagram({ nodes, edges }: Props) {
  const rfInstanceRef = useRef<ReactFlowInstance | null>(null);
  const positions = useMemo(() => circularLayout(nodes), [nodes]);

  const rfNodes: Node[] = useMemo(() => {
    return nodes.map((n) => {
      const status: NodeStatus =
        n.type === "orchestrator" ? (n.status ?? "idle") : (n.status ?? "idle");

      // Base color per status
      const color =
        n.type === "orchestrator" ? statusColors.orchestrator :
        status === "failed" ? statusColors.failed :
        status === "running" ? statusColors.running :
        status === "queued" ? statusColors.queued :
        status === "success" ? statusColors.success :
        statusColors.idle;

      // Soft backgrounds for nodes
      const background =
        n.type === "orchestrator" ? "#ffffff" :
        status === "failed" ? "#fee2e2" :
        status === "running" ? "#fffbeb" :
        status === "queued" ? "#eff6ff" :
        status === "success" ? "#ecfdf5" :
        "#f8fafc"; // idle: slate-50

      const style: React.CSSProperties = {
        padding: 12,
        borderRadius: 14,
        border: `2px solid ${color}`,
        background,
        fontWeight: 600,
        color: "#0f172a",
        // subtle gradient ring for the orchestrator
        ...(n.type === "orchestrator"
          ? {
              background:
                "linear-gradient(180deg, rgba(255,255,255,0.85), rgba(255,255,255,0.95))",
              border: `2px solid ${statusColors.orchestrator}`,
              ...(status === "running" ? glow(statusColors.orchestrator) : {}),
            }
          : {}),
        ...(status === "running" && n.type !== "orchestrator" ? glow(color) : {}),
        transition: "background-color 120ms ease, border-color 120ms ease, transform 120ms ease",
      };

      // Orchestrator badge
      const label =
        n.type === "orchestrator"
          ? `🧭  ${n.label}`
          : status === "failed"
          ? `❌ ${n.label}`
          : status === "running"
          ? `⚡ ${n.label}`
          : status === "queued"
          ? `⏳ ${n.label}`
          : status === "success"
          ? `✅ ${n.label}`
          : `● ${n.label}`;

      return {
        id: n.id,
        position: positions[n.id] ?? { x: 0, y: 0 },
        data: { label },
        draggable: false,
        selectable: false,
        style,
      } as Node;
    });
  }, [nodes, positions]);

  const rfEdges: Edge[] = useMemo(() => {
    return edges.map((e) => {
      const color = edgeColors[e.status || "finished"] || edgeColors.finished;
      const animated = e.status === "started"; // animate while running
      const label =
        e.status === "enqueued"
          ? (e.reason ? `enqueued: ${e.reason}` : "enqueued")
          : e.status === "started"
          ? "started"
          : e.status === "failed"
          ? "failed"
          : e.elapsed_ms != null
          ? `${e.elapsed_ms} ms`
          : undefined;

      return {
        id: e.id,
        source: e.from,
        target: e.to,
        animated,
        label,
        style: {
          stroke: color,
          strokeWidth: 2.25,
          ...(e.status === "enqueued" ? { strokeDasharray: "6 3" } : {}),
          transition: "stroke 120ms ease",
        },
        markerEnd: { type: MarkerType.ArrowClosed, color },
        // no interaction
        selectable: false,
        focusable: false,
      } as Edge;
    });
  }, [edges]);

  // Fit the view once ready and on graph changes
  useEffect(() => {
    if (!rfInstanceRef.current) return;
    const t = setTimeout(() => {
      try {
        rfInstanceRef.current?.fitView({ padding: 0.25, includeHiddenNodes: true });
      } catch {}
    }, 60);
    return () => clearTimeout(t);
  }, [rfNodes.length, rfEdges.length]);

  return (
    <ReactFlow
      nodes={rfNodes}
      edges={rfEdges}
      onInit={(inst) => {
        rfInstanceRef.current = inst;
        setTimeout(() => {
          try {
            inst.fitView({ padding: 0.25, includeHiddenNodes: true });
          } catch {}
        }, 60);
      }}
      panOnScroll
      panOnDrag
      zoomOnScroll={false}
      zoomOnPinch
      elementsSelectable={false}
      proOptions={{ hideAttribution: true }}
      style={{ width: "100%", height: "100%" }}
    >
      <Background gap={18} color="#e5e7eb" />
      <MiniMap pannable zoomable />
      <Controls showInteractive={false} />
    </ReactFlow>
  );
}
