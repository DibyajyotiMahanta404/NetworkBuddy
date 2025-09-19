import { useState, useEffect, useRef } from "react";
import { AgentCard, type Agent } from "@/components/AgentCard";
import { AgentDiagram } from "@/components/AgentDiagram";
import { LogPanel } from "@/components/LogPanel";
import { Badge } from "@/components/ui/badge";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Send, Wifi, WifiOff } from "lucide-react";

// WebSocket service with reconnection logic
class WebSocketService {
  private agentsWs: WebSocket | null = null;
  private chatWs: WebSocket | null = null;
  private onAgentUpdateCallback: ((agents: any[]) => void) | null = null;
  private onLogUpdateCallback: ((log: any) => void) | null = null;
  private onInputOutputCallback: ((type: "input" | "output", data: any) => void) | null = null;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 10;
  private reconnectInterval = 3000; // 3 seconds
  private connectionCheckInterval: ReturnType<typeof setInterval> | null = null;

  connectAgentsWebSocket(onUpdate: (agents: any[]) => void) {
    this.onAgentUpdateCallback = onUpdate;
    this.tryConnectAgentsWebSocket();
  }

  private tryConnectAgentsWebSocket() {
    try {
      // Close existing connection if any
      if (this.agentsWs) {
        this.agentsWs.close();
      }

      this.agentsWs = new WebSocket("ws://localhost:8000/run");

      this.agentsWs.onopen = () => {
        console.log("Connected to agents WebSocket");
        this.reconnectAttempts = 0;
      };

      this.agentsWs.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.type === "agents_update" && data.data) {
            const agents = data.data.map((agentData: any) => ({
              id: agentData.id,
              name: agentData.id.replace("-", " ").replace(/\b\w/g, (l) => l.toUpperCase()),
              type: agentData.id.replace("-agent", ""),
              status: agentData.status,
              description: `${agentData.id.replace("-", " ")} agent for network operations`,
              lastActivity: agentData.lastActivity || "Never",
              metrics: {
                requests: agentData.metrics?.requests || 0,
                success: agentData.metrics?.successes || 0,
                errors: agentData.metrics?.errors || 0,
              },
            }));
            this.onAgentUpdateCallback?.(agents);
          }
        } catch (error) {
          console.error("Error parsing agents WebSocket message:", error);
        }
      };

      this.agentsWs.onerror = (error) => {
        console.error("Agents WebSocket error:", error);
      };

      this.agentsWs.onclose = (event) => {
        console.log("Agents WebSocket disconnected", event.code, event.reason);
        this.scheduleReconnect();
      };
    } catch (error) {
      console.error("Failed to connect to agents WebSocket:", error);
      this.scheduleReconnect();
    }
  }

  connectChatWebSocket(
    onLogUpdate: (log: any) => void,
    onInputOutput: (type: "input" | "output", data: any) => void
  ) {
    this.onLogUpdateCallback = onLogUpdate;
    this.onInputOutputCallback = onInputOutput;
    this.tryConnectChatWebSocket();
  }

  private tryConnectChatWebSocket() {
    try {
      // Close existing connection if any
      if (this.chatWs) {
        this.chatWs.close();
      }

      this.chatWs = new WebSocket("ws://localhost:8000/run");

      this.chatWs.onopen = () => {
        console.log("Connected to chat WebSocket");
        this.reconnectAttempts = 0;
      };

      this.chatWs.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);

          if (data.event === "response") {
            // Add to main logs
            this.onLogUpdateCallback?.({
              id: Date.now().toString(),
              timestamp: new Date().toLocaleTimeString(),
              level: "info",
              source: "Orchestrator",
              message: `Processed query: ${data.data.result}`,
              data: data.data,
            });

            // Extract agent traces for input/output
            if (data.data.trace) {
              data.data.trace.forEach((trace: any) => {
                this.onInputOutputCallback?.("input", {
                  id: Date.now().toString() + "-input",
                  timestamp: new Date().toLocaleTimeString(),
                  level: "info",
                  source: trace.agent,
                  message: `Input: ${JSON.stringify(trace.input)}`,
                  data: trace.input,
                });

                this.onInputOutputCallback?.("output", {
                  id: Date.now().toString() + "-output",
                  timestamp: new Date().toLocaleTimeString(),
                  level: "info",
                  source: trace.agent,
                  message: `Output: ${JSON.stringify(trace.output)}`,
                  data: trace.output,
                });
              });
            }
          } else if (data.event === "error") {
            this.onLogUpdateCallback?.({
              id: Date.now().toString(),
              timestamp: new Date().toLocaleTimeString(),
              level: "error",
              source: "System",
              message: data.message,
              data: data,
            });
          } else if (data.event === "processing_started") {
            this.onLogUpdateCallback?.({
              id: Date.now().toString(),
              timestamp: new Date().toLocaleTimeString(),
              level: "info",
              source: "System",
              message: data.message,
              data: data,
            });
          }
        } catch (error) {
          console.error("Error parsing chat WebSocket message:", error);
        }
      };

      this.chatWs.onerror = (error) => {
        console.error("Chat WebSocket error:", error);
      };

      this.chatWs.onclose = (event) => {
        console.log("Chat WebSocket disconnected", event.code, event.reason);
        this.scheduleReconnect();
      };
    } catch (error) {
      console.error("Failed to connect to chat WebSocket:", error);
      this.scheduleReconnect();
    }
  }

  private scheduleReconnect() {
    if (this.reconnectAttempts < this.maxReconnectAttempts) {
      this.reconnectAttempts++;
      console.log(
        `Attempting to reconnect in ${this.reconnectInterval / 1000} seconds (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`
      );

      setTimeout(() => {
        if (this.onAgentUpdateCallback) {
          this.tryConnectAgentsWebSocket();
        }
        if (this.onLogUpdateCallback && this.onInputOutputCallback) {
          this.tryConnectChatWebSocket();
        }
      }, this.reconnectInterval);
    } else {
      console.error("Max reconnection attempts reached. Please check if the server is running.");
    }
  }

  sendQuery(query: string) {
    if (this.chatWs && this.chatWs.readyState === WebSocket.OPEN) {
      this.chatWs.send(
        JSON.stringify({
          user_input: query,
          session_id: `session_${Date.now()}`,
        })
      );
      return true;
    } else {
      console.error("WebSocket is not connected. Current state:", this.chatWs?.readyState);
      return false;
    }
  }

  disconnect() {
    this.agentsWs?.close();
    this.chatWs?.close();
    if (this.connectionCheckInterval) {
      clearInterval(this.connectionCheckInterval);
    }
  }

  getConnectionStatus() {
    return {
      agents: this.agentsWs?.readyState === WebSocket.OPEN ? "connected" : "disconnected",
      chat: this.chatWs?.readyState === WebSocket.OPEN ? "connected" : "disconnected",
    };
  }

  isConnected() {
    const status = this.getConnectionStatus();
    return status.agents === "connected" && status.chat === "connected";
  }
}

const websocketService = new WebSocketService();

const Index = () => {
  const [selectedAgent, setSelectedAgent] = useState<Agent | null>(null);
  const [networkQuery, setNetworkQuery] = useState("");
  const [isProcessing, setIsProcessing] = useState(false);
  const [logs, setLogs] = useState<any[]>([]);
  const [inputLogs, setInputLogs] = useState<any[]>([]);
  const [outputLogs, setOutputLogs] = useState<any[]>([]);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [connectionStatus, setConnectionStatus] = useState({
    agents: "disconnected",
    chat: "disconnected",
  });

  useEffect(() => {
    // Connect to agents WebSocket
    websocketService.connectAgentsWebSocket((agents) => {
      setAgents(agents);
    });

    // Connect to chat WebSocket for logs and input/output
    websocketService.connectChatWebSocket(
      (log) => {
        setLogs((prev) => [...prev.slice(-49), log]);
      },
      (type, data) => {
        if (type === "input") {
          setInputLogs((prev) => [...prev.slice(-19), data]);
        } else {
          setOutputLogs((prev) => [...prev.slice(-19), data]);
        }
      }
    );

    // Check connection status periodically
    const connectionCheckInterval = setInterval(() => {
      setConnectionStatus(websocketService.getConnectionStatus());
    }, 2000);

    return () => {
      websocketService.disconnect();
      clearInterval(connectionCheckInterval);
    };
  }, []);

  const handleSendQuery = () => {
    if (!networkQuery.trim()) return;

    setIsProcessing(true);

    // Add user query to logs
    setLogs((prev) => [
      ...prev,
      {
        id: Date.now().toString(),
        timestamp: new Date().toLocaleTimeString(),
        level: "info",
        source: "User",
        message: `Query: ${networkQuery}`,
      },
    ]);

    // Send query via WebSocket
    const success = websocketService.sendQuery(networkQuery);

    if (!success) {
      setLogs((prev) => [
        ...prev,
        {
          id: Date.now().toString(),
          timestamp: new Date().toLocaleTimeString(),
          level: "error",
          source: "System",
          message:
            "Failed to send query. WebSocket not connected. Please check if backend is running.",
        },
      ]);
      setIsProcessing(false);
      return;
    }

    // Clear query
    setNetworkQuery("");

    // Reset processing state after a delay
    setTimeout(() => setIsProcessing(false), 5000);
  };

  const activeAgents = agents.filter((agent) => agent.status === "active").length;
  const processingAgents = agents.filter((agent) => agent.status === "processing").length;
  const isConnected = connectionStatus.agents === "connected" && connectionStatus.chat === "connected";

  // Format logs for display
  const formattedLogs = logs.map((log) => ({
    text: `[${log.timestamp}] ${log.source}: ${log.message}`,
    className: `text-${
      log.level === "error"
        ? "destructive"
        : log.level === "warning"
        ? "warning"
        : log.level === "success"
        ? "success"
        : log.source === "User"
        ? "info"
        : "primary"
    }`,
  }));

  return (
    <div className="min-h-screen bg-background p-6">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold bg-gradient-primary bg-clip-text text-transparent">
              AI Agent Portal
            </h1>
            <p className="text-muted-foreground mt-1">
              Monitor and manage autonomous agents in real-time
            </p>
          </div>
          <div className="flex gap-3">
            <Badge variant="secondary" className="bg-success/20 text-success">
              {activeAgents} Active
            </Badge>
            <Badge variant="secondary" className="bg-warning/20 text-warning">
              {processingAgents} Processing
            </Badge>
            <Badge
              variant={connectionStatus.agents === "connected" ? "secondary" : "destructive"}
              className={
                connectionStatus.agents === "connected"
                  ? "bg-success/20 text-success"
                  : "bg-destructive/20 text-destructive"
              }
            >
              {connectionStatus.agents === "connected" ? (
                <Wifi className="w-3 h-3 mr-1" />
              ) : (
                <WifiOff className="w-3 h-3 mr-1" />
              )}
              Agents: {connectionStatus.agents}
            </Badge>
            <Badge
              variant={connectionStatus.chat === "connected" ? "secondary" : "destructive"}
              className={
                connectionStatus.chat === "connected"
                  ? "bg-success/20 text-success"
                  : "bg-destructive/20 text-destructive"
              }
            >
              {connectionStatus.chat === "connected" ? (
                <Wifi className="w-3 h-3 mr-1" />
              ) : (
                <WifiOff className="w-3 h-3 mr-1" />
              )}
              Chat: {connectionStatus.chat}
            </Badge>
          </div>
        </div>
      </div>

      {/* Connection Status Alert */}
      {!isConnected && (
        <div className="mb-6 p-4 bg-destructive/20 border border-destructive rounded-lg">
          <div className="flex items-center">
            <WifiOff className="w-5 h-5 text-destructive mr-2" />
            <span className="text-destructive font-medium">WebSocket Connection Failed</span>
          </div>
          <p className="text-sm text-muted-foreground mt-1">
            Unable to connect to backend server. Please ensure the backend is running on port 8000.
          </p>
        </div>
      )}

      {/* Agent Workflow */}
      <div className="mb-6">
        <Card className="p-4 bg-gradient-secondary border-border">
          <h2 className="font-semibold mb-3 text-primary">Agent Workflow</h2>
          <div className="grid grid-cols-7 gap-2 mb-4">
            <div className="text-center">
              <div className="w-12 h-12 mx-auto mb-2 bg-primary/20 rounded-lg flex items-center justify-center">
                <span className="text-primary font-bold">🎯</span>
              </div>
              <p className="text-xs text-muted-foreground">Orchestrator</p>
            </div>
            <div className="text-center">
              <div className="w-12 h-12 mx-auto mb-2 bg-destructive/20 rounded-lg flex items-center justify-center">
                <span className="text-destructive font-bold">1</span>
              </div>
              <p className="text-xs text-muted-foreground">Fault Detection</p>
            </div>
            <div className="text-center">
              <div className="w-12 h-12 mx-auto mb-2 bg-warning/20 rounded-lg flex items-center justify-center">
                <span className="text-warning font-bold">2</span>
              </div>
              <p className="text-xs text-muted-foreground">Performance Analysis</p>
            </div>
            <div className="text-center">
              <div className="w-12 h-12 mx-auto mb-2 bg-info/20 rounded-lg flex items-center justify-center">
                <span className="text-info font-bold">3</span>
              </div>
              <p className="text-xs text-muted-foreground">Inventory Management</p>
            </div>
            <div className="text-center">
              <div className="w-12 h-12 mx-auto mb-2 bg-accent/20 rounded-lg flex items-center justify-center">
                <span className="text-accent font-bold">4</span>
              </div>
              <p className="text-xs text-muted-foreground">Ticket Creation</p>
            </div>
            <div className="text-center">
              <div className="w-12 h-12 mx-auto mb-2 bg-secondary/20 rounded-lg flex items-center justify-center">
                <span className="text-secondary font-bold">5</span>
              </div>
              <p className="text-xs text-muted-foreground">Notifications</p>
            </div>
            <div className="text-center">
              <div className="w-12 h-12 mx-auto mb-2 bg-success/20 rounded-lg flex items-center justify-center">
                <span className="text-success font-bold">6</span>
              </div>
              <p className="text-xs text-muted-foreground">Customer Support</p>
            </div>
          </div>
        </Card>
      </div>

      {/* AI Assistant Query Prompt */}
      <div className="mb-6">
        <Card className="p-4 bg-gradient-secondary border-border">
          <h2 className="font-semibold mb-3 text-primary">AI Assistant Agent</h2>
          <p className="text-muted-foreground text-sm mb-3">
            Ask specific questions about network issues, performance metrics, or troubleshooting
          </p>
          <div className="flex gap-3">
            <Textarea
              placeholder="e.g., Why is server-03 experiencing high CPU usage? What's causing network latency in segment A?"
              value={networkQuery}
              onChange={(e) => setNetworkQuery(e.target.value)}
              className="flex-1 min-h-[80px] resize-none"
              disabled={isProcessing || !isConnected}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  handleSendQuery();
                }
              }}
            />
            <Button
              className="px-4 h-auto"
              onClick={handleSendQuery}
              disabled={isProcessing || !networkQuery.trim() || !isConnected}
            >
              {isProcessing ? "Processing..." : <Send className="w-4 h-4" />}
            </Button>
          </div>
          {!isConnected && (
            <p className="text-destructive text-sm mt-2">
              Cannot send query. WebSocket is not connected.
            </p>
          )}
        </Card>
      </div>

      {/* Logs */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <LogPanel title="System Logs" logs={formattedLogs} />
        <LogPanel
          title="Agent Inputs"
          logs={inputLogs.map((log) => ({
            text: `[${log.timestamp}] ${log.source}: ${log.message}`,
            className: "text-info",
          }))}
        />
        <LogPanel
          title="Agent Outputs"
          logs={outputLogs.map((log) => ({
            text: `[${log.timestamp}] ${log.source}: ${log.message}`,
            className: "text-success",
          }))}
        />
      </div>
    </div>
  );
};

export default Index;
