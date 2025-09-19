import { useEffect, useState, useRef } from "react";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { cn } from "@/lib/utils";

interface LogEntry {
  id: string;
  timestamp: string;
  level: "info" | "warning" | "error" | "success";
  source: string;
  message: string;
  data?: any;
}

interface LogPanelProps {
  selectedAgent?: {
    id: string;
    name: string;
  };
}

const logLevelConfig = {
  info: { color: "text-primary", bg: "bg-primary/10" },
  warning: { color: "text-warning", bg: "bg-warning/10" },
  error: { color: "text-destructive", bg: "bg-destructive/10" },
  success: { color: "text-success", bg: "bg-success/10" }
};

export function LogPanel({ selectedAgent }: LogPanelProps) {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [inputLogs, setInputLogs] = useState<LogEntry[]>([]);
  const [outputLogs, setOutputLogs] = useState<LogEntry[]>([]);
  const scrollRef = useRef<HTMLDivElement>(null);

  // Simulate real-time logs
  useEffect(() => {
    const generateLog = (type: "main" | "input" | "output"): LogEntry => {
      const sources = selectedAgent 
        ? [selectedAgent.name]
        : ["Fault Agent", "Performance Agent", "Ticket Agent", "Orchestrator"];
      
      const messages = {
        main: [
          "Processing network anomaly detection",
          "Analyzing performance metrics",
          "Generating trouble ticket",
          "Correlating fault patterns",
          "Updating agent state",
          "Broadcasting status update"
        ],
        input: [
          "Received telemetry data",
          "Processing incoming alerts",
          "Parsing configuration update",
          "Validating input parameters"
        ],
        output: [
          "Sending response to orchestrator",
          "Publishing metrics to dashboard",
          "Dispatching notification",
          "Updating external systems"
        ]
      };

      const levels: Array<LogEntry['level']> = ["info", "info", "info", "warning", "error", "success"];
      
      return {
        id: Math.random().toString(36).substr(2, 9),
        timestamp: new Date().toLocaleTimeString(),
        level: levels[Math.floor(Math.random() * levels.length)],
        source: sources[Math.floor(Math.random() * sources.length)],
        message: messages[type][Math.floor(Math.random() * messages[type].length)],
        data: Math.random() > 0.7 ? { 
          duration: `${Math.floor(Math.random() * 500)}ms`,
          memory: `${Math.floor(Math.random() * 100)}MB`
        } : undefined
      };
    };

    const interval = setInterval(() => {
      setLogs(prev => [...prev.slice(-49), generateLog("main")]);
      
      if (Math.random() > 0.6) {
        setInputLogs(prev => [...prev.slice(-19), generateLog("input")]);
      }
      
      if (Math.random() > 0.7) {
        setOutputLogs(prev => [...prev.slice(-19), generateLog("output")]);
      }
    }, 2000);

    return () => clearInterval(interval);
  }, [selectedAgent]);

  // Auto-scroll to bottom
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [logs]);

  const LogList = ({ logs, title }: { logs: LogEntry[], title: string }) => (
    <Card className="h-full bg-gradient-secondary border-border">
      <div className="p-3 border-b border-border">
        <h3 className="font-medium text-sm flex items-center justify-between">
          {title}
          <Badge variant="secondary" className="text-xs">
            {logs.length}
          </Badge>
        </h3>
      </div>
      <ScrollArea className="h-[calc(100%-60px)] p-3">
        <div className="space-y-2">
          {logs.map((log) => {
            const config = logLevelConfig[log.level];
            return (
              <div key={log.id} className={cn("p-2 rounded-md text-xs", config.bg)}>
                <div className="flex items-center justify-between mb-1">
                  <div className="flex items-center gap-2">
                    <span className={cn("font-medium", config.color)}>
                      {log.level.toUpperCase()}
                    </span>
                    <span className="text-muted-foreground">{log.source}</span>
                  </div>
                  <span className="text-muted-foreground">{log.timestamp}</span>
                </div>
                <div className="text-foreground">{log.message}</div>
                {log.data && (
                  <div className="mt-1 text-muted-foreground font-mono">
                    {Object.entries(log.data).map(([key, value]) => (
                      <span key={key} className="mr-3">
                        {key}: {String(value)}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </ScrollArea>
    </Card>
  );

  return (
    <div className="h-full flex flex-col gap-4">
      {/* Input/Output Panels */}
      <div className="grid grid-cols-2 gap-4 h-1/3">
        <LogList logs={inputLogs} title="Agent Input" />
        <LogList logs={outputLogs} title="Agent Output" />
      </div>
      
      {/* Main Logs */}
      <div className="flex-1">
        <LogList logs={logs} title={selectedAgent ? `${selectedAgent.name} Logs` : "System Logs"} />
      </div>
    </div>
  );
}