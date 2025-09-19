import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { Activity, Cpu, AlertTriangle, Network } from "lucide-react";

export interface Agent {
  id: string;
  name: string;
  type: string;
  status: "active" | "processing" | "idle" | "error";
  description: string;
  lastActivity: string;
  metrics: {
    requests: number;
    success: number;
    errors: number;
  };
}

interface AgentCardProps {
  agent: Agent;
  isSelected: boolean;
  onSelect: (agent: Agent) => void;
}

const statusConfig = {
  active: {
    color: "text-agent-active",
    bgColor: "bg-agent-active/20",
    borderColor: "border-agent-active",
    icon: Activity,
    label: "Active"
  },
  processing: {
    color: "text-agent-processing",
    bgColor: "bg-agent-processing/20",
    borderColor: "border-agent-processing",
    icon: Cpu,
    label: "Processing"
  },
  idle: {
    color: "text-agent-idle",
    bgColor: "bg-agent-idle/20",
    borderColor: "border-agent-idle",
    icon: Network,
    label: "Idle"
  },
  error: {
    color: "text-agent-error",
    bgColor: "bg-agent-error/20",
    borderColor: "border-agent-error",
    icon: AlertTriangle,
    label: "Error"
  }
};

export function AgentCard({ agent, isSelected, onSelect }: AgentCardProps) {
  const config = statusConfig[agent.status];
  const StatusIcon = config.icon;

  return (
    <Card
      className={cn(
        "p-4 cursor-pointer transition-all duration-200 border-2",
        "bg-gradient-secondary shadow-card hover:shadow-lg",
        config.borderColor,
        isSelected && "border-primary shadow-glow bg-gradient-primary/10",
        agent.status === "processing" && "animate-pulse"
      )}
      onClick={() => onSelect(agent)}
    >
      <div className="flex items-start justify-between mb-3">
        <div className="flex items-center gap-2">
          <StatusIcon className={cn("h-4 w-4", config.color)} />
          <h3 className="font-semibold text-sm">{agent.name}</h3>
        </div>
        <Badge 
          variant="secondary" 
          className={cn("text-xs", config.bgColor, config.color)}
        >
          {config.label}
        </Badge>
      </div>
      
      <p className="text-xs text-muted-foreground mb-3 line-clamp-2">
        {agent.description}
      </p>
      
      <div className="grid grid-cols-3 gap-2 text-xs">
        <div className="text-center">
          <div className="font-medium text-foreground">{agent.metrics.requests}</div>
          <div className="text-muted-foreground">Requests</div>
        </div>
        <div className="text-center">
          <div className="font-medium text-success">{agent.metrics.success}</div>
          <div className="text-muted-foreground">Success</div>
        </div>
        <div className="text-center">
          <div className="font-medium text-destructive">{agent.metrics.errors}</div>
          <div className="text-muted-foreground">Errors</div>
        </div>
      </div>
      
      <div className="mt-3 pt-3 border-t border-border">
        <div className="text-xs text-muted-foreground">
          Last activity: {agent.lastActivity}
        </div>
      </div>
    </Card>
  );
}