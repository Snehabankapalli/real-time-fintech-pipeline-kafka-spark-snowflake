export type Category =
  | "Fitness"
  | "Career"
  | "Finance"
  | "Skincare"
  | "Agents"
  | "Home"
  | "Portfolio"
  | "Experiments";

export type FileStatus = "active" | "draft" | "archived";
export type RiskLevel = "low" | "medium" | "high";
export type AgentStatus = "active" | "idle" | "paused";
export type ConnectionType = "oauth" | "api-key" | "manual-upload" | "local";
export type LogStatus = "success" | "pending" | "failed";

export interface FileMetadata {
  id: string;
  title: string;
  category: Category;
  path: string;
  tags: string[];
  status: FileStatus;
  createdBy: string;
  notes: string;
  createdAt: string;
  updatedAt: string;
}

export interface ActivityLogEntry {
  timestamp: string;
  action: string;
  status: LogStatus;
}

export interface Agent {
  id: string;
  name: string;
  icon: string;
  category: string;
  purpose: string;
  allowedDataSources: string[];
  actions: string[];
  riskLevel: RiskLevel;
  requiresApproval: boolean;
  status: AgentStatus;
  activityLog: ActivityLogEntry[];
  notes: string;
  createdAt: string;
}

export interface DataConnection {
  id: string;
  name: string;
  icon: string;
  description: string;
  connected: boolean;
  type: ConnectionType;
  riskLevel: RiskLevel;
  category: string;
}

export interface Permission {
  id: string;
  agentId: string;
  dataSource: string;
  readAllowed: boolean;
  writeAllowed: boolean;
  requiresApproval: boolean;
}

export interface CategoryConfig {
  name: Category;
  color: string;
  bgColor: string;
  borderColor: string;
  dotColor: string;
  emoji: string;
}

export const CATEGORIES: CategoryConfig[] = [
  {
    name: "Fitness",
    color: "text-emerald-400",
    bgColor: "bg-emerald-500/10",
    borderColor: "border-emerald-500/20",
    dotColor: "bg-emerald-400",
    emoji: "🏋️",
  },
  {
    name: "Career",
    color: "text-blue-400",
    bgColor: "bg-blue-500/10",
    borderColor: "border-blue-500/20",
    dotColor: "bg-blue-400",
    emoji: "🎯",
  },
  {
    name: "Finance",
    color: "text-amber-400",
    bgColor: "bg-amber-500/10",
    borderColor: "border-amber-500/20",
    dotColor: "bg-amber-400",
    emoji: "💰",
  },
  {
    name: "Skincare",
    color: "text-pink-400",
    bgColor: "bg-pink-500/10",
    borderColor: "border-pink-500/20",
    dotColor: "bg-pink-400",
    emoji: "✨",
  },
  {
    name: "Agents",
    color: "text-violet-400",
    bgColor: "bg-violet-500/10",
    borderColor: "border-violet-500/20",
    dotColor: "bg-violet-400",
    emoji: "🤖",
  },
  {
    name: "Home",
    color: "text-orange-400",
    bgColor: "bg-orange-500/10",
    borderColor: "border-orange-500/20",
    dotColor: "bg-orange-400",
    emoji: "🏡",
  },
  {
    name: "Portfolio",
    color: "text-indigo-400",
    bgColor: "bg-indigo-500/10",
    borderColor: "border-indigo-500/20",
    dotColor: "bg-indigo-400",
    emoji: "💼",
  },
  {
    name: "Experiments",
    color: "text-cyan-400",
    bgColor: "bg-cyan-500/10",
    borderColor: "border-cyan-500/20",
    dotColor: "bg-cyan-400",
    emoji: "🧪",
  },
];
