import agentsData from "@/data/agents.json";
import { Agent } from "@/types";
import AgentCard from "@/components/AgentCard";
import { Bot, Shield, Zap, PauseCircle } from "lucide-react";

export default function AgentsPage() {
  const agents = agentsData as Agent[];
  const active = agents.filter((a) => a.status === "active");
  const idle = agents.filter((a) => a.status === "idle");
  const needsApproval = agents.filter((a) => a.requiresApproval);
  const totalLogs = agents.reduce((acc, a) => acc + a.activityLog.length, 0);

  const categories = Array.from(new Set(agents.map((a) => a.category)));

  return (
    <div className="p-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-xl font-bold text-white flex items-center gap-2">
          <Bot size={18} className="text-blue-400" />
          Agent Hub
        </h1>
        <p className="text-[#6e6e8a] text-sm mt-0.5">
          {agents.length} personal AI agents — permission-controlled, purpose-built.
        </p>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        {[
          { label: "Total Agents", value: agents.length, icon: Bot, color: "text-violet-400", bg: "bg-violet-500/8" },
          { label: "Active", value: active.length, icon: Zap, color: "text-emerald-400", bg: "bg-emerald-500/8" },
          { label: "Idle", value: idle.length, icon: PauseCircle, color: "text-slate-400", bg: "bg-slate-500/8" },
          { label: "Need Approval", value: needsApproval.length, icon: Shield, color: "text-amber-400", bg: "bg-amber-500/8" },
        ].map(({ label, value, icon: Icon, color, bg }) => (
          <div key={label} className={`bg-[#111122] border border-[#1a1a2e] rounded-xl p-4`}>
            <div className={`w-8 h-8 ${bg} rounded-lg flex items-center justify-center mb-2`}>
              <Icon size={14} className={color} />
            </div>
            <div className={`text-xl font-bold ${color}`}>{value}</div>
            <div className="text-[12px] text-[#6e6e8a]">{label}</div>
          </div>
        ))}
      </div>

      {/* Permission layer notice */}
      <div className="bg-violet-500/5 border border-violet-500/15 rounded-xl p-4 mb-6 flex items-start gap-3">
        <Shield size={16} className="text-violet-400 flex-shrink-0 mt-0.5" />
        <div>
          <div className="text-sm font-semibold text-violet-300 mb-1">Permission Layer Active</div>
          <p className="text-[12px] text-[#6e6e8a] leading-relaxed">
            All agents access data through approved connectors only. No unrestricted phone or device access.
            Agents marked <strong className="text-amber-400">Approval Required</strong> need your sign-off before
            executing write actions. Manage permissions on the{" "}
            <a href="/connections" className="text-violet-400 hover:text-violet-300 underline underline-offset-2">
              Connections
            </a>{" "}
            page.
          </p>
        </div>
      </div>

      {/* Agents by category */}
      {categories.map((cat) => {
        const catAgents = agents.filter((a) => a.category === cat);
        return (
          <div key={cat} className="mb-8">
            <h2 className="text-sm font-semibold text-[#6e6e8a] uppercase tracking-widest mb-3">
              {cat}
            </h2>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
              {catAgents.map((agent) => (
                <AgentCard key={agent.id} agent={agent} />
              ))}
            </div>
          </div>
        );
      })}

      {/* Activity summary */}
      <div className="bg-[#111122] border border-[#1a1a2e] rounded-xl p-5 mt-4">
        <h3 className="font-semibold text-slate-100 text-sm mb-3 flex items-center gap-2">
          <Zap size={14} className="text-blue-400" />
          Recent Activity ({totalLogs} total events)
        </h3>
        <div className="space-y-2">
          {agents
            .flatMap((a) =>
              a.activityLog.map((log) => ({ ...log, agentName: a.name, agentIcon: a.icon }))
            )
            .sort((a, b) => b.timestamp.localeCompare(a.timestamp))
            .slice(0, 6)
            .map((log, i) => (
              <div key={i} className="flex items-center gap-3 text-[12px]">
                <span className="text-base">{log.agentIcon}</span>
                <span className="text-[#6e6e8a]">{log.agentName}</span>
                <span className="text-[#3a3a50]">·</span>
                <span className="text-slate-300 flex-1 truncate">{log.action}</span>
                <span
                  className={`flex-shrink-0 px-1.5 py-0.5 rounded text-[10px] font-semibold ${
                    log.status === "success"
                      ? "bg-emerald-500/10 text-emerald-400"
                      : log.status === "pending"
                      ? "bg-amber-500/10 text-amber-400"
                      : "bg-red-500/10 text-red-400"
                  }`}
                >
                  {log.status}
                </span>
              </div>
            ))}
        </div>
      </div>
    </div>
  );
}
