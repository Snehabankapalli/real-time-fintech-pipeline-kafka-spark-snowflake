"use client";

import Link from "next/link";
import { Agent } from "@/types";
import { Shield, Clock, ChevronRight, CheckCircle2 } from "lucide-react";

interface AgentCardProps {
  agent: Agent;
}

const riskConfig = {
  low: { label: "Low risk", classes: "bg-emerald-500/10 text-emerald-400 border-emerald-500/20" },
  medium: { label: "Medium risk", classes: "bg-amber-500/10 text-amber-400 border-amber-500/20" },
  high: { label: "High risk", classes: "bg-red-500/10 text-red-400 border-red-500/20" },
};

const statusConfig = {
  active: { dot: "bg-emerald-400", label: "Active", pulse: true },
  idle: { dot: "bg-slate-500", label: "Idle", pulse: false },
  paused: { dot: "bg-amber-400", label: "Paused", pulse: false },
};

export default function AgentCard({ agent }: AgentCardProps) {
  const risk = riskConfig[agent.riskLevel];
  const status = statusConfig[agent.status];
  const lastActivity = agent.activityLog[0];

  return (
    <div className="group bg-[#111122] border border-[#1a1a2e] rounded-xl p-5 flex flex-col gap-4 hover:border-[#2d2d4e] hover:shadow-lg hover:shadow-violet-950/20 transition-all duration-200">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div className="flex items-center gap-3">
          <div className="text-2xl">{agent.icon}</div>
          <div>
            <h3 className="font-semibold text-slate-100 text-sm group-hover:text-white transition-colors">
              {agent.name}
            </h3>
            <div className="text-[11px] text-[#6e6e8a] mt-0.5">{agent.category}</div>
          </div>
        </div>

        <div className="flex items-center gap-1.5">
          <div
            className={`w-2 h-2 rounded-full ${status.dot} ${
              status.pulse ? "animate-pulse" : ""
            }`}
          />
          <span className="text-[11px] text-[#6e6e8a]">{status.label}</span>
        </div>
      </div>

      {/* Purpose */}
      <p className="text-[12px] text-[#6e6e8a] leading-relaxed line-clamp-2">{agent.purpose}</p>

      {/* Badges */}
      <div className="flex items-center gap-2 flex-wrap">
        <span className={`text-[10px] font-semibold px-2 py-0.5 rounded-full border flex items-center gap-1 ${risk.classes}`}>
          <Shield size={9} />
          {risk.label}
        </span>

        {agent.requiresApproval && (
          <span className="text-[10px] font-semibold px-2 py-0.5 rounded-full border bg-violet-500/10 text-violet-400 border-violet-500/20 flex items-center gap-1">
            <CheckCircle2 size={9} />
            Approval required
          </span>
        )}

        <span className="text-[10px] text-[#6e6e8a] bg-white/4 border border-[#1a1a2e] rounded-full px-2 py-0.5">
          {agent.allowedDataSources.length} sources
        </span>
      </div>

      {/* Last activity */}
      {lastActivity && (
        <div className="text-[11px] text-[#6e6e8a] flex items-center gap-1.5 border-t border-[#1a1a2e] pt-3">
          <Clock size={10} />
          <span className="truncate">{lastActivity.action}</span>
        </div>
      )}

      {/* CTA */}
      <Link
        href={`/agents/${agent.id}`}
        className="flex items-center justify-between text-[11px] text-violet-400 hover:text-violet-300 bg-violet-500/8 hover:bg-violet-500/12 border border-violet-500/15 hover:border-violet-500/25 rounded-lg px-3 py-2 transition-all duration-150 group/btn mt-auto"
      >
        View Details
        <ChevronRight size={12} className="group-hover/btn:translate-x-0.5 transition-transform" />
      </Link>
    </div>
  );
}
