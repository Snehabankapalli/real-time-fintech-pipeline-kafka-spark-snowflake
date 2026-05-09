"use client";

import { useState, useEffect } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { Agent, DataConnection, Permission } from "@/types";
import {
  ArrowLeft,
  Shield,
  CheckCircle2,
  Clock,
  Database,
  Activity,
  FileText,
  Eye,
  Edit3,
  AlertTriangle,
} from "lucide-react";

const riskConfig = {
  low: { label: "Low Risk", classes: "bg-emerald-500/10 text-emerald-400 border-emerald-500/20" },
  medium: { label: "Medium Risk", classes: "bg-amber-500/10 text-amber-400 border-amber-500/20" },
  high: { label: "High Risk", classes: "bg-red-500/10 text-red-400 border-red-500/20" },
};

const statusConfig = {
  active: { dot: "bg-emerald-400", label: "Active", pulse: true },
  idle: { dot: "bg-slate-500", label: "Idle", pulse: false },
  paused: { dot: "bg-amber-400", label: "Paused", pulse: false },
};

export default function AgentDetailPage() {
  const { id } = useParams<{ id: string }>();
  const [agent, setAgent] = useState<Agent | null>(null);
  const [connections, setConnections] = useState<DataConnection[]>([]);
  const [permissions, setPermissions] = useState<Permission[]>([]);
  const [notes, setNotes] = useState("");
  const [savingNotes, setSavingNotes] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetch(`/api/agents/${id}`).then((r) => r.json()),
      fetch("/api/connections").then((r) => r.json()),
      fetch("/api/permissions").then((r) => r.json()),
    ]).then(([a, c, p]) => {
      setAgent(a);
      setNotes(a.notes || "");
      setConnections(c);
      setPermissions(p.filter((perm: Permission) => perm.agentId === id));
      setLoading(false);
    });
  }, [id]);

  const toggleApproval = async () => {
    if (!agent) return;
    const updated = { ...agent, requiresApproval: !agent.requiresApproval };
    setAgent(updated);
    await fetch(`/api/agents/${id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ requiresApproval: updated.requiresApproval }),
    });
  };

  const saveNotes = async () => {
    setSavingNotes(true);
    await fetch(`/api/agents/${id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ notes }),
    });
    setSavingNotes(false);
  };

  if (loading) {
    return (
      <div className="p-6 space-y-4">
        {[...Array(4)].map((_, i) => (
          <div key={i} className="bg-[#111122] border border-[#1a1a2e] rounded-xl h-24 animate-pulse" />
        ))}
      </div>
    );
  }

  if (!agent) {
    return (
      <div className="p-6 text-center">
        <p className="text-[#6e6e8a]">Agent not found.</p>
        <Link href="/agents" className="text-violet-400 hover:text-violet-300 text-sm mt-2 inline-block">
          ← Back to Agents
        </Link>
      </div>
    );
  }

  const risk = riskConfig[agent.riskLevel];
  const status = statusConfig[agent.status];

  return (
    <div className="p-6 max-w-4xl mx-auto">
      {/* Back */}
      <Link
        href="/agents"
        className="inline-flex items-center gap-1.5 text-xs text-[#6e6e8a] hover:text-slate-300 mb-5 transition-colors"
      >
        <ArrowLeft size={13} />
        Agent Hub
      </Link>

      {/* Header card */}
      <div className="bg-[#111122] border border-[#1a1a2e] rounded-2xl p-6 mb-5">
        <div className="flex items-start justify-between flex-wrap gap-4">
          <div className="flex items-center gap-4">
            <div className="text-4xl">{agent.icon}</div>
            <div>
              <h1 className="text-xl font-bold text-white">{agent.name}</h1>
              <div className="flex items-center gap-3 mt-1 flex-wrap">
                <span className="text-sm text-[#6e6e8a]">{agent.category}</span>
                <div className="flex items-center gap-1.5">
                  <div className={`w-2 h-2 rounded-full ${status.dot} ${status.pulse ? "animate-pulse" : ""}`} />
                  <span className="text-sm text-[#6e6e8a]">{status.label}</span>
                </div>
              </div>
            </div>
          </div>

          <div className="flex items-center gap-2 flex-wrap">
            <span className={`text-xs font-semibold px-2.5 py-1 rounded-full border flex items-center gap-1 ${risk.classes}`}>
              <Shield size={10} />
              {risk.label}
            </span>

            {agent.riskLevel === "high" && (
              <span className="text-xs text-red-400 flex items-center gap-1">
                <AlertTriangle size={11} />
                Handle with care
              </span>
            )}
          </div>
        </div>

        <p className="mt-4 text-sm text-[#94a3b8] leading-relaxed">{agent.purpose}</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
        {/* Actions */}
        <div className="bg-[#111122] border border-[#1a1a2e] rounded-xl p-5">
          <h2 className="font-semibold text-slate-100 text-sm mb-3 flex items-center gap-2">
            <Activity size={14} className="text-violet-400" />
            Actions It Can Perform
          </h2>
          <div className="space-y-2">
            {agent.actions.map((action) => (
              <div
                key={action}
                className="flex items-center gap-2 text-sm text-[#94a3b8] bg-white/3 border border-[#1a1a2e] rounded-lg px-3 py-2"
              >
                <span className="w-1.5 h-1.5 rounded-full bg-violet-400 flex-shrink-0" />
                {action.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())}
              </div>
            ))}
          </div>
        </div>

        {/* Data Sources + Permissions */}
        <div className="bg-[#111122] border border-[#1a1a2e] rounded-xl p-5">
          <h2 className="font-semibold text-slate-100 text-sm mb-3 flex items-center gap-2">
            <Database size={14} className="text-blue-400" />
            Allowed Data Sources
          </h2>
          <div className="space-y-2">
            {agent.allowedDataSources.map((sourceId) => {
              const conn = connections.find((c) => c.id === sourceId);
              const perm = permissions.find((p) => p.dataSource === sourceId);
              return (
                <div
                  key={sourceId}
                  className="flex items-center justify-between gap-2 bg-white/3 border border-[#1a1a2e] rounded-lg px-3 py-2"
                >
                  <div className="flex items-center gap-2">
                    <span className="text-base">{conn?.icon || "📦"}</span>
                    <span className="text-sm text-[#94a3b8]">{conn?.name || sourceId}</span>
                  </div>
                  <div className="flex items-center gap-1.5">
                    {perm?.readAllowed && (
                      <span className="flex items-center gap-0.5 text-[10px] text-emerald-400 bg-emerald-500/8 border border-emerald-500/15 rounded px-1.5 py-0.5">
                        <Eye size={9} /> R
                      </span>
                    )}
                    {perm?.writeAllowed && (
                      <span className="flex items-center gap-0.5 text-[10px] text-blue-400 bg-blue-500/8 border border-blue-500/15 rounded px-1.5 py-0.5">
                        <Edit3 size={9} /> W
                      </span>
                    )}
                    {perm?.requiresApproval && (
                      <span className="flex items-center gap-0.5 text-[10px] text-amber-400 bg-amber-500/8 border border-amber-500/15 rounded px-1.5 py-0.5">
                        <CheckCircle2 size={9} /> Approval
                      </span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Approval toggle */}
        <div className="bg-[#111122] border border-[#1a1a2e] rounded-xl p-5">
          <div className="flex items-center justify-between mb-3">
            <h2 className="font-semibold text-slate-100 text-sm flex items-center gap-2">
              <CheckCircle2 size={14} className="text-amber-400" />
              Requires Approval
            </h2>
            <button
              onClick={toggleApproval}
              className={`w-11 h-6 rounded-full border transition-all duration-300 relative ${
                agent.requiresApproval
                  ? "bg-violet-600 border-violet-500"
                  : "bg-[#1a1a2e] border-[#2d2d4e]"
              }`}
            >
              <span
                className={`absolute top-0.5 left-0.5 w-5 h-5 rounded-full bg-white transition-transform duration-300 ${
                  agent.requiresApproval ? "translate-x-5" : ""
                }`}
              />
            </button>
          </div>
          <p className="text-[12px] text-[#6e6e8a] leading-relaxed">
            {agent.requiresApproval
              ? "This agent will pause and wait for your approval before executing write actions."
              : "This agent operates autonomously within its approved data sources."}
          </p>
        </div>

        {/* Activity Log */}
        <div className="bg-[#111122] border border-[#1a1a2e] rounded-xl p-5">
          <h2 className="font-semibold text-slate-100 text-sm mb-3 flex items-center gap-2">
            <Clock size={14} className="text-[#6e6e8a]" />
            Activity Log
          </h2>
          {agent.activityLog.length === 0 ? (
            <p className="text-[12px] text-[#6e6e8a]">No activity recorded yet.</p>
          ) : (
            <div className="space-y-2">
              {agent.activityLog.map((log, i) => (
                <div key={i} className="flex items-start gap-2">
                  <span
                    className={`mt-0.5 w-1.5 h-1.5 rounded-full flex-shrink-0 ${
                      log.status === "success"
                        ? "bg-emerald-400"
                        : log.status === "pending"
                        ? "bg-amber-400"
                        : "bg-red-400"
                    }`}
                  />
                  <div className="flex-1 min-w-0">
                    <div className="text-[12px] text-slate-300 truncate">{log.action}</div>
                    <div className="text-[11px] text-[#6e6e8a]">
                      {new Date(log.timestamp).toLocaleString()}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Notes / Memory */}
      <div className="bg-[#111122] border border-[#1a1a2e] rounded-xl p-5 mt-5">
        <h2 className="font-semibold text-slate-100 text-sm mb-3 flex items-center gap-2">
          <FileText size={14} className="text-[#6e6e8a]" />
          Notes & Memory
        </h2>
        <textarea
          value={notes}
          onChange={(e) => setNotes(e.target.value)}
          rows={4}
          placeholder="Add private notes, constraints, or memory for this agent..."
          className="w-full bg-[#080811] border border-[#1a1a2e] rounded-lg px-3 py-2.5 text-sm text-slate-100 focus:outline-none focus:border-violet-500/50 transition-colors resize-none"
        />
        <button
          onClick={saveNotes}
          disabled={savingNotes}
          className="mt-2 bg-violet-600/15 hover:bg-violet-600/25 border border-violet-500/20 text-violet-400 text-sm font-medium px-4 py-2 rounded-lg transition-all disabled:opacity-50"
        >
          {savingNotes ? "Saving..." : "Save Notes"}
        </button>
      </div>
    </div>
  );
}
