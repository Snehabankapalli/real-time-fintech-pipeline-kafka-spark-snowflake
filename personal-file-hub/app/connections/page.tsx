"use client";

import { useState, useEffect } from "react";
import { DataConnection, Permission, Agent } from "@/types";
import ConnectionCard from "@/components/ConnectionCard";
import { Link2, Shield, Eye, Edit3, CheckCircle2, Info } from "lucide-react";

export default function ConnectionsPage() {
  const [connections, setConnections] = useState<DataConnection[]>([]);
  const [permissions, setPermissions] = useState<Permission[]>([]);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<"connections" | "permissions">("connections");

  useEffect(() => {
    Promise.all([
      fetch("/api/connections").then((r) => r.json()),
      fetch("/api/permissions").then((r) => r.json()),
      fetch("/api/agents").then((r) => r.json()),
    ]).then(([c, p, a]) => {
      setConnections(c);
      setPermissions(p);
      setAgents(a);
      setLoading(false);
    });
  }, []);

  const categories = Array.from(new Set(connections.map((c) => c.category)));
  const connected = connections.filter((c) => c.connected).length;

  const togglePermission = async (perm: Permission, field: "readAllowed" | "writeAllowed" | "requiresApproval") => {
    const updated = { ...perm, [field]: !perm[field] };
    setPermissions((prev) => prev.map((p) => (p.id === perm.id ? updated : p)));
    await fetch("/api/permissions", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(updated),
    });
  };

  return (
    <div className="p-6 max-w-5xl mx-auto">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-xl font-bold text-white flex items-center gap-2">
          <Link2 size={18} className="text-emerald-400" />
          Data Connections
        </h1>
        <p className="text-[#6e6e8a] text-sm mt-0.5">
          {connected} of {connections.length} sources connected. Control exactly what your agents can access.
        </p>
      </div>

      {/* Security notice */}
      <div className="bg-[#0f1a1a] border border-emerald-500/15 rounded-xl p-4 mb-6 flex items-start gap-3">
        <Shield size={16} className="text-emerald-400 flex-shrink-0 mt-0.5" />
        <div>
          <div className="text-sm font-semibold text-emerald-300 mb-1">
            Permission-Based Access Model
          </div>
          <p className="text-[12px] text-[#6e6e8a] leading-relaxed">
            Agents can only read/write data sources you explicitly approve below. No agent has unrestricted
            device, phone, or cloud access. Toggle connections here — then set per-agent permissions in the
            Permissions tab.
          </p>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 p-1 bg-[#111122] border border-[#1a1a2e] rounded-xl mb-6 w-fit">
        {(["connections", "permissions"] as const).map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            className={`px-4 py-2 rounded-lg text-sm font-medium capitalize transition-all ${
              activeTab === tab
                ? "bg-violet-600 text-white"
                : "text-[#6e6e8a] hover:text-slate-300"
            }`}
          >
            {tab}
          </button>
        ))}
      </div>

      {loading ? (
        <div className="space-y-3">
          {[...Array(6)].map((_, i) => (
            <div key={i} className="bg-[#111122] border border-[#1a1a2e] rounded-xl h-16 animate-pulse" />
          ))}
        </div>
      ) : activeTab === "connections" ? (
        <div className="space-y-6">
          {categories.map((cat) => (
            <div key={cat}>
              <h3 className="text-[11px] font-semibold text-[#6e6e8a] uppercase tracking-widest mb-2">
                {cat}
              </h3>
              <div className="space-y-2">
                {connections
                  .filter((c) => c.category === cat)
                  .map((conn) => (
                    <ConnectionCard key={conn.id} connection={conn} />
                  ))}
              </div>
            </div>
          ))}
        </div>
      ) : (
        /* Permissions matrix */
        <div>
          <div className="flex items-center gap-4 mb-4 text-[11px] text-[#6e6e8a] flex-wrap">
            <span className="flex items-center gap-1">
              <span className="w-3 h-3 rounded bg-emerald-500/15 border border-emerald-500/25 inline-block" />
              <Eye size={10} /> Read allowed
            </span>
            <span className="flex items-center gap-1">
              <span className="w-3 h-3 rounded bg-blue-500/15 border border-blue-500/25 inline-block" />
              <Edit3 size={10} /> Write allowed
            </span>
            <span className="flex items-center gap-1">
              <span className="w-3 h-3 rounded bg-amber-500/15 border border-amber-500/25 inline-block" />
              <CheckCircle2 size={10} /> Requires approval
            </span>
          </div>

          <div className="space-y-3">
            {agents.map((agent) => {
              const agentPerms = permissions.filter((p) => p.agentId === agent.id);
              if (agentPerms.length === 0) return null;

              return (
                <div key={agent.id} className="bg-[#111122] border border-[#1a1a2e] rounded-xl p-4">
                  <div className="flex items-center gap-2 mb-3">
                    <span className="text-lg">{agent.icon}</span>
                    <span className="font-semibold text-sm text-slate-100">{agent.name}</span>
                    <span
                      className={`text-[10px] px-2 py-0.5 rounded-full border font-semibold ${
                        agent.riskLevel === "low"
                          ? "bg-emerald-500/10 text-emerald-400 border-emerald-500/20"
                          : agent.riskLevel === "medium"
                          ? "bg-amber-500/10 text-amber-400 border-amber-500/20"
                          : "bg-red-500/10 text-red-400 border-red-500/20"
                      }`}
                    >
                      {agent.riskLevel} risk
                    </span>
                  </div>

                  <div className="space-y-2">
                    {agentPerms.map((perm) => {
                      const conn = connections.find((c) => c.id === perm.dataSource);
                      return (
                        <div
                          key={perm.id}
                          className="flex items-center gap-3 bg-[#080811] rounded-lg px-3 py-2.5"
                        >
                          <span className="text-base">{conn?.icon || "📦"}</span>
                          <span className="text-xs text-[#94a3b8] flex-1">
                            {conn?.name || perm.dataSource}
                          </span>

                          <div className="flex items-center gap-2">
                            {(["readAllowed", "writeAllowed", "requiresApproval"] as const).map(
                              (field) => {
                                const active = perm[field];
                                const Icon = field === "readAllowed" ? Eye : field === "writeAllowed" ? Edit3 : CheckCircle2;
                                const color = field === "readAllowed" ? "emerald" : field === "writeAllowed" ? "blue" : "amber";
                                return (
                                  <button
                                    key={field}
                                    onClick={() => togglePermission(perm, field)}
                                    title={field}
                                    className={`w-7 h-7 rounded-lg border flex items-center justify-center transition-all ${
                                      active
                                        ? `bg-${color}-500/15 border-${color}-500/25 text-${color}-400`
                                        : "bg-white/3 border-[#1a1a2e] text-[#6e6e8a] hover:border-[#2d2d4e]"
                                    }`}
                                  >
                                    <Icon size={11} />
                                  </button>
                                );
                              }
                            )}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              );
            })}
          </div>

          <div className="mt-4 flex items-start gap-2 text-[12px] text-[#6e6e8a]">
            <Info size={12} className="flex-shrink-0 mt-0.5 text-violet-400" />
            Click the icons to toggle permissions per agent. Changes are saved instantly to{" "}
            <code className="text-violet-400 font-mono">data/permissions.json</code>.
          </div>
        </div>
      )}
    </div>
  );
}
