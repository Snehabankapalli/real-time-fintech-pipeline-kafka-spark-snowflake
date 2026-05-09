import Link from "next/link";
import filesData from "@/data/files.json";
import agentsData from "@/data/agents.json";
import connectionsData from "@/data/connections.json";
import { FileMetadata, Agent, DataConnection, CATEGORIES } from "@/types";
import {
  Files,
  Bot,
  Link2,
  Upload,
  TrendingUp,
  ArrowRight,
  Zap,
  Shield,
} from "lucide-react";

export default function DashboardPage() {
  const files = filesData as FileMetadata[];
  const agents = agentsData as Agent[];
  const connections = connectionsData as DataConnection[];

  const activeAgents = agents.filter((a) => a.status === "active").length;
  const connectedSources = connections.filter((c) => c.connected).length;
  const recentFiles = files.slice(0, 4);
  const activeAgentsList = agents.filter((a) => a.status === "active").slice(0, 3);
  const approvalAgents = agents.filter((a) => a.requiresApproval).length;

  const hour = new Date().getHours();
  const greeting =
    hour < 12 ? "Good morning" : hour < 17 ? "Good afternoon" : "Good evening";

  return (
    <div className="p-6 max-w-6xl mx-auto">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-white">
          {greeting}{" "}
          <span className="inline-block animate-pulse">
            {hour < 12 ? "☀️" : hour < 17 ? "🌤️" : "🌙"}
          </span>
        </h1>
        <p className="text-[#6e6e8a] mt-1 text-sm">
          Your AI command center — everything in one place.
        </p>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        {[
          {
            label: "Vault Files",
            value: files.length,
            icon: Files,
            color: "text-violet-400",
            bg: "bg-violet-500/8",
            border: "border-violet-500/15",
            href: "/files",
          },
          {
            label: "Active Agents",
            value: activeAgents,
            icon: Bot,
            color: "text-blue-400",
            bg: "bg-blue-500/8",
            border: "border-blue-500/15",
            href: "/agents",
          },
          {
            label: "Connected Sources",
            value: connectedSources,
            icon: Link2,
            color: "text-emerald-400",
            bg: "bg-emerald-500/8",
            border: "border-emerald-500/15",
            href: "/connections",
          },
          {
            label: "Approval Required",
            value: approvalAgents,
            icon: Shield,
            color: "text-amber-400",
            bg: "bg-amber-500/8",
            border: "border-amber-500/15",
            href: "/agents",
          },
        ].map(({ label, value, icon: Icon, color, bg, border, href }) => (
          <Link
            key={label}
            href={href}
            className={`group bg-[#111122] border ${border} rounded-xl p-5 hover:scale-[1.02] transition-all duration-200 hover:shadow-lg hover:shadow-black/30`}
          >
            <div className={`w-9 h-9 ${bg} rounded-lg flex items-center justify-center mb-3`}>
              <Icon size={16} className={color} />
            </div>
            <div className={`text-2xl font-bold ${color}`}>{value}</div>
            <div className="text-[12px] text-[#6e6e8a] mt-0.5">{label}</div>
          </Link>
        ))}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Recent Files */}
        <div className="lg:col-span-2">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-semibold text-slate-100 flex items-center gap-2">
              <TrendingUp size={15} className="text-violet-400" />
              Recent Files
            </h2>
            <Link
              href="/files"
              className="text-[12px] text-violet-400 hover:text-violet-300 flex items-center gap-1 transition-colors"
            >
              View all <ArrowRight size={12} />
            </Link>
          </div>

          <div className="space-y-2">
            {recentFiles.map((file) => {
              const cat = CATEGORIES.find((c) => c.name === file.category);
              return (
                <Link
                  key={file.id}
                  href={`/files/${file.id}`}
                  className="group flex items-center gap-4 bg-[#111122] border border-[#1a1a2e] rounded-xl p-4 hover:border-[#2d2d4e] hover:shadow-md hover:shadow-black/20 transition-all duration-150"
                >
                  <div className="text-xl">{cat?.emoji}</div>
                  <div className="flex-1 min-w-0">
                    <div className="font-medium text-sm text-slate-100 truncate group-hover:text-white transition-colors">
                      {file.title}
                    </div>
                    <div className="flex items-center gap-2 mt-0.5">
                      <span className={`text-[10px] ${cat?.color}`}>{file.category}</span>
                      <span className="text-[#3a3a50]">·</span>
                      <span className="text-[11px] text-[#6e6e8a]">{file.createdAt}</span>
                    </div>
                  </div>
                  <div className="flex items-center gap-1.5">
                    {file.tags.slice(0, 2).map((tag) => (
                      <span
                        key={tag}
                        className="hidden sm:inline text-[10px] text-[#6e6e8a] bg-white/4 border border-[#1a1a2e] rounded-full px-2 py-0.5"
                      >
                        #{tag}
                      </span>
                    ))}
                    <span
                      className={`text-[10px] font-semibold px-2 py-0.5 rounded-full border ${
                        file.status === "active"
                          ? "bg-emerald-500/10 text-emerald-400 border-emerald-500/20"
                          : "bg-amber-500/10 text-amber-400 border-amber-500/20"
                      }`}
                    >
                      {file.status}
                    </span>
                  </div>
                </Link>
              );
            })}
          </div>
        </div>

        {/* Right column */}
        <div className="space-y-6">
          {/* Active Agents */}
          <div>
            <div className="flex items-center justify-between mb-4">
              <h2 className="font-semibold text-slate-100 flex items-center gap-2">
                <Zap size={15} className="text-blue-400" />
                Active Agents
              </h2>
              <Link
                href="/agents"
                className="text-[12px] text-violet-400 hover:text-violet-300 flex items-center gap-1 transition-colors"
              >
                View all <ArrowRight size={12} />
              </Link>
            </div>

            <div className="space-y-2">
              {activeAgentsList.map((agent) => (
                <Link
                  key={agent.id}
                  href={`/agents/${agent.id}`}
                  className="group flex items-center gap-3 bg-[#111122] border border-[#1a1a2e] rounded-xl p-3 hover:border-[#2d2d4e] transition-all duration-150"
                >
                  <span className="text-lg">{agent.icon}</span>
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-medium text-slate-100 truncate group-hover:text-white transition-colors">
                      {agent.name}
                    </div>
                    <div className="flex items-center gap-1 mt-0.5">
                      <div className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                      <span className="text-[11px] text-[#6e6e8a]">Active</span>
                    </div>
                  </div>
                  {agent.requiresApproval && (
                    <Shield size={12} className="text-amber-400 flex-shrink-0" />
                  )}
                </Link>
              ))}
            </div>
          </div>

          {/* Quick Actions */}
          <div>
            <h2 className="font-semibold text-slate-100 mb-3">Quick Actions</h2>
            <div className="space-y-2">
              <Link
                href="/upload"
                className="flex items-center gap-3 bg-violet-600/10 hover:bg-violet-600/15 border border-violet-500/20 hover:border-violet-500/30 text-violet-300 rounded-xl px-4 py-3 text-sm font-medium transition-all duration-150"
              >
                <Upload size={15} />
                Upload new HTML file
              </Link>
              <Link
                href="/connections"
                className="flex items-center gap-3 bg-white/4 hover:bg-white/6 border border-[#1a1a2e] hover:border-[#2d2d4e] text-slate-300 rounded-xl px-4 py-3 text-sm font-medium transition-all duration-150"
              >
                <Link2 size={15} />
                Manage data sources
              </Link>
            </div>
          </div>

          {/* Permission reminder */}
          <div className="bg-[#0f0f1a] border border-violet-500/10 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-2">
              <Shield size={13} className="text-violet-400" />
              <span className="text-xs font-semibold text-violet-300">Permission Layer</span>
            </div>
            <p className="text-[11px] text-[#6e6e8a] leading-relaxed">
              All agents access data through approved connectors only. {approvalAgents} agents
              require manual approval for write actions.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
