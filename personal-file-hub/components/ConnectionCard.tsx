"use client";

import { useState } from "react";
import { DataConnection } from "@/types";
import { Shield, Wifi, WifiOff } from "lucide-react";

interface ConnectionCardProps {
  connection: DataConnection;
}

const riskConfig = {
  low: { label: "Low risk", classes: "text-emerald-400" },
  medium: { label: "Medium risk", classes: "text-amber-400" },
  high: { label: "High risk", classes: "text-red-400" },
};

const typeLabels: Record<string, string> = {
  oauth: "OAuth",
  "api-key": "API Key",
  "manual-upload": "Manual Upload",
  local: "Local",
};

export default function ConnectionCard({ connection: initial }: ConnectionCardProps) {
  const [connection, setConnection] = useState(initial);
  const [loading, setLoading] = useState(false);
  const risk = riskConfig[connection.riskLevel];

  const toggle = async () => {
    setLoading(true);
    try {
      const res = await fetch("/api/connections", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id: connection.id, connected: !connection.connected }),
      });
      if (res.ok) {
        const updated = await res.json();
        setConnection(updated);
      }
    } finally {
      setLoading(false);
    }
  };

  return (
    <div
      className={`bg-[#111122] border rounded-xl p-4 flex items-center gap-4 transition-all duration-200 ${
        connection.connected
          ? "border-[#1e2d3d] hover:border-[#1e3d54]"
          : "border-[#1a1a2e] hover:border-[#2d2d4e]"
      }`}
    >
      {/* Icon */}
      <div className="text-2xl flex-shrink-0">{connection.icon}</div>

      {/* Info */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <h3 className="font-semibold text-sm text-slate-100">{connection.name}</h3>
          <span className="text-[10px] text-[#6e6e8a] bg-white/4 border border-[#1a1a2e] rounded-full px-2 py-0.5 flex-shrink-0">
            {typeLabels[connection.type]}
          </span>
        </div>
        <p className="text-[12px] text-[#6e6e8a] mt-0.5 truncate">{connection.description}</p>
        <div className={`flex items-center gap-1 mt-1 text-[11px] ${risk.classes}`}>
          <Shield size={10} />
          {risk.label}
        </div>
      </div>

      {/* Toggle */}
      <button
        onClick={toggle}
        disabled={loading}
        className={`flex items-center gap-1.5 text-[11px] font-medium px-3 py-1.5 rounded-lg border transition-all duration-200 flex-shrink-0 ${
          connection.connected
            ? "bg-emerald-500/10 text-emerald-400 border-emerald-500/20 hover:bg-emerald-500/15"
            : "bg-white/4 text-[#6e6e8a] border-[#1a1a2e] hover:border-[#2d2d4e] hover:text-slate-300"
        } disabled:opacity-50`}
      >
        {connection.connected ? (
          <>
            <Wifi size={11} /> Connected
          </>
        ) : (
          <>
            <WifiOff size={11} /> Connect
          </>
        )}
      </button>
    </div>
  );
}
