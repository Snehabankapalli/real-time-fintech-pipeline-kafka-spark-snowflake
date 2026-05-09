"use client";

import Link from "next/link";
import { usePathname, useSearchParams } from "next/navigation";
import { useState } from "react";
import { CATEGORIES } from "@/types";
import {
  LayoutDashboard,
  Files,
  Bot,
  Link2,
  Upload,
  ChevronRight,
  Cpu,
  Menu,
  X,
} from "lucide-react";

interface SidebarProps {
  fileCounts: Record<string, number>;
  totalFiles: number;
  totalAgents: number;
  connectedSources: number;
}

const navItems = [
  { href: "/", label: "Dashboard", icon: LayoutDashboard },
  { href: "/files", label: "Vault Files", icon: Files },
  { href: "/agents", label: "Agent Hub", icon: Bot },
  { href: "/connections", label: "Data Connections", icon: Link2 },
  { href: "/upload", label: "Add File", icon: Upload },
];

export default function Sidebar({
  fileCounts,
  totalFiles,
  totalAgents,
  connectedSources,
}: SidebarProps) {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const activeCategory = searchParams.get("category");
  const [mobileOpen, setMobileOpen] = useState(false);

  const isActive = (href: string) => {
    if (href === "/") return pathname === "/";
    return pathname.startsWith(href);
  };

  const sidebar = (
    <div className="flex flex-col h-full">
      {/* Logo */}
      <div className="px-5 py-5 border-b border-[#1a1a2e]">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-violet-600 flex items-center justify-center flex-shrink-0">
            <Cpu size={16} className="text-white" />
          </div>
          <div>
            <div className="font-bold text-sm text-white">Claude Vault</div>
            <div className="text-[10px] text-[#6e6e8a] mt-0.5">AI Command Center</div>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto py-4 px-3">
        {/* Navigation */}
        <div className="mb-5">
          <div className="px-2 mb-2 text-[10px] font-semibold text-[#6e6e8a] uppercase tracking-widest">
            Navigation
          </div>
          <nav className="space-y-0.5">
            {navItems.map(({ href, label, icon: Icon }) => (
              <Link
                key={href}
                href={href}
                onClick={() => setMobileOpen(false)}
                className={`flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all duration-150 group ${
                  isActive(href)
                    ? "bg-violet-600/20 text-violet-300 border border-violet-500/20"
                    : "text-[#94a3b8] hover:text-white hover:bg-white/5"
                }`}
              >
                <Icon
                  size={15}
                  className={isActive(href) ? "text-violet-400" : "text-[#6e6e8a] group-hover:text-slate-400"}
                />
                {label}
                {isActive(href) && (
                  <ChevronRight size={12} className="ml-auto text-violet-400" />
                )}
              </Link>
            ))}
          </nav>
        </div>

        {/* Categories */}
        <div>
          <div className="px-2 mb-2 text-[10px] font-semibold text-[#6e6e8a] uppercase tracking-widest">
            Categories
          </div>
          <div className="space-y-0.5">
            {CATEGORIES.map((cat) => {
              const count = fileCounts[cat.name] || 0;
              const isActiveCategory =
                pathname.startsWith("/files") && activeCategory === cat.name;

              return (
                <Link
                  key={cat.name}
                  href={`/files?category=${cat.name}`}
                  onClick={() => setMobileOpen(false)}
                  className={`flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm transition-all duration-150 group ${
                    isActiveCategory
                      ? "bg-white/5 text-white"
                      : "text-[#6e6e8a] hover:text-slate-300 hover:bg-white/3"
                  }`}
                >
                  <span className="text-base leading-none">{cat.emoji}</span>
                  <span className="flex-1 text-xs">{cat.name}</span>
                  {count > 0 && (
                    <span
                      className={`text-[10px] font-medium px-1.5 py-0.5 rounded-full ${
                        isActiveCategory
                          ? "bg-violet-500/20 text-violet-400"
                          : "bg-white/5 text-[#6e6e8a]"
                      }`}
                    >
                      {count}
                    </span>
                  )}
                </Link>
              );
            })}
          </div>
        </div>
      </div>

      {/* Footer stats */}
      <div className="px-4 py-4 border-t border-[#1a1a2e]">
        <div className="grid grid-cols-3 gap-2 text-center">
          <div>
            <div className="text-sm font-bold text-violet-400">{totalFiles}</div>
            <div className="text-[10px] text-[#6e6e8a]">Files</div>
          </div>
          <div>
            <div className="text-sm font-bold text-blue-400">{totalAgents}</div>
            <div className="text-[10px] text-[#6e6e8a]">Agents</div>
          </div>
          <div>
            <div className="text-sm font-bold text-emerald-400">{connectedSources}</div>
            <div className="text-[10px] text-[#6e6e8a]">Sources</div>
          </div>
        </div>
      </div>
    </div>
  );

  return (
    <>
      {/* Mobile toggle */}
      <button
        className="fixed top-4 left-4 z-50 md:hidden bg-[#111122] border border-[#1a1a2e] rounded-lg p-2 text-slate-400"
        onClick={() => setMobileOpen((v) => !v)}
      >
        {mobileOpen ? <X size={18} /> : <Menu size={18} />}
      </button>

      {/* Mobile overlay */}
      {mobileOpen && (
        <div
          className="fixed inset-0 z-40 bg-black/60 md:hidden"
          onClick={() => setMobileOpen(false)}
        />
      )}

      {/* Mobile sidebar */}
      <aside
        className={`fixed inset-y-0 left-0 z-50 w-64 bg-[#0c0c18] border-r border-[#1a1a2e] transition-transform duration-300 md:hidden ${
          mobileOpen ? "translate-x-0" : "-translate-x-full"
        }`}
      >
        {sidebar}
      </aside>

      {/* Desktop sidebar */}
      <aside className="hidden md:flex w-64 flex-col bg-[#0c0c18] border-r border-[#1a1a2e] flex-shrink-0">
        {sidebar}
      </aside>
    </>
  );
}
