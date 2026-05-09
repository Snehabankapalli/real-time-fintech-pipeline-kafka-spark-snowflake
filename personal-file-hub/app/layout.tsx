import type { Metadata } from "next";
import "./globals.css";
import { Suspense } from "react";
import Sidebar from "@/components/Sidebar";
import filesData from "@/data/files.json";
import agentsData from "@/data/agents.json";
import connectionsData from "@/data/connections.json";
import { FileMetadata, Agent, DataConnection, Category } from "@/types";

export const metadata: Metadata = {
  title: "Claude Vault",
  description: "Personal AI Command Center — store, organize, and manage your Claude-created files and agents",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  const files = filesData as FileMetadata[];
  const agents = agentsData as Agent[];
  const connections = connectionsData as DataConnection[];

  const fileCounts = files.reduce<Record<string, number>>((acc, f) => {
    acc[f.category] = (acc[f.category] || 0) + 1;
    return acc;
  }, {});

  const connectedSources = connections.filter((c) => c.connected).length;

  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
      </head>
      <body className="bg-[#080811] text-slate-100 antialiased">
        <div className="flex h-screen overflow-hidden">
          <Suspense fallback={null}>
            <Sidebar
              fileCounts={fileCounts}
              totalFiles={files.length}
              totalAgents={agents.length}
              connectedSources={connectedSources}
            />
          </Suspense>
          <main className="flex-1 overflow-y-auto">{children}</main>
        </div>
      </body>
    </html>
  );
}
