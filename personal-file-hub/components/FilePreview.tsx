"use client";

import { useState } from "react";
import Link from "next/link";
import { FileMetadata, CATEGORIES } from "@/types";
import { ArrowLeft, Maximize2, Minimize2, ExternalLink, Pencil, Clock, Tag } from "lucide-react";
import EditFileModal from "./EditFileModal";

interface FilePreviewProps {
  file: FileMetadata;
}

export default function FilePreview({ file: initialFile }: FilePreviewProps) {
  const [file, setFile] = useState(initialFile);
  const [fullscreen, setFullscreen] = useState(false);
  const [editOpen, setEditOpen] = useState(false);

  const cat = CATEGORIES.find((c) => c.name === file.category);

  return (
    <>
      {editOpen && (
        <EditFileModal
          file={file}
          onClose={() => setEditOpen(false)}
          onSave={(updated) => {
            setFile(updated);
            setEditOpen(false);
          }}
        />
      )}

      <div className={`flex flex-col h-full ${fullscreen ? "fixed inset-0 z-40 bg-[#080811]" : ""}`}>
        {/* Top bar */}
        <div className="flex items-center justify-between px-5 py-3 border-b border-[#1a1a2e] bg-[#0c0c18] flex-shrink-0">
          <div className="flex items-center gap-3">
            <Link
              href="/files"
              className="flex items-center gap-1.5 text-xs text-[#6e6e8a] hover:text-slate-300 transition-colors"
            >
              <ArrowLeft size={13} />
              Back
            </Link>

            <div className="w-px h-4 bg-[#1a1a2e]" />

            <span
              className={`inline-flex items-center gap-1 text-[11px] font-medium px-2 py-0.5 rounded-full border ${cat?.bgColor} ${cat?.color} ${cat?.borderColor}`}
            >
              {cat?.emoji} {file.category}
            </span>

            <h1 className="font-semibold text-sm text-slate-100 hidden sm:block">{file.title}</h1>
          </div>

          <div className="flex items-center gap-2">
            <button
              onClick={() => setEditOpen(true)}
              className="flex items-center gap-1.5 text-xs text-[#6e6e8a] hover:text-slate-300 bg-white/4 hover:bg-white/8 border border-[#1a1a2e] rounded-lg px-2.5 py-1.5 transition-all"
            >
              <Pencil size={12} />
              Edit
            </button>

            <a
              href={file.path}
              target="_blank"
              rel="noreferrer"
              className="flex items-center gap-1.5 text-xs text-[#6e6e8a] hover:text-slate-300 bg-white/4 hover:bg-white/8 border border-[#1a1a2e] rounded-lg px-2.5 py-1.5 transition-all"
            >
              <ExternalLink size={12} />
              Open
            </a>

            <button
              onClick={() => setFullscreen((v) => !v)}
              className="flex items-center gap-1.5 text-xs text-[#6e6e8a] hover:text-slate-300 bg-white/4 hover:bg-white/8 border border-[#1a1a2e] rounded-lg px-2.5 py-1.5 transition-all"
            >
              {fullscreen ? <Minimize2 size={12} /> : <Maximize2 size={12} />}
            </button>
          </div>
        </div>

        <div className={`flex flex-1 min-h-0 ${fullscreen ? "" : "flex-col lg:flex-row"}`}>
          {/* iFrame */}
          <div className="flex-1 min-h-0">
            <iframe
              src={file.path}
              className="w-full h-full border-none"
              title={file.title}
              sandbox="allow-scripts allow-same-origin allow-forms"
              style={{ minHeight: fullscreen ? "100vh" : "60vh" }}
            />
          </div>

          {/* Metadata panel */}
          {!fullscreen && (
            <div className="w-full lg:w-72 flex-shrink-0 border-t lg:border-t-0 lg:border-l border-[#1a1a2e] bg-[#0c0c18] overflow-y-auto">
              <div className="p-5 space-y-5">
                <div>
                  <h2 className="font-bold text-slate-100 text-base">{file.title}</h2>
                  <p className="text-[11px] text-[#6e6e8a] mt-1 font-mono">{file.id}</p>
                </div>

                <div className="space-y-3">
                  <div>
                    <div className="text-[10px] font-semibold text-[#6e6e8a] uppercase tracking-widest mb-1.5">
                      Status
                    </div>
                    <span
                      className={`text-[11px] font-semibold px-2 py-0.5 rounded-full border ${
                        file.status === "active"
                          ? "bg-emerald-500/10 text-emerald-400 border-emerald-500/20"
                          : file.status === "draft"
                          ? "bg-amber-500/10 text-amber-400 border-amber-500/20"
                          : "bg-slate-500/10 text-slate-400 border-slate-500/20"
                      }`}
                    >
                      {file.status}
                    </span>
                  </div>

                  <div>
                    <div className="text-[10px] font-semibold text-[#6e6e8a] uppercase tracking-widest mb-1.5 flex items-center gap-1">
                      <Tag size={10} /> Tags
                    </div>
                    <div className="flex flex-wrap gap-1.5">
                      {file.tags.map((tag) => (
                        <span
                          key={tag}
                          className="text-[11px] text-[#6e6e8a] bg-white/4 border border-[#1a1a2e] rounded-full px-2 py-0.5"
                        >
                          #{tag}
                        </span>
                      ))}
                    </div>
                  </div>

                  <div>
                    <div className="text-[10px] font-semibold text-[#6e6e8a] uppercase tracking-widest mb-1.5 flex items-center gap-1">
                      <Clock size={10} /> Timeline
                    </div>
                    <div className="text-[12px] text-[#6e6e8a] space-y-1">
                      <div>Created: {file.createdAt}</div>
                      <div>Updated: {file.updatedAt}</div>
                      <div>By: {file.createdBy}</div>
                    </div>
                  </div>

                  {file.notes && (
                    <div>
                      <div className="text-[10px] font-semibold text-[#6e6e8a] uppercase tracking-widest mb-1.5">
                        Notes
                      </div>
                      <p className="text-[12px] text-[#6e6e8a] leading-relaxed border-l-2 border-violet-500/30 pl-3">
                        {file.notes}
                      </p>
                    </div>
                  )}

                  <div>
                    <div className="text-[10px] font-semibold text-[#6e6e8a] uppercase tracking-widest mb-1.5">
                      File Path
                    </div>
                    <code className="text-[11px] text-violet-400 bg-violet-500/8 border border-violet-500/15 rounded px-2 py-1 block break-all font-mono">
                      {file.path}
                    </code>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
}
