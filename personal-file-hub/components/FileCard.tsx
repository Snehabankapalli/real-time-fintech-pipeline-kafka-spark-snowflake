"use client";

import Link from "next/link";
import { FileMetadata, CATEGORIES } from "@/types";
import { ExternalLink, Pencil, Clock, User } from "lucide-react";

interface FileCardProps {
  file: FileMetadata;
  onEdit?: (file: FileMetadata) => void;
}

const statusConfig = {
  active: { label: "Active", classes: "bg-emerald-500/10 text-emerald-400 border-emerald-500/20" },
  draft: { label: "Draft", classes: "bg-amber-500/10 text-amber-400 border-amber-500/20" },
  archived: { label: "Archived", classes: "bg-slate-500/10 text-slate-400 border-slate-500/20" },
};

export default function FileCard({ file, onEdit }: FileCardProps) {
  const cat = CATEGORIES.find((c) => c.name === file.category);
  const status = statusConfig[file.status];

  return (
    <div className="group relative bg-[#111122] border border-[#1a1a2e] rounded-xl p-5 flex flex-col gap-4 hover:border-[#2d2d4e] hover:shadow-lg hover:shadow-violet-950/20 transition-all duration-200">
      {/* Header */}
      <div className="flex items-start justify-between gap-2">
        <div className="flex items-center gap-2 flex-wrap">
          <span
            className={`inline-flex items-center gap-1.5 text-xs font-medium px-2 py-0.5 rounded-full border ${cat?.bgColor} ${cat?.color} ${cat?.borderColor}`}
          >
            <span>{cat?.emoji}</span>
            {file.category}
          </span>
          <span className={`text-[10px] font-semibold px-2 py-0.5 rounded-full border ${status.classes}`}>
            {status.label}
          </span>
        </div>
      </div>

      {/* Title */}
      <div>
        <h3 className="font-semibold text-slate-100 text-base leading-tight mb-1.5 group-hover:text-white transition-colors">
          {file.title}
        </h3>

        {/* Tags */}
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

      {/* Notes */}
      {file.notes && (
        <p className="text-[12px] text-[#6e6e8a] leading-relaxed line-clamp-2 border-l-2 border-[#1a1a2e] pl-3">
          {file.notes}
        </p>
      )}

      {/* Footer */}
      <div className="flex items-center justify-between mt-auto">
        <div className="flex items-center gap-3 text-[11px] text-[#6e6e8a]">
          <span className="flex items-center gap-1">
            <User size={11} />
            {file.createdBy}
          </span>
          <span className="flex items-center gap-1">
            <Clock size={11} />
            {file.createdAt}
          </span>
        </div>

        <div className="flex items-center gap-2">
          {onEdit && (
            <button
              onClick={() => onEdit(file)}
              className="flex items-center gap-1 text-[11px] text-[#6e6e8a] hover:text-slate-300 bg-white/4 hover:bg-white/8 border border-[#1a1a2e] hover:border-[#2d2d4e] rounded-lg px-2.5 py-1.5 transition-all duration-150"
            >
              <Pencil size={11} />
              Edit
            </button>
          )}
          <Link
            href={`/files/${file.id}`}
            className="flex items-center gap-1 text-[11px] text-violet-400 hover:text-violet-300 bg-violet-500/10 hover:bg-violet-500/15 border border-violet-500/20 hover:border-violet-500/30 rounded-lg px-2.5 py-1.5 transition-all duration-150"
          >
            <ExternalLink size={11} />
            Preview
          </Link>
        </div>
      </div>
    </div>
  );
}
