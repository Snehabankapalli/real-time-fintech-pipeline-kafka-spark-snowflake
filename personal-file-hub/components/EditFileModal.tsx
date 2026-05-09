"use client";

import { useState } from "react";
import { FileMetadata, CATEGORIES } from "@/types";
import { X, Save } from "lucide-react";

interface EditFileModalProps {
  file: FileMetadata;
  onClose: () => void;
  onSave: (updated: FileMetadata) => void;
}

export default function EditFileModal({ file, onClose, onSave }: EditFileModalProps) {
  const [form, setForm] = useState({
    title: file.title,
    category: file.category,
    tags: file.tags.join(", "),
    status: file.status,
    notes: file.notes,
  });
  const [saving, setSaving] = useState(false);

  const handleSave = async () => {
    setSaving(true);
    try {
      const payload = {
        ...form,
        tags: form.tags
          .split(",")
          .map((t) => t.trim())
          .filter(Boolean),
      };

      const res = await fetch(`/api/files/${file.id}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (res.ok) {
        const updated = await res.json();
        onSave(updated);
      }
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />

      <div className="relative bg-[#0f0f1e] border border-[#1a1a2e] rounded-2xl p-6 w-full max-w-md shadow-2xl shadow-black/50 animate-slide-up">
        {/* Header */}
        <div className="flex items-center justify-between mb-5">
          <div>
            <h2 className="font-bold text-slate-100">Edit Metadata</h2>
            <p className="text-[12px] text-[#6e6e8a] mt-0.5">{file.id}</p>
          </div>
          <button
            onClick={onClose}
            className="text-[#6e6e8a] hover:text-slate-300 bg-white/4 hover:bg-white/8 rounded-lg p-1.5 transition-all"
          >
            <X size={16} />
          </button>
        </div>

        {/* Fields */}
        <div className="space-y-4">
          <div>
            <label className="block text-[11px] font-semibold text-[#6e6e8a] uppercase tracking-wider mb-1.5">
              Title
            </label>
            <input
              type="text"
              value={form.title}
              onChange={(e) => setForm({ ...form, title: e.target.value })}
              className="w-full bg-[#080811] border border-[#1a1a2e] rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-violet-500/50 transition-colors"
            />
          </div>

          <div>
            <label className="block text-[11px] font-semibold text-[#6e6e8a] uppercase tracking-wider mb-1.5">
              Category
            </label>
            <select
              value={form.category}
              onChange={(e) => setForm({ ...form, category: e.target.value as FileMetadata["category"] })}
              className="w-full bg-[#080811] border border-[#1a1a2e] rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-violet-500/50 transition-colors"
            >
              {CATEGORIES.map((cat) => (
                <option key={cat.name} value={cat.name}>
                  {cat.emoji} {cat.name}
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="block text-[11px] font-semibold text-[#6e6e8a] uppercase tracking-wider mb-1.5">
              Status
            </label>
            <select
              value={form.status}
              onChange={(e) => setForm({ ...form, status: e.target.value as FileMetadata["status"] })}
              className="w-full bg-[#080811] border border-[#1a1a2e] rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-violet-500/50 transition-colors"
            >
              <option value="active">Active</option>
              <option value="draft">Draft</option>
              <option value="archived">Archived</option>
            </select>
          </div>

          <div>
            <label className="block text-[11px] font-semibold text-[#6e6e8a] uppercase tracking-wider mb-1.5">
              Tags{" "}
              <span className="text-[#6e6e8a] normal-case font-normal">(comma separated)</span>
            </label>
            <input
              type="text"
              value={form.tags}
              onChange={(e) => setForm({ ...form, tags: e.target.value })}
              placeholder="workout, health, strength"
              className="w-full bg-[#080811] border border-[#1a1a2e] rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-violet-500/50 transition-colors"
            />
          </div>

          <div>
            <label className="block text-[11px] font-semibold text-[#6e6e8a] uppercase tracking-wider mb-1.5">
              Notes
            </label>
            <textarea
              value={form.notes}
              onChange={(e) => setForm({ ...form, notes: e.target.value })}
              rows={3}
              placeholder="Any notes or next steps..."
              className="w-full bg-[#080811] border border-[#1a1a2e] rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-violet-500/50 transition-colors resize-none"
            />
          </div>
        </div>

        {/* Actions */}
        <div className="flex gap-3 mt-6">
          <button
            onClick={onClose}
            className="flex-1 py-2 rounded-lg border border-[#1a1a2e] text-sm text-[#6e6e8a] hover:text-slate-300 hover:border-[#2d2d4e] transition-all"
          >
            Cancel
          </button>
          <button
            onClick={handleSave}
            disabled={saving}
            className="flex-1 py-2 rounded-lg bg-violet-600 hover:bg-violet-500 text-white text-sm font-semibold flex items-center justify-center gap-2 transition-all disabled:opacity-60"
          >
            <Save size={14} />
            {saving ? "Saving..." : "Save Changes"}
          </button>
        </div>
      </div>
    </div>
  );
}
