"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { CATEGORIES } from "@/types";
import { Upload, FileCode, CheckCircle2, X, Plus } from "lucide-react";

export default function UploadPage() {
  const router = useRouter();
  const [dragOver, setDragOver] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [preview, setPreview] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [tagInput, setTagInput] = useState("");

  const [form, setForm] = useState({
    title: "",
    category: "Experiments" as (typeof CATEGORIES)[number]["name"],
    tags: [] as string[],
    notes: "",
    status: "active",
  });

  const handleFile = (f: File) => {
    setFile(f);
    const nameWithoutExt = f.name.replace(/\.html?$/, "");
    setForm((prev) => ({
      ...prev,
      title: prev.title || nameWithoutExt.replace(/[-_]/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()),
    }));
    const reader = new FileReader();
    reader.onload = (e) => setPreview(e.target?.result as string);
    reader.readAsText(f);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const f = e.dataTransfer.files[0];
    if (f?.name.match(/\.html?$/)) handleFile(f);
  };

  const addTag = () => {
    const t = tagInput.trim();
    if (t && !form.tags.includes(t)) {
      setForm((prev) => ({ ...prev, tags: [...prev.tags, t] }));
      setTagInput("");
    }
  };

  const removeTag = (tag: string) => {
    setForm((prev) => ({ ...prev, tags: prev.tags.filter((t) => t !== tag) }));
  };

  const handleSave = async () => {
    if (!file || !form.title) return;
    setSaving(true);

    const slug = form.title.toLowerCase().replace(/\s+/g, "-").replace(/[^a-z0-9-]/g, "");
    const catSlug = form.category.toLowerCase();
    const fileName = file.name;

    const payload = {
      id: `${catSlug}-${slug}`,
      title: form.title,
      category: form.category,
      path: `/vault/${catSlug}/${fileName}`,
      tags: form.tags,
      status: form.status,
      createdBy: "Claude",
      notes: form.notes,
    };

    try {
      const res = await fetch("/api/files", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (res.ok) {
        setSaved(true);
        setTimeout(() => router.push("/files"), 1500);
      }
    } finally {
      setSaving(false);
    }
  };

  if (saved) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="text-center">
          <CheckCircle2 size={48} className="text-emerald-400 mx-auto mb-4" />
          <h2 className="text-xl font-bold text-white mb-2">File Registered!</h2>
          <p className="text-[#6e6e8a] text-sm">Redirecting to vault...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6 max-w-3xl mx-auto">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-xl font-bold text-white flex items-center gap-2">
          <Upload size={18} className="text-violet-400" />
          Add File to Vault
        </h1>
        <p className="text-[#6e6e8a] text-sm mt-0.5">
          Register an HTML file and its metadata into Claude Vault.
        </p>
      </div>

      {/* Note */}
      <div className="bg-violet-500/8 border border-violet-500/15 rounded-xl p-4 mb-6 text-[12px] text-[#6e6e8a] leading-relaxed">
        <strong className="text-violet-300">How it works:</strong> Fill in the metadata below and upload your HTML file to preview it. Then manually copy your .html file into{" "}
        <code className="text-violet-400 font-mono">public/vault/[category]/</code> — the metadata will be saved to{" "}
        <code className="text-violet-400 font-mono">data/files.json</code> automatically.
      </div>

      {/* Drop zone */}
      <div
        onDrop={handleDrop}
        onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
        onDragLeave={() => setDragOver(false)}
        onClick={() => document.getElementById("file-input")?.click()}
        className={`border-2 border-dashed rounded-2xl p-10 text-center cursor-pointer transition-all duration-200 mb-6 ${
          dragOver
            ? "border-violet-500/60 bg-violet-500/8"
            : file
            ? "border-emerald-500/40 bg-emerald-500/5"
            : "border-[#1a1a2e] hover:border-[#2d2d4e] hover:bg-white/2"
        }`}
      >
        <input
          id="file-input"
          type="file"
          accept=".html,.htm"
          className="hidden"
          onChange={(e) => e.target.files?.[0] && handleFile(e.target.files[0])}
        />

        {file ? (
          <div>
            <FileCode size={32} className="text-emerald-400 mx-auto mb-2" />
            <div className="font-semibold text-emerald-400">{file.name}</div>
            <div className="text-[12px] text-[#6e6e8a] mt-1">
              {(file.size / 1024).toFixed(1)} KB · Click to change
            </div>
          </div>
        ) : (
          <div>
            <Upload size={32} className="text-[#6e6e8a] mx-auto mb-2" />
            <div className="text-slate-300 font-medium">Drop your HTML file here</div>
            <div className="text-[12px] text-[#6e6e8a] mt-1">or click to browse · .html and .htm supported</div>
          </div>
        )}
      </div>

      {/* Metadata form */}
      <div className="bg-[#111122] border border-[#1a1a2e] rounded-2xl p-6 space-y-5">
        <h2 className="font-semibold text-slate-100">File Metadata</h2>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div>
            <label className="block text-[11px] font-semibold text-[#6e6e8a] uppercase tracking-wider mb-1.5">
              Title *
            </label>
            <input
              type="text"
              value={form.title}
              onChange={(e) => setForm({ ...form, title: e.target.value })}
              placeholder="My Workout Tracker"
              className="w-full bg-[#080811] border border-[#1a1a2e] rounded-lg px-3 py-2.5 text-sm text-slate-100 focus:outline-none focus:border-violet-500/50 transition-colors"
            />
          </div>

          <div>
            <label className="block text-[11px] font-semibold text-[#6e6e8a] uppercase tracking-wider mb-1.5">
              Category
            </label>
            <select
              value={form.category}
              onChange={(e) => setForm({ ...form, category: e.target.value as typeof form.category })}
              className="w-full bg-[#080811] border border-[#1a1a2e] rounded-lg px-3 py-2.5 text-sm text-slate-100 focus:outline-none focus:border-violet-500/50 transition-colors"
            >
              {CATEGORIES.map((cat) => (
                <option key={cat.name} value={cat.name}>
                  {cat.emoji} {cat.name}
                </option>
              ))}
            </select>
          </div>
        </div>

        <div>
          <label className="block text-[11px] font-semibold text-[#6e6e8a] uppercase tracking-wider mb-1.5">
            Status
          </label>
          <div className="flex gap-2">
            {["active", "draft", "archived"].map((s) => (
              <button
                key={s}
                onClick={() => setForm({ ...form, status: s })}
                className={`text-[12px] font-medium px-3 py-1.5 rounded-full border capitalize transition-all ${
                  form.status === s
                    ? s === "active"
                      ? "bg-emerald-500/15 text-emerald-400 border-emerald-500/25"
                      : s === "draft"
                      ? "bg-amber-500/15 text-amber-400 border-amber-500/25"
                      : "bg-slate-500/15 text-slate-400 border-slate-500/25"
                    : "text-[#6e6e8a] border-[#1a1a2e] hover:border-[#2d2d4e]"
                }`}
              >
                {s}
              </button>
            ))}
          </div>
        </div>

        <div>
          <label className="block text-[11px] font-semibold text-[#6e6e8a] uppercase tracking-wider mb-1.5">
            Tags
          </label>
          <div className="flex gap-2">
            <input
              type="text"
              value={tagInput}
              onChange={(e) => setTagInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && addTag()}
              placeholder="Add tag and press Enter..."
              className="flex-1 bg-[#080811] border border-[#1a1a2e] rounded-lg px-3 py-2 text-sm text-slate-100 focus:outline-none focus:border-violet-500/50 transition-colors"
            />
            <button
              onClick={addTag}
              className="bg-violet-600/15 hover:bg-violet-600/25 border border-violet-500/20 text-violet-400 rounded-lg px-3 py-2 transition-all"
            >
              <Plus size={14} />
            </button>
          </div>
          {form.tags.length > 0 && (
            <div className="flex flex-wrap gap-1.5 mt-2">
              {form.tags.map((tag) => (
                <span
                  key={tag}
                  className="flex items-center gap-1 text-[11px] text-[#6e6e8a] bg-white/4 border border-[#1a1a2e] rounded-full pl-2.5 pr-1.5 py-0.5"
                >
                  #{tag}
                  <button onClick={() => removeTag(tag)} className="hover:text-red-400 transition-colors">
                    <X size={10} />
                  </button>
                </span>
              ))}
            </div>
          )}
        </div>

        <div>
          <label className="block text-[11px] font-semibold text-[#6e6e8a] uppercase tracking-wider mb-1.5">
            Notes
          </label>
          <textarea
            value={form.notes}
            onChange={(e) => setForm({ ...form, notes: e.target.value })}
            rows={3}
            placeholder="Any notes, next steps, or things to improve..."
            className="w-full bg-[#080811] border border-[#1a1a2e] rounded-lg px-3 py-2.5 text-sm text-slate-100 focus:outline-none focus:border-violet-500/50 transition-colors resize-none"
          />
        </div>

        <button
          onClick={handleSave}
          disabled={!file || !form.title || saving}
          className="w-full bg-violet-600 hover:bg-violet-500 disabled:opacity-40 disabled:cursor-not-allowed text-white font-semibold py-3 rounded-xl flex items-center justify-center gap-2 transition-all duration-200"
        >
          <Upload size={15} />
          {saving ? "Saving..." : "Register File"}
        </button>
      </div>

      {/* Preview */}
      {preview && (
        <div className="mt-6">
          <h3 className="font-semibold text-slate-100 mb-3 text-sm">Preview</h3>
          <div className="rounded-2xl overflow-hidden border border-[#1a1a2e]" style={{ height: 480 }}>
            <iframe
              srcDoc={preview}
              className="w-full h-full border-none"
              sandbox="allow-scripts allow-same-origin"
              title="Preview"
            />
          </div>
        </div>
      )}
    </div>
  );
}
