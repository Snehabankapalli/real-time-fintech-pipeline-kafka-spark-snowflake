"use client";

import { useState, useEffect, Suspense } from "react";
import { useSearchParams, useRouter } from "next/navigation";
import FileCard from "@/components/FileCard";
import EditFileModal from "@/components/EditFileModal";
import { FileMetadata, CATEGORIES } from "@/types";
import { Search, SlidersHorizontal, Files, X } from "lucide-react";

function FilesContent() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const categoryParam = searchParams.get("category");
  const queryParam = searchParams.get("q") || "";

  const [files, setFiles] = useState<FileMetadata[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState(queryParam);
  const [selectedCategory, setSelectedCategory] = useState(categoryParam || "");
  const [statusFilter, setStatusFilter] = useState("");
  const [editingFile, setEditingFile] = useState<FileMetadata | null>(null);

  useEffect(() => {
    fetch("/api/files")
      .then((r) => r.json())
      .then((data) => {
        setFiles(data);
        setLoading(false);
      });
  }, []);

  useEffect(() => {
    setSelectedCategory(categoryParam || "");
  }, [categoryParam]);

  const filtered = files.filter((f) => {
    const matchCat = !selectedCategory || f.category === selectedCategory;
    const matchStatus = !statusFilter || f.status === statusFilter;
    const q = search.toLowerCase();
    const matchSearch =
      !q ||
      f.title.toLowerCase().includes(q) ||
      f.tags.some((t) => t.toLowerCase().includes(q)) ||
      f.notes.toLowerCase().includes(q);
    return matchCat && matchStatus && matchSearch;
  });

  const handleCategoryClick = (cat: string) => {
    const next = cat === selectedCategory ? "" : cat;
    setSelectedCategory(next);
    const params = new URLSearchParams(searchParams.toString());
    if (next) {
      params.set("category", next);
    } else {
      params.delete("category");
    }
    router.push(`/files?${params.toString()}`);
  };

  const clearFilters = () => {
    setSearch("");
    setSelectedCategory("");
    setStatusFilter("");
    router.push("/files");
  };

  const hasFilters = search || selectedCategory || statusFilter;

  return (
    <>
      {editingFile && (
        <EditFileModal
          file={editingFile}
          onClose={() => setEditingFile(null)}
          onSave={(updated) => {
            setFiles((prev) => prev.map((f) => (f.id === updated.id ? updated : f)));
            setEditingFile(null);
          }}
        />
      )}

      <div className="p-6 max-w-7xl mx-auto">
        {/* Header */}
        <div className="flex items-start justify-between mb-6 gap-4">
          <div>
            <h1 className="text-xl font-bold text-white flex items-center gap-2">
              <Files size={18} className="text-violet-400" />
              Vault Files
            </h1>
            <p className="text-[#6e6e8a] text-sm mt-0.5">
              {filtered.length} file{filtered.length !== 1 ? "s" : ""}
              {selectedCategory ? ` in ${selectedCategory}` : ""}
            </p>
          </div>
        </div>

        {/* Search + Filters */}
        <div className="flex flex-col sm:flex-row gap-3 mb-5">
          <div className="relative flex-1">
            <Search
              size={14}
              className="absolute left-3 top-1/2 -translate-y-1/2 text-[#6e6e8a]"
            />
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search files, tags, notes..."
              className="w-full bg-[#111122] border border-[#1a1a2e] rounded-xl pl-9 pr-4 py-2.5 text-sm text-slate-100 placeholder-[#6e6e8a] focus:outline-none focus:border-violet-500/40 transition-colors"
            />
          </div>

          <div className="flex items-center gap-2">
            <SlidersHorizontal size={13} className="text-[#6e6e8a] flex-shrink-0" />
            <select
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
              className="bg-[#111122] border border-[#1a1a2e] rounded-xl px-3 py-2.5 text-sm text-slate-100 focus:outline-none focus:border-violet-500/40 transition-colors"
            >
              <option value="">All statuses</option>
              <option value="active">Active</option>
              <option value="draft">Draft</option>
              <option value="archived">Archived</option>
            </select>

            {hasFilters && (
              <button
                onClick={clearFilters}
                className="flex items-center gap-1 text-[12px] text-[#6e6e8a] hover:text-slate-300 bg-white/4 border border-[#1a1a2e] rounded-lg px-3 py-2.5 transition-all"
              >
                <X size={12} /> Clear
              </button>
            )}
          </div>
        </div>

        {/* Category pills */}
        <div className="flex items-center gap-2 flex-wrap mb-6">
          <button
            onClick={() => handleCategoryClick("")}
            className={`text-[12px] font-medium px-3 py-1.5 rounded-full border transition-all ${
              !selectedCategory
                ? "bg-violet-500/15 text-violet-300 border-violet-500/25"
                : "text-[#6e6e8a] border-[#1a1a2e] hover:border-[#2d2d4e] hover:text-slate-300"
            }`}
          >
            All
          </button>
          {CATEGORIES.map((cat) => {
            const count = files.filter((f) => f.category === cat.name).length;
            if (count === 0) return null;
            return (
              <button
                key={cat.name}
                onClick={() => handleCategoryClick(cat.name)}
                className={`text-[12px] font-medium px-3 py-1.5 rounded-full border transition-all flex items-center gap-1.5 ${
                  selectedCategory === cat.name
                    ? `${cat.bgColor} ${cat.color} ${cat.borderColor}`
                    : "text-[#6e6e8a] border-[#1a1a2e] hover:border-[#2d2d4e] hover:text-slate-300"
                }`}
              >
                {cat.emoji} {cat.name}
                <span className="text-[10px] opacity-70">{count}</span>
              </button>
            );
          })}
        </div>

        {/* File grid */}
        {loading ? (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {[...Array(6)].map((_, i) => (
              <div
                key={i}
                className="bg-[#111122] border border-[#1a1a2e] rounded-xl p-5 h-52 animate-pulse"
              />
            ))}
          </div>
        ) : filtered.length === 0 ? (
          <div className="text-center py-20">
            <div className="text-4xl mb-3">📭</div>
            <h3 className="text-slate-300 font-semibold mb-1">No files found</h3>
            <p className="text-[#6e6e8a] text-sm">
              {hasFilters ? "Try adjusting your filters." : "Drop HTML files in /public/vault and register them in data/files.json"}
            </p>
          </div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {filtered.map((file) => (
              <FileCard key={file.id} file={file} onEdit={setEditingFile} />
            ))}
          </div>
        )}
      </div>
    </>
  );
}

export default function FilesPage() {
  return (
    <Suspense>
      <FilesContent />
    </Suspense>
  );
}
