import { NextRequest, NextResponse } from "next/server";
import { promises as fs } from "fs";
import path from "path";
import { FileMetadata } from "@/types";

const DATA_PATH = path.join(process.cwd(), "data", "files.json");

async function readFiles(): Promise<FileMetadata[]> {
  const raw = await fs.readFile(DATA_PATH, "utf-8");
  return JSON.parse(raw);
}

async function writeFiles(files: FileMetadata[]): Promise<void> {
  await fs.writeFile(DATA_PATH, JSON.stringify(files, null, 2), "utf-8");
}

export async function GET(_: NextRequest, { params }: { params: { id: string } }) {
  try {
    const files = await readFiles();
    const file = files.find((f) => f.id === params.id);
    if (!file) return NextResponse.json({ error: "Not found" }, { status: 404 });
    return NextResponse.json(file);
  } catch {
    return NextResponse.json({ error: "Failed to read file" }, { status: 500 });
  }
}

export async function PUT(req: NextRequest, { params }: { params: { id: string } }) {
  try {
    const body = await req.json();
    const files = await readFiles();
    const idx = files.findIndex((f) => f.id === params.id);

    if (idx === -1) return NextResponse.json({ error: "Not found" }, { status: 404 });

    files[idx] = {
      ...files[idx],
      ...body,
      id: params.id,
      updatedAt: new Date().toISOString().split("T")[0],
    };

    await writeFiles(files);
    return NextResponse.json(files[idx]);
  } catch {
    return NextResponse.json({ error: "Failed to update file" }, { status: 500 });
  }
}

export async function DELETE(_: NextRequest, { params }: { params: { id: string } }) {
  try {
    const files = await readFiles();
    const filtered = files.filter((f) => f.id !== params.id);

    if (filtered.length === files.length) {
      return NextResponse.json({ error: "Not found" }, { status: 404 });
    }

    await writeFiles(filtered);
    return NextResponse.json({ success: true });
  } catch {
    return NextResponse.json({ error: "Failed to delete file" }, { status: 500 });
  }
}
