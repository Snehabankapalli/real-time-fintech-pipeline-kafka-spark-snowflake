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

export async function GET() {
  try {
    const files = await readFiles();
    return NextResponse.json(files);
  } catch {
    return NextResponse.json({ error: "Failed to read files" }, { status: 500 });
  }
}

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const files = await readFiles();

    const newFile: FileMetadata = {
      id: body.id || body.title.toLowerCase().replace(/\s+/g, "-"),
      title: body.title,
      category: body.category,
      path: body.path,
      tags: body.tags || [],
      status: body.status || "active",
      createdBy: body.createdBy || "Claude",
      notes: body.notes || "",
      createdAt: new Date().toISOString().split("T")[0],
      updatedAt: new Date().toISOString().split("T")[0],
    };

    files.push(newFile);
    await writeFiles(files);

    return NextResponse.json(newFile, { status: 201 });
  } catch {
    return NextResponse.json({ error: "Failed to create file" }, { status: 500 });
  }
}
