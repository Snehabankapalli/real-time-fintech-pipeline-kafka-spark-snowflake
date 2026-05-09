import { NextRequest, NextResponse } from "next/server";
import { promises as fs } from "fs";
import path from "path";
import { DataConnection } from "@/types";

const DATA_PATH = path.join(process.cwd(), "data", "connections.json");

async function readConnections(): Promise<DataConnection[]> {
  const raw = await fs.readFile(DATA_PATH, "utf-8");
  return JSON.parse(raw);
}

async function writeConnections(connections: DataConnection[]): Promise<void> {
  await fs.writeFile(DATA_PATH, JSON.stringify(connections, null, 2), "utf-8");
}

export async function GET() {
  try {
    const connections = await readConnections();
    return NextResponse.json(connections);
  } catch {
    return NextResponse.json({ error: "Failed to read connections" }, { status: 500 });
  }
}

export async function PUT(req: NextRequest) {
  try {
    const { id, connected } = await req.json();
    const connections = await readConnections();
    const idx = connections.findIndex((c) => c.id === id);

    if (idx === -1) return NextResponse.json({ error: "Not found" }, { status: 404 });

    connections[idx].connected = connected;
    await writeConnections(connections);

    return NextResponse.json(connections[idx]);
  } catch {
    return NextResponse.json({ error: "Failed to update connection" }, { status: 500 });
  }
}
