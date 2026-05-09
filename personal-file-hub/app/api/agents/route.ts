import { NextResponse } from "next/server";
import { promises as fs } from "fs";
import path from "path";
import { Agent } from "@/types";

const DATA_PATH = path.join(process.cwd(), "data", "agents.json");

async function readAgents(): Promise<Agent[]> {
  const raw = await fs.readFile(DATA_PATH, "utf-8");
  return JSON.parse(raw);
}

export async function GET() {
  try {
    const agents = await readAgents();
    return NextResponse.json(agents);
  } catch {
    return NextResponse.json({ error: "Failed to read agents" }, { status: 500 });
  }
}
