import { NextRequest, NextResponse } from "next/server";
import { promises as fs } from "fs";
import path from "path";
import { Agent } from "@/types";

const DATA_PATH = path.join(process.cwd(), "data", "agents.json");

async function readAgents(): Promise<Agent[]> {
  const raw = await fs.readFile(DATA_PATH, "utf-8");
  return JSON.parse(raw);
}

async function writeAgents(agents: Agent[]): Promise<void> {
  await fs.writeFile(DATA_PATH, JSON.stringify(agents, null, 2), "utf-8");
}

export async function GET(_: NextRequest, { params }: { params: { id: string } }) {
  try {
    const agents = await readAgents();
    const agent = agents.find((a) => a.id === params.id);
    if (!agent) return NextResponse.json({ error: "Not found" }, { status: 404 });
    return NextResponse.json(agent);
  } catch {
    return NextResponse.json({ error: "Failed to read agent" }, { status: 500 });
  }
}

export async function PUT(req: NextRequest, { params }: { params: { id: string } }) {
  try {
    const body = await req.json();
    const agents = await readAgents();
    const idx = agents.findIndex((a) => a.id === params.id);

    if (idx === -1) return NextResponse.json({ error: "Not found" }, { status: 404 });

    agents[idx] = { ...agents[idx], ...body, id: params.id };
    await writeAgents(agents);

    return NextResponse.json(agents[idx]);
  } catch {
    return NextResponse.json({ error: "Failed to update agent" }, { status: 500 });
  }
}
