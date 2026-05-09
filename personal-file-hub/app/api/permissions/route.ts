import { NextRequest, NextResponse } from "next/server";
import { promises as fs } from "fs";
import path from "path";
import { Permission } from "@/types";

const DATA_PATH = path.join(process.cwd(), "data", "permissions.json");

async function readPermissions(): Promise<Permission[]> {
  const raw = await fs.readFile(DATA_PATH, "utf-8");
  return JSON.parse(raw);
}

async function writePermissions(permissions: Permission[]): Promise<void> {
  await fs.writeFile(DATA_PATH, JSON.stringify(permissions, null, 2), "utf-8");
}

export async function GET() {
  try {
    const permissions = await readPermissions();
    return NextResponse.json(permissions);
  } catch {
    return NextResponse.json({ error: "Failed to read permissions" }, { status: 500 });
  }
}

export async function PUT(req: NextRequest) {
  try {
    const body = await req.json();
    const permissions = await readPermissions();
    const idx = permissions.findIndex((p) => p.id === body.id);

    if (idx === -1) return NextResponse.json({ error: "Not found" }, { status: 404 });

    permissions[idx] = { ...permissions[idx], ...body };
    await writePermissions(permissions);

    return NextResponse.json(permissions[idx]);
  } catch {
    return NextResponse.json({ error: "Failed to update permission" }, { status: 500 });
  }
}
