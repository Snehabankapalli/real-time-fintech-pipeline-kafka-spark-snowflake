# Claude Vault — Personal AI Command Center

A beautiful, local-first personal dashboard to store, organize, preview, and manage all your Claude-created HTML files and AI agents.

## Tech Stack

- **Next.js 14** + **TypeScript** — App Router
- **Tailwind CSS** — dark-mode, premium UI
- **Local JSON files** — `data/files.json`, `data/agents.json`, `data/connections.json`, `data/permissions.json`
- **iframe preview** — renders HTML files directly in the vault
- **REST API routes** — `app/api/` handles all mutations

---

## Getting Started

```bash
cd personal-file-hub
npm install
npm run dev
# Open http://localhost:3000
```

---

## How to Add New HTML Files

### Step 1 — Drop the file into the vault

Copy your HTML file into the correct category folder:

```
public/
  vault/
    fitness/      ← your-file.html
    career/
    finance/
    skincare/
    agents/
    home/
    portfolio/
    experiments/
```

### Step 2 — Register metadata in `data/files.json`

Add an entry to the JSON array:

```json
{
  "id": "fitness-morning-stretch",
  "title": "Morning Stretch Routine",
  "category": "Fitness",
  "path": "/vault/fitness/morning-stretch.html",
  "tags": ["stretch", "mobility", "morning"],
  "status": "active",
  "createdBy": "Claude",
  "notes": "Works well on mobile too",
  "createdAt": "2024-03-20",
  "updatedAt": "2024-03-20"
}
```

**Field reference:**

| Field | Values | Notes |
|-------|--------|-------|
| `id` | Unique slug | Use `category-title` format |
| `category` | `Fitness`, `Career`, `Finance`, `Skincare`, `Agents`, `Home`, `Portfolio`, `Experiments` | |
| `status` | `active`, `draft`, `archived` | |
| `path` | `/vault/[category]/[filename].html` | Must match actual file location |
| `createdBy` | `Claude` | or your name |

### Step 3 — Restart dev server (optional)

The layout reads `files.json` at build time for the sidebar counts. For dev, a quick page reload is enough.

---

## Project Structure

```
personal-file-hub/
├── app/
│   ├── layout.tsx            # Root layout with sidebar
│   ├── page.tsx              # Dashboard
│   ├── files/
│   │   ├── page.tsx          # All files + search/filter
│   │   └── [id]/page.tsx     # File preview + metadata
│   ├── upload/page.tsx       # Register new files via UI
│   ├── agents/
│   │   ├── page.tsx          # Agent hub
│   │   └── [id]/page.tsx     # Agent detail + permissions
│   ├── connections/page.tsx   # Data sources + permission matrix
│   └── api/
│       ├── files/            # GET, POST; [id]: GET, PUT, DELETE
│       ├── agents/           # GET; [id]: GET, PUT
│       ├── connections/      # GET, PUT
│       └── permissions/      # GET, PUT
├── components/
│   ├── Sidebar.tsx           # Navigation + category list
│   ├── FileCard.tsx          # File grid card
│   ├── FilePreview.tsx       # iframe preview page
│   ├── AgentCard.tsx         # Agent hub card
│   ├── ConnectionCard.tsx    # Connection toggle card
│   └── EditFileModal.tsx     # Metadata edit modal
├── data/
│   ├── files.json            # All file metadata
│   ├── agents.json           # Agent configs
│   ├── connections.json      # Data source connections
│   └── permissions.json      # Agent ↔ source permissions
├── public/
│   └── vault/                # Your HTML files live here
│       ├── fitness/
│       ├── career/
│       ├── finance/
│       ├── skincare/
│       ├── agents/
│       ├── home/
│       ├── portfolio/
│       └── experiments/
└── types/
    └── index.ts              # All TypeScript types + CATEGORIES config
```

---

## Features

### Vault Files
- Grid view with category/status/tag filtering
- Instant search across title, tags, and notes
- iframe preview with fullscreen mode
- Edit metadata inline (modal)
- Status management (active / draft / archived)

### Agent Hub
- 9 pre-configured personal AI agents
- Per-agent detail pages with purpose, actions, data sources
- Activity log per agent
- Approval toggle (agents pause before write actions)
- Notes / memory per agent

### Data Connections
- 11 data sources (Gmail, Calendar, GitHub, Apple Health, bank CSV, etc.)
- Connect / disconnect toggle
- Permission matrix: per agent, per source — Read / Write / Requires Approval
- All stored in local JSON — no cloud sync

### Permission Layer (Key Design Rule)
> **Agents access your data through approved connectors, not unrestricted device/phone access.**
- Every agent lists its allowed data sources explicitly
- Write actions by high-risk agents require manual approval
- High-risk agent: `relationship-reflection-agent` — local notes only, no cloud sync ever

---

## Upgrading to a Database

When you're ready to move from JSON files to SQLite or Supabase:

1. Replace `fs.readFile/writeFile` calls in `app/api/*/route.ts` with DB queries
2. Run a one-time migration seeding your existing `data/*.json` files
3. Everything else (UI, components, types) stays the same

---

## Adding New Agents

Edit `data/agents.json` and follow the schema:

```json
{
  "id": "travel-planner",
  "name": "Travel Planner Agent",
  "icon": "✈️",
  "category": "Lifestyle",
  "purpose": "Plans trips, tracks bookings, suggests itineraries.",
  "allowedDataSources": ["google-calendar", "local-notes"],
  "actions": ["plan-trip", "track-bookings", "suggest-itinerary"],
  "riskLevel": "low",
  "requiresApproval": false,
  "status": "idle",
  "activityLog": [],
  "notes": "",
  "createdAt": "2024-03-20"
}
```

Then add permissions in `data/permissions.json` for each data source the agent needs.
