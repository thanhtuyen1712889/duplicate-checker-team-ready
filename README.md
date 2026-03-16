# Duplicate Checker + SEO Brain

This workspace contains a lightweight Python app for checking duplicate
content across product-detail pages that share a known template.

It also now includes `SEO Brain`, a local dashboard for importing a manual SEO
master plan and converting it into a dependency-aware execution system.

## What it does

- accepts `.docx` and `.txt` inputs
- accepts `Google Docs URL` inputs when the document can be fetched
- auto-detects the PandaPak-style product-detail template
- allows manual template selection when auto-detect is not enough
- stores custom templates created from sample files
- splits content into sections
- ignores approved boilerplate sections such as supplier content
- gives lower weight to fact-heavy/table sections
- checks higher-risk sections such as features, use cases, FAQ answers, and conclusion
- stores the latest version of each document key in a local SQLite corpus
- serves a web UI with:
  - dashboard for content users
  - filterable corpus list
  - CSV export
  - template management page for admins
  - green/yellow/red results and top findings

## SEO Brain: what it does

- imports `.xlsx` SEO planning workbooks
- reads `Content Calendar` and `SEO IMAGE AUDIT`
- creates URL-level task trees for content, onpage, publish, indexation, internal links, and offpage
- calculates `planned`, `forecast`, and `actual` timing
- lets you edit KPI targets, page status, task owner, actual deadlines, and done dates inline
- recalculates warnings after every change
- stores everything in local SQLite at `data/seo_brain.sqlite3`

## Run locally

```bash
python3 app.py serve
```

Open `http://127.0.0.1:8765`.

## Run SEO Brain locally

```bash
python3 app.py seo-serve
```

Open `http://127.0.0.1:8876`.

On macOS, you can also double-click:

```text
start_seo_brain.command
```

On macOS, you can also double-click:

```text
start_duplicate_checker.command
```

This starts the local server and opens the browser automatically.

## Run for the team on one machine

If one machine will host the tool for the whole team on the same local network:

```text
start_team_server.command
```

This starts the server on `0.0.0.0` and prints the local network URL.

## Deploy to Render + Supabase

This repo now includes:

- `requirements.txt`
- `render.yaml`
- `.env.example`

Recommended production-lite setup:

1. Create a free Supabase project.
2. In Supabase, copy the Postgres connection string.
3. Prefer the pooled connection string for app traffic.
4. Make sure the connection string includes `sslmode=require`.
5. Create a Render web service from this repo.
6. Use the included `render.yaml` or set the same values manually.
7. Add these environment variables in Render:
   - `DATABASE_URL`
   - `APP_USERNAME`
   - `APP_PASSWORD`
   - optional `OPENAI_API_KEY`
8. Deploy.
9. Open the public URL and log in with the basic-auth username/password.

When `DATABASE_URL` is set:

- documents are stored in Postgres instead of local SQLite
- custom templates created in the UI are also stored in Postgres
- redeploys and restarts will keep the data

## Files for deployment

- `render.yaml`: Render service definition
- `.env.example`: sample environment variables
- `requirements.txt`: Python package list for hosted mode

## Import sample files into the corpus

```bash
python3 app.py import "/absolute/path/to/file1.docx" "/absolute/path/to/file2.docx"
```

## Import an SEO workbook

```bash
python3 app.py seo-import "/absolute/path/to/workbook.xlsx" --name "Pandapak SEO Brain"
```

## Template workflow

1. Open the `Templates` page.
2. Create a new template from 1-3 sample files.
3. Pick the closest built-in strategy.
4. Save the template.
5. If you upload 2-3 samples, the template can be used for auto-detect more reliably.
6. Content users can still choose that template manually from the dashboard at any time.

## Recommended content workflow

1. Writer uploads a DOCX, pastes text, or enters a Google Docs URL.
2. Writer keeps the same `Ma bai` when resubmitting revisions of the same page.
3. Tool returns `green`, `yellow`, or `red`.
4. Writer opens the result page, reviews the highlighted sections, edits, and resubmits.

## Notes

- Duplicate Checker local mode stays lightweight, but SEO Brain needs `openpyxl` for Excel imports.
- Hosted mode adds `psycopg` from `requirements.txt` so the app can talk to Postgres.
- If `OPENAI_API_KEY` is present, the scoring engine will try OpenAI embeddings for better semantic similarity and fall back automatically if the request fails.
- Without an API key, semantic scoring uses a local heuristic based on token cosine and sentence-level overlap.
- Google Docs fetching depends on the document being accessible from the machine running the app.
- The hosted app supports HTTP Basic Auth through `APP_USERNAME` and `APP_PASSWORD`.
- The local team-server mode is still intended for trusted internal networks only.
