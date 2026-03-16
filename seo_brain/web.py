"""HTTP UI for the SEO Brain app."""

from __future__ import annotations

import html
import json
import os
from datetime import date, timedelta
from email.parser import BytesParser
from email.policy import default
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

from .service import DEFAULT_TEAM_CAPACITY, SeoBrainService, format_date, now_iso, safe_slug


def escape(value: Any) -> str:
    return html.escape(str(value))


class SeoBrainApp:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.service = SeoBrainService(base_dir)

    def make_handler(self) -> type[BaseHTTPRequestHandler]:
        app = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                parsed = urlparse(self.path)
                if parsed.path == "/healthz":
                    self.respond_text("ok")
                    return
                if parsed.path == "/":
                    self.respond_html(app.render_dashboard(parsed.query))
                    return
                self.send_error(HTTPStatus.NOT_FOUND)

            def do_POST(self) -> None:
                if self.path == "/import":
                    self.handle_import()
                    return
                if self.path == "/api/task":
                    self.handle_task_update()
                    return
                if self.path == "/api/page":
                    self.handle_page_update()
                    return
                if self.path == "/api/kpi":
                    self.handle_kpi_update()
                    return
                if self.path == "/api/settings":
                    self.handle_settings_update()
                    return
                self.send_error(HTTPStatus.NOT_FOUND)

            def handle_import(self) -> None:
                fields, files = self.parse_form()
                source_name = ""
                workbook_bytes = b""
                if "upload" in files:
                    source_name = str(files["upload"]["filename"])
                    workbook_bytes = bytes(files["upload"]["content"])
                else:
                    local_path = (fields.get("local_path") or "").strip()
                    if local_path:
                        path = Path(local_path).expanduser().resolve()
                        if path.exists():
                            source_name = path.name
                            workbook_bytes = path.read_bytes()
                if not workbook_bytes:
                    self.respond_html(app.render_dashboard("", error="Hay upload file Excel hoac nhap local path hop le."), status=400)
                    return
                project_name = (fields.get("project_name") or "").strip()
                try:
                    result = app.service.import_workbook(source_name, workbook_bytes, project_name=project_name)
                except Exception as exc:  # noqa: BLE001
                    self.respond_html(app.render_dashboard("", error=f"Import that bai: {exc}"), status=500)
                    return
                notice = urlencode({"project": result["project_id"], "notice": f"Da import {result['project_name']}."})
                self.redirect(f"/?{notice}")

            def handle_task_update(self) -> None:
                payload = self.parse_json()
                try:
                    record = app.service.update_task_field(
                        int(payload.get("task_id", 0)),
                        str(payload.get("field", "")),
                        str(payload.get("value", "")),
                    )
                except Exception as exc:  # noqa: BLE001
                    self.respond_json({"ok": False, "error": str(exc)}, status=400)
                    return
                self.respond_json({"ok": True, "record": record})

            def handle_page_update(self) -> None:
                payload = self.parse_json()
                try:
                    record = app.service.update_page_field(
                        int(payload.get("page_id", 0)),
                        str(payload.get("field", "")),
                        str(payload.get("value", "")),
                    )
                except Exception as exc:  # noqa: BLE001
                    self.respond_json({"ok": False, "error": str(exc)}, status=400)
                    return
                self.respond_json({"ok": True, "record": record})

            def handle_kpi_update(self) -> None:
                payload = self.parse_json()
                try:
                    project_id = int(payload.get("project_id", 0))
                    month_key = str(payload.get("month_key", "")).strip()
                    metric_name = str(payload.get("metric_name", "")).strip()
                    if not month_key or not metric_name:
                        raise ValueError("Month va metric name la bat buoc.")
                    metric_key = str(payload.get("metric_key") or safe_slug(metric_name))
                    target_raw = payload.get("target_value")
                    target_value = float(target_raw) if target_raw not in (None, "") else None
                    actual_raw = payload.get("actual_override")
                    actual_override = float(actual_raw) if actual_raw not in (None, "") else None
                    app.service.upsert_kpi(
                        project_id=project_id,
                        month_key=month_key,
                        metric_key=metric_key,
                        metric_name=metric_name,
                        unit=str(payload.get("unit") or "value"),
                        target_value=target_value,
                        actual_override=actual_override,
                        linked_workstream=str(payload.get("linked_workstream") or ""),
                        notes=str(payload.get("notes") or ""),
                    )
                except Exception as exc:  # noqa: BLE001
                    self.respond_json({"ok": False, "error": str(exc)}, status=400)
                    return
                self.respond_json({"ok": True})

            def handle_settings_update(self) -> None:
                payload = self.parse_json()
                try:
                    project_id = int(payload.get("project_id", 0))
                    capacities = {
                        role: float(payload.get(role, DEFAULT_TEAM_CAPACITY[role]) or DEFAULT_TEAM_CAPACITY[role])
                        for role in DEFAULT_TEAM_CAPACITY
                    }
                    kickoff_date = str(payload.get("kickoff_date") or "")
                    app.service.update_team_capacity(project_id, capacities, kickoff_date=kickoff_date)
                except Exception as exc:  # noqa: BLE001
                    self.respond_json({"ok": False, "error": str(exc)}, status=400)
                    return
                self.respond_json({"ok": True})

            def parse_form(self) -> tuple[dict[str, str], dict[str, dict[str, bytes | str]]]:
                content_type = self.headers.get("Content-Type", "")
                content_length = int(self.headers.get("Content-Length", "0") or "0")
                payload = self.rfile.read(content_length)
                if content_type.startswith("multipart/form-data"):
                    message = BytesParser(policy=default).parsebytes(
                        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("utf-8") + payload
                    )
                    fields: dict[str, str] = {}
                    files: dict[str, dict[str, bytes | str]] = {}
                    for part in message.iter_parts():
                        name = part.get_param("name", header="content-disposition")
                        filename = part.get_filename()
                        body = part.get_payload(decode=True) or b""
                        if not name:
                            continue
                        if filename:
                            files[name] = {"filename": filename, "content": body}
                        else:
                            charset = part.get_content_charset() or "utf-8"
                            fields[name] = body.decode(charset, errors="replace")
                    return fields, files
                data = parse_qs(payload.decode("utf-8", errors="replace"))
                return {key: values[0] for key, values in data.items()}, {}

            def parse_json(self) -> dict[str, Any]:
                content_length = int(self.headers.get("Content-Length", "0") or "0")
                payload = self.rfile.read(content_length)
                if not payload:
                    return {}
                return json.loads(payload.decode("utf-8"))

            def respond_html(self, content: str, status: int = 200) -> None:
                encoded = content.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def respond_json(self, payload: dict[str, Any], status: int = 200) -> None:
                encoded = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def respond_text(self, payload: str, status: int = 200) -> None:
                encoded = payload.encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def redirect(self, location: str) -> None:
                self.send_response(302)
                self.send_header("Location", location)
                self.end_headers()

        return Handler

    def create_server(self, host: str, port: int) -> ThreadingHTTPServer:
        return ThreadingHTTPServer((host, port), self.make_handler())

    def render_dashboard(self, query_string: str, *, error: str = "") -> str:
        params = parse_qs(query_string)
        notice = (params.get("notice", [""])[0] or "").strip()
        project_param = (params.get("project", [""])[0] or "").strip()
        month_key = (params.get("month", [""])[0] or "").strip()
        workstream = (params.get("workstream", [""])[0] or "").strip()
        warning = (params.get("warning", [""])[0] or "").strip()
        page_query = (params.get("q", [""])[0] or "").strip()
        priority = (params.get("priority", [""])[0] or "").strip()
        content_status = (params.get("status", [""])[0] or "").strip()
        projects = self.service.list_projects()
        selected_project_id = int(project_param or (projects[0]["id"] if projects else 0))
        project = self.service.get_project(selected_project_id) if selected_project_id else None
        if not project:
            return self.render_shell(
                "SEO Brain",
                self.render_empty_state(error=error or notice),
            )

        summary = self.service.get_project_summary(selected_project_id)
        settings = self.service.get_project_settings(selected_project_id)
        pages = self.service.list_pages(selected_project_id, priority=priority, status=content_status, query=page_query)
        tasks = self.service.list_tasks(selected_project_id, month_key=month_key, workstream=workstream, warning=warning)
        kpi_board = self.service.monthly_kpi_board(selected_project_id)
        week_start = date.today() - timedelta(days=date.today().weekday())
        weekly_plan = self.service.weekly_plan(selected_project_id, week_start=week_start)
        body = f"""
        <section class="hero">
          <div>
            <p class="eyebrow">SEO Operating System</p>
            <h1>{escape(project['name'])}</h1>
            <p class="lede">Import workbook, auto-split theo page workflow, canh dependency content -> onpage -> publish -> index -> offpage, va canh bao impact neu deadline thuc te bi tre.</p>
          </div>
          <div class="hero-panel">
            <form method="get" class="project-switch">
              <label>Project</label>
              <select name="project" onchange="this.form.submit()">
                {self.render_project_options(projects, selected_project_id)}
              </select>
            </form>
            <p><strong>Imported:</strong> {escape(project['last_imported_at'])}</p>
            <p><strong>Source:</strong> {escape(project['source_name'])}</p>
          </div>
        </section>
        {self.render_flash(error=error, notice=notice)}
        <section class="summary-grid">
          <article class="stat-card">
            <strong>{summary['total_pages']}</strong>
            <span>Total pages tracked</span>
          </article>
          <article class="stat-card">
            <strong>{summary['published_pages']}</strong>
            <span>Published pages</span>
          </article>
          <article class="stat-card">
            <strong>{summary['red_tasks']}</strong>
            <span>Red warnings</span>
          </article>
          <article class="stat-card">
            <strong>{summary['average_health']}</strong>
            <span>Avg page health</span>
          </article>
        </section>
        <section class="panel panel-kpi" id="kpi">
          <div class="panel-head">
            <div>
              <p class="eyebrow">1. KPI Board</p>
              <h2>Luong hoa muc tieu tung thang</h2>
            </div>
            <p class="muted">Click metric de nhay xuong danh sach task cua month/workstream do.</p>
          </div>
          <div class="month-grid">
            {''.join(self.render_month_card(selected_project_id, month) for month in kpi_board)}
          </div>
          <form id="kpi-form" class="inline-form">
            <input type="hidden" name="project_id" value="{selected_project_id}" />
            <label>Month
              <input type="month" name="month_key" required />
            </label>
            <label>KPI name
              <input type="text" name="metric_name" placeholder="Ranking top 10 / Organic clicks / ..." required />
            </label>
            <label>Target
              <input type="number" step="0.1" name="target_value" />
            </label>
            <label>Unit
              <input type="text" name="unit" value="value" />
            </label>
            <label>Linked workstream
              <select name="linked_workstream">
                {self.render_workstream_options("")}
              </select>
            </label>
            <button type="submit">Add KPI</button>
          </form>
        </section>
        <section class="panel">
          <div class="panel-head">
            <div>
              <p class="eyebrow">2. Weekly Board</p>
              <h2>Tuan nay ai lam gi</h2>
            </div>
            <p class="muted">Week of {week_start.strftime('%d/%m/%Y')}</p>
          </div>
          <div class="kanban-grid">
            {''.join(self.render_week_lane(owner, items) for owner, items in weekly_plan.items()) or '<div class="empty">Khong co task trong tuan nay.</div>'}
          </div>
        </section>
        <section class="panel" id="pages">
          <div class="panel-head">
            <div>
              <p class="eyebrow">3. URL Tracker</p>
              <h2>Tracking tung URL va deadline page-level</h2>
            </div>
          </div>
          <form method="get" class="filter-bar">
            <input type="hidden" name="project" value="{selected_project_id}" />
            <label>Search
              <input type="text" name="q" value="{escape(page_query)}" placeholder="keyword / URL / title" />
            </label>
            <label>Priority
              <select name="priority">
                <option value="">All</option>
                {self.render_priority_options(priority)}
              </select>
            </label>
            <label>Status
              <select name="status">
                <option value="">All</option>
                {self.render_status_options(content_status)}
              </select>
            </label>
            <button type="submit">Filter</button>
          </form>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Page</th>
                  <th>Priority</th>
                  <th>Content</th>
                  <th>Publish target</th>
                  <th>Index</th>
                  <th>PIC</th>
                  <th>Health</th>
                </tr>
              </thead>
              <tbody>
                {''.join(self.render_page_row(page) for page in pages) or '<tr><td colspan="7" class="empty-cell">Khong co page phu hop bo loc.</td></tr>'}
              </tbody>
            </table>
          </div>
        </section>
        <section class="panel" id="tasks">
          <div class="panel-head">
            <div>
              <p class="eyebrow">4. Task Engine</p>
              <h2>Task con, dependency, warning va actual deadline</h2>
            </div>
          </div>
          <form method="get" class="filter-bar">
            <input type="hidden" name="project" value="{selected_project_id}" />
            <label>Month
              <select name="month">
                <option value="">All</option>
                {self.render_month_options(kpi_board, month_key)}
              </select>
            </label>
            <label>Workstream
              <select name="workstream">
                <option value="">All</option>
                {self.render_workstream_options(workstream)}
              </select>
            </label>
            <label>Warning
              <select name="warning">
                <option value="">All</option>
                {self.render_warning_options(warning)}
              </select>
            </label>
            <button type="submit">Filter</button>
          </form>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Page</th>
                  <th>Owner</th>
                  <th>Planned</th>
                  <th>Forecast</th>
                  <th>Actual due</th>
                  <th>Done</th>
                  <th>Status</th>
                  <th>Warning</th>
                  <th>Impact</th>
                </tr>
              </thead>
              <tbody>
                {''.join(self.render_task_row(task) for task in tasks) or '<tr><td colspan="10" class="empty-cell">Khong co task phu hop bo loc.</td></tr>'}
              </tbody>
            </table>
          </div>
        </section>
        <section class="settings-grid">
          <section class="panel">
            <div class="panel-head">
              <div>
                <p class="eyebrow">5. Import</p>
                <h2>Nhap workbook de tao roadmap moi</h2>
              </div>
            </div>
            <form method="post" action="/import" enctype="multipart/form-data" class="stack-form">
              <label>Project name
                <input type="text" name="project_name" placeholder="Pandapak SEO Brain" />
              </label>
              <label>Excel workbook
                <input type="file" name="upload" accept=".xlsx,.xlsm" />
              </label>
              <label>Or local path
                <input type="text" name="local_path" value="/Users/bssgroup/Downloads/Copy of [INTERNAL] MASTER PLAN SEO -  PANDAPAK X DW.xlsx" />
              </label>
              <button type="submit">Import workbook</button>
            </form>
          </section>
          <section class="panel">
            <div class="panel-head">
              <div>
                <p class="eyebrow">6. Team Capacity</p>
                <h2>Canh effort theo suc chua team</h2>
              </div>
            </div>
            <form id="capacity-form" class="stack-form">
              <input type="hidden" name="project_id" value="{selected_project_id}" />
              <label>Kickoff date
                <input type="date" name="kickoff_date" value="{escape(settings.get('kickoff_date', ''))}" />
              </label>
              {''.join(self.render_capacity_input(role, settings['team_capacity'].get(role, value)) for role, value in DEFAULT_TEAM_CAPACITY.items())}
              <button type="submit">Rebalance schedule</button>
            </form>
          </section>
        </section>
        """
        return self.render_shell(project["name"], body)

    def render_empty_state(self, *, error: str = "") -> str:
        return f"""
        <section class="hero empty-hero">
          <div>
            <p class="eyebrow">SEO Operating System</p>
            <h1>SEO Brain local tool</h1>
            <p class="lede">Import file Excel, bien roadmap manual thanh dashboard co task tree, dependency, warning impact, va tracking page-level.</p>
          </div>
        </section>
        {self.render_flash(error=error, notice='')}
        <section class="panel">
          <form method="post" action="/import" enctype="multipart/form-data" class="stack-form">
            <label>Project name
              <input type="text" name="project_name" placeholder="Pandapak SEO Brain" />
            </label>
            <label>Excel workbook
              <input type="file" name="upload" accept=".xlsx,.xlsm" />
            </label>
            <label>Or local path
              <input type="text" name="local_path" value="/Users/bssgroup/Downloads/Copy of [INTERNAL] MASTER PLAN SEO -  PANDAPAK X DW.xlsx" />
            </label>
            <button type="submit">Import first workbook</button>
          </form>
        </section>
        """

    def render_project_options(self, projects: list[Any], selected_project_id: int) -> str:
        return "".join(
            f'<option value="{row["id"]}"{" selected" if int(row["id"]) == selected_project_id else ""}>{escape(row["name"])}</option>'
            for row in projects
        )

    def render_month_card(self, project_id: int, month: dict[str, Any]) -> str:
        cards = []
        for metric in month["metrics"]:
            href = f"/?project={project_id}&month={month['month_key']}&workstream={metric['linked_workstream']}#tasks"
            cards.append(
                f"""
                <div class="metric-card {escape(metric['status'])}">
                  <span>{escape(metric['metric_name'])}</span>
                  <strong>{self.render_number(metric['actual_value'])}</strong>
                  <small>Target {self.render_number(metric['target_value'])} {escape(metric['unit'])} | Forecast {self.render_number(metric['forecast_value'])}</small>
                  <a class="metric-link" href="{href}">Open linked tasks</a>
                  <input class="kpi-inline" data-project-id="{project_id}" data-month="{month['month_key']}" data-metric-key="{escape(metric['metric_key'])}" data-metric-name="{escape(metric['metric_name'])}" data-unit="{escape(metric['unit'])}" type="number" step="0.1" value="{'' if metric['target_value'] is None else metric['target_value']}" />
                </div>
                """
            )
        return f"""
        <article class="month-card">
          <div class="month-head">
            <h3>{escape(month['label'])}</h3>
            <span>{escape(month['month_key'])}</span>
          </div>
          <div class="metric-grid">
            {''.join(cards) or '<div class="empty">Chua co KPI.</div>'}
          </div>
        </article>
        """

    def render_week_lane(self, owner: str, items: list[dict[str, Any]]) -> str:
        return f"""
        <article class="lane">
          <header>
            <h3>{escape(owner)}</h3>
            <span>{len(items)} tasks</span>
          </header>
          {''.join(
              f'<div class="lane-card {escape(item["warning_level"])}"><strong>{escape(item["task_name"])}</strong><span>{escape(item["page_title"])}</span><small>{format_date(item["forecast_due"])}</small></div>'
              for item in items
          )}
        </article>
        """

    def render_page_row(self, page: Any) -> str:
        health = float(page["health_score"] or 0)
        health_class = "health-good" if health >= 75 else "health-warn" if health >= 50 else "health-bad"
        return f"""
        <tr>
          <td>
            <strong>{escape(page['title'])}</strong>
            <div class="subtle">{escape(page['primary_keyword'] or page['url'] or '')}</div>
          </td>
          <td>
            <select class="autosave" data-kind="page" data-id="{page['id']}" data-field="priority">
              {self.render_priority_options(page['priority'])}
            </select>
          </td>
          <td>
            <select class="autosave" data-kind="page" data-id="{page['id']}" data-field="content_status">
              {self.render_status_options(page['content_status'])}
            </select>
          </td>
          <td>
            <input class="autosave" data-kind="page" data-id="{page['id']}" data-field="publish_target" type="date" value="{escape(page['publish_target'] or '')}" />
            <div class="subtle">Review {format_date(page['review_deadline'] or '')}</div>
          </td>
          <td>
            <input class="autosave" data-kind="page" data-id="{page['id']}" data-field="index_status" type="text" value="{escape(page['index_status'] or '')}" />
            <div class="subtle">{escape(page['image_status'] or 'No image note')}</div>
          </td>
          <td>
            <input class="autosave" data-kind="page" data-id="{page['id']}" data-field="pic" type="text" value="{escape(page['pic'] or '')}" />
          </td>
          <td>
            <span class="health-pill {health_class}">{health:.0f}</span>
          </td>
        </tr>
        """

    def render_task_row(self, task: Any) -> str:
        return f"""
        <tr>
          <td>
            <strong>{escape(task['task_name'])}</strong>
            <div class="subtle">{escape(task['workstream'])}</div>
          </td>
          <td>
            <strong>{escape(task['page_title'] or '')}</strong>
            <div class="subtle">{escape(task['priority'] or '')}</div>
          </td>
          <td>
            <input class="autosave" data-kind="task" data-id="{task['id']}" data-field="owner" type="text" value="{escape(task['owner'] or '')}" />
          </td>
          <td>{format_date(task['planned_due'] or '')}</td>
          <td>{format_date(task['forecast_due'] or '')}</td>
          <td><input class="autosave" data-kind="task" data-id="{task['id']}" data-field="actual_due" type="date" value="{escape(task['actual_due'] or '')}" /></td>
          <td><input class="autosave" data-kind="task" data-id="{task['id']}" data-field="actual_done" type="date" value="{escape(task['actual_done'] or '')}" /></td>
          <td>
            <select class="autosave" data-kind="task" data-id="{task['id']}" data-field="status">
              {self.render_task_status_options(task['status'])}
            </select>
            <div class="subtle">{escape(task['success_check'] or '')}</div>
          </td>
          <td><span class="tag tag-{escape(task['warning_level'])}">{escape(task['warning_level'])}</span></td>
          <td>{escape(task['impact_score'])}</td>
        </tr>
        """

    def render_priority_options(self, current: str) -> str:
        return "".join(
            f'<option value="{value}"{" selected" if value == current else ""}>{value}</option>'
            for value in ("P1", "P2", "P3")
        )

    def render_status_options(self, current: str) -> str:
        options = ["To do", "Doing", "To review (Internal)", "To review (Client)", "Published", "Not started"]
        return "".join(
            f'<option value="{escape(value)}"{" selected" if value == current else ""}>{escape(value)}</option>'
            for value in options
        )

    def render_task_status_options(self, current: str) -> str:
        options = [("todo", "To do"), ("doing", "Doing"), ("blocked", "Blocked"), ("done", "Done")]
        return "".join(
            f'<option value="{value}"{" selected" if value == current else ""}>{label}</option>'
            for value, label in options
        )

    def render_workstream_options(self, current: str) -> str:
        options = ["All", "Research", "Content", "Onpage", "Launch", "Measurement", "Internal Link", "Offpage"]
        html_options = []
        for option in options:
            value = "" if option == "All" else option
            html_options.append(
                f'<option value="{value}"{" selected" if value == current else ""}>{option}</option>'
            )
        return "".join(html_options)

    def render_warning_options(self, current: str) -> str:
        options = ["green", "yellow", "orange", "red", "done"]
        return "".join(
            f'<option value="{value}"{" selected" if value == current else ""}>{value}</option>'
            for value in options
        )

    def render_month_options(self, board: list[dict[str, Any]], current: str) -> str:
        return "".join(
            f'<option value="{escape(month["month_key"])}"{" selected" if month["month_key"] == current else ""}>{escape(month["label"])}</option>'
            for month in board
        )

    def render_capacity_input(self, role: str, value: float) -> str:
        return f"""
        <label>{escape(role)} hrs/week
          <input type="number" step="0.5" name="{escape(role)}" value="{escape(value)}" />
        </label>
        """

    def render_number(self, value: Any) -> str:
        if value is None:
            return "-"
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)

    def render_flash(self, *, error: str = "", notice: str = "") -> str:
        parts = []
        if error:
            parts.append(f'<div class="flash flash-error">{escape(error)}</div>')
        if notice:
            parts.append(f'<div class="flash flash-notice">{escape(notice)}</div>')
        return "".join(parts)

    def render_shell(self, title: str, body: str) -> str:
        return f"""<!doctype html>
        <html lang="en">
        <head>
          <meta charset="utf-8" />
          <meta name="viewport" content="width=device-width, initial-scale=1" />
          <title>{escape(title)} · SEO Brain</title>
          <style>
            :root {{
              --bg: #f3eee5;
              --paper: rgba(255, 252, 245, 0.88);
              --ink: #1e2a24;
              --muted: #6d756d;
              --line: rgba(42, 61, 44, 0.16);
              --accent: #0d6c63;
              --accent-2: #cd7b2d;
              --good: #2d7a48;
              --warn: #b98014;
              --bad: #b33a30;
              --shadow: 0 18px 50px rgba(45, 49, 32, 0.12);
            }}
            * {{ box-sizing: border-box; }}
            body {{
              margin: 0;
              font-family: "Avenir Next", "Trebuchet MS", sans-serif;
              color: var(--ink);
              background:
                radial-gradient(circle at top left, rgba(13, 108, 99, 0.18), transparent 32%),
                radial-gradient(circle at top right, rgba(205, 123, 45, 0.18), transparent 26%),
                linear-gradient(180deg, #f6f1e8 0%, #efe7dc 100%);
            }}
            .shell {{
              max-width: 1460px;
              margin: 0 auto;
              padding: 28px 24px 64px;
            }}
            .hero {{
              display: grid;
              gap: 18px;
              grid-template-columns: 2fr 1fr;
              padding: 28px;
              border-radius: 28px;
              background: linear-gradient(135deg, rgba(255,255,255,0.88), rgba(250,245,237,0.76));
              border: 1px solid rgba(42, 61, 44, 0.12);
              box-shadow: var(--shadow);
              margin-bottom: 18px;
            }}
            .empty-hero {{
              grid-template-columns: 1fr;
            }}
            .eyebrow {{
              margin: 0 0 8px;
              text-transform: uppercase;
              letter-spacing: 0.12em;
              font-size: 12px;
              font-weight: 700;
              color: var(--accent);
            }}
            h1, h2, h3 {{
              margin: 0;
              font-family: "Georgia", "Palatino Linotype", serif;
            }}
            h1 {{ font-size: clamp(34px, 5vw, 54px); line-height: 1; }}
            h2 {{ font-size: 30px; margin-bottom: 4px; }}
            h3 {{ font-size: 20px; }}
            .lede {{
              margin: 12px 0 0;
              max-width: 900px;
              color: var(--muted);
              font-size: 17px;
              line-height: 1.7;
            }}
            .hero-panel {{
              padding: 18px;
              border-radius: 22px;
              background: rgba(13, 108, 99, 0.08);
              border: 1px solid rgba(13, 108, 99, 0.15);
            }}
            .project-switch {{
              display: grid;
              gap: 8px;
              margin-bottom: 14px;
            }}
            .summary-grid, .settings-grid {{
              display: grid;
              gap: 16px;
              grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
              margin-bottom: 18px;
            }}
            .settings-grid {{
              grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
            }}
            .stat-card, .panel {{
              border-radius: 24px;
              background: var(--paper);
              border: 1px solid var(--line);
              box-shadow: var(--shadow);
            }}
            .stat-card {{
              padding: 22px;
            }}
            .stat-card strong {{
              display: block;
              font-size: 34px;
              margin-bottom: 6px;
            }}
            .stat-card span {{
              color: var(--muted);
            }}
            .panel {{
              padding: 22px;
              margin-bottom: 18px;
            }}
            .panel-head {{
              display: flex;
              justify-content: space-between;
              gap: 16px;
              align-items: end;
              margin-bottom: 16px;
            }}
            .muted {{
              color: var(--muted);
            }}
            .month-grid {{
              display: grid;
              gap: 16px;
              grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
            }}
            .month-card {{
              border-radius: 18px;
              border: 1px solid var(--line);
              background: rgba(255,255,255,0.82);
              padding: 16px;
            }}
            .month-head {{
              display: flex;
              justify-content: space-between;
              gap: 8px;
              margin-bottom: 12px;
            }}
            .metric-grid {{
              display: grid;
              gap: 10px;
            }}
            .metric-card {{
              display: grid;
              gap: 6px;
              padding: 12px;
              border-radius: 16px;
              border: 1px solid transparent;
              background: rgba(239, 246, 243, 0.8);
            }}
            .metric-card.green {{ border-color: rgba(45,122,72,0.3); }}
            .metric-card.yellow {{ border-color: rgba(185,128,20,0.35); }}
            .metric-card.red {{ border-color: rgba(179,58,48,0.35); }}
            .metric-card strong {{
              font-size: 26px;
            }}
            .metric-link {{
              color: var(--accent);
              font-size: 13px;
              text-decoration: none;
              font-weight: 700;
            }}
            .metric-card small {{
              color: var(--muted);
              line-height: 1.4;
            }}
            .kpi-inline {{
              width: 100%;
              margin-top: 6px;
            }}
            .kanban-grid {{
              display: grid;
              gap: 14px;
              grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            }}
            .lane {{
              border-radius: 18px;
              border: 1px solid var(--line);
              background: rgba(255,255,255,0.72);
              padding: 14px;
            }}
            .lane header {{
              display: flex;
              justify-content: space-between;
              margin-bottom: 10px;
            }}
            .lane-card {{
              border-radius: 14px;
              padding: 10px 12px;
              margin-bottom: 8px;
              background: rgba(243,238,229,0.9);
              border-left: 4px solid var(--accent);
            }}
            .lane-card.yellow {{ border-left-color: var(--warn); }}
            .lane-card.orange, .lane-card.red {{ border-left-color: var(--bad); }}
            .lane-card span, .lane-card small {{
              display: block;
              color: var(--muted);
              margin-top: 4px;
            }}
            .filter-bar, .inline-form, .stack-form {{
              display: grid;
              gap: 12px;
              grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
              align-items: end;
              margin-bottom: 14px;
            }}
            .stack-form {{
              grid-template-columns: 1fr;
            }}
            label {{
              display: grid;
              gap: 6px;
              font-size: 13px;
              font-weight: 700;
              color: var(--muted);
            }}
            input, select, button, textarea {{
              width: 100%;
              border: 1px solid rgba(42, 61, 44, 0.18);
              border-radius: 12px;
              padding: 10px 12px;
              font: inherit;
              color: var(--ink);
              background: rgba(255,255,255,0.92);
            }}
            button {{
              cursor: pointer;
              background: linear-gradient(135deg, #11594f, #0d6c63);
              color: #fff;
              font-weight: 700;
              border: none;
            }}
            .table-wrap {{
              overflow: auto;
            }}
            table {{
              width: 100%;
              border-collapse: collapse;
            }}
            th, td {{
              border-top: 1px solid var(--line);
              padding: 12px 10px;
              text-align: left;
              vertical-align: top;
              min-width: 120px;
            }}
            th {{
              color: var(--muted);
              font-size: 13px;
            }}
            .subtle {{
              margin-top: 4px;
              color: var(--muted);
              font-size: 12px;
              line-height: 1.4;
            }}
            .tag, .health-pill {{
              display: inline-flex;
              align-items: center;
              justify-content: center;
              border-radius: 999px;
              padding: 4px 10px;
              font-size: 12px;
              font-weight: 700;
            }}
            .tag-green, .health-good {{
              background: rgba(45,122,72,0.13);
              color: var(--good);
            }}
            .tag-yellow, .health-warn {{
              background: rgba(185,128,20,0.13);
              color: var(--warn);
            }}
            .tag-orange, .tag-red, .health-bad {{
              background: rgba(179,58,48,0.13);
              color: var(--bad);
            }}
            .tag-done {{
              background: rgba(13,108,99,0.13);
              color: var(--accent);
            }}
            .flash {{
              margin-bottom: 18px;
              padding: 14px 18px;
              border-radius: 16px;
            }}
            .flash-error {{
              background: rgba(179,58,48,0.12);
              color: var(--bad);
              border: 1px solid rgba(179,58,48,0.2);
            }}
            .flash-notice {{
              background: rgba(45,122,72,0.12);
              color: var(--good);
              border: 1px solid rgba(45,122,72,0.2);
            }}
            .empty, .empty-cell {{
              color: var(--muted);
              padding: 12px;
            }}
            .empty-cell {{
              text-align: center;
            }}
            .toast {{
              position: fixed;
              right: 18px;
              bottom: 18px;
              padding: 12px 16px;
              border-radius: 14px;
              background: rgba(30, 42, 36, 0.92);
              color: #fff;
              font-size: 13px;
              opacity: 0;
              transform: translateY(8px);
              transition: opacity 0.2s ease, transform 0.2s ease;
              pointer-events: none;
            }}
            .toast.visible {{
              opacity: 1;
              transform: translateY(0);
            }}
            @media (max-width: 900px) {{
              .hero {{
                grid-template-columns: 1fr;
              }}
            }}
          </style>
        </head>
        <body>
          <main class="shell">
            {body}
          </main>
          <div id="toast" class="toast">Saved</div>
          <script>
            const toast = document.getElementById("toast");
            function showToast(message) {{
              toast.textContent = message;
              toast.classList.add("visible");
              window.clearTimeout(window.__toastTimer);
              window.__toastTimer = window.setTimeout(() => toast.classList.remove("visible"), 1600);
            }}

            async function postJson(url, payload) {{
              const response = await fetch(url, {{
                method: "POST",
                headers: {{ "Content-Type": "application/json" }},
                body: JSON.stringify(payload)
              }});
              const data = await response.json();
              if (!data.ok) {{
                throw new Error(data.error || "Request failed");
              }}
              return data;
            }}

            document.querySelectorAll(".autosave").forEach((element) => {{
              element.addEventListener("change", async () => {{
                const kind = element.dataset.kind;
                const id = Number(element.dataset.id);
                const field = element.dataset.field;
                try {{
                  await postJson(kind === "task" ? "/api/task" : "/api/page", {{
                    [`${{kind}}_id`]: id,
                    field,
                    value: element.value
                  }});
                  showToast("Saved and recalculated");
                }} catch (error) {{
                  showToast(error.message);
                }}
              }});
            }});

            document.querySelectorAll(".kpi-inline").forEach((element) => {{
              element.addEventListener("change", async (event) => {{
                event.preventDefault();
                try {{
                  await postJson("/api/kpi", {{
                    project_id: Number(element.dataset.projectId),
                    month_key: element.dataset.month,
                    metric_key: element.dataset.metricKey,
                    metric_name: element.dataset.metricName,
                    unit: element.dataset.unit,
                    target_value: element.value
                  }});
                  showToast("KPI updated");
                }} catch (error) {{
                  showToast(error.message);
                }}
              }});
            }});

            const kpiForm = document.getElementById("kpi-form");
            if (kpiForm) {{
              kpiForm.addEventListener("submit", async (event) => {{
                event.preventDefault();
                const formData = new FormData(kpiForm);
                const payload = Object.fromEntries(formData.entries());
                try {{
                  await postJson("/api/kpi", payload);
                  showToast("KPI added");
                  window.location.reload();
                }} catch (error) {{
                  showToast(error.message);
                }}
              }});
            }}

            const capacityForm = document.getElementById("capacity-form");
            if (capacityForm) {{
              capacityForm.addEventListener("submit", async (event) => {{
                event.preventDefault();
                const formData = new FormData(capacityForm);
                const payload = Object.fromEntries(formData.entries());
                try {{
                  await postJson("/api/settings", payload);
                  showToast("Schedule rebalanced");
                  window.location.reload();
                }} catch (error) {{
                  showToast(error.message);
                }}
              }});
            }}
          </script>
        </body>
        </html>"""
