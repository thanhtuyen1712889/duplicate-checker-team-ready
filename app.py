#!/usr/bin/env python3
"""Self-contained web app for template-aware duplicate checking."""

from __future__ import annotations

import argparse
import base64
import csv
import html
import io
import json
import os
import sys
from email.parser import BytesParser
from email.policy import default
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

from duplicate_checker.service import DuplicateCheckerService, parse_google_doc_id, slugify
from seo_brain.web import SeoBrainApp


APP_ROOT = Path(__file__).resolve().parent
SERVICE = DuplicateCheckerService(APP_ROOT)
SEO_APP = SeoBrainApp(APP_ROOT)


class AppHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self.respond_text("ok")
            return
        if not self.require_auth():
            return
        if parsed.path == "/":
            self.respond_html(render_dashboard(parsed.query))
            return
        if parsed.path == "/document":
            params = parse_qs(parsed.query)
            doc_id = int(params.get("id", ["0"])[0] or 0)
            self.respond_html(render_document(doc_id))
            return
        if parsed.path == "/templates":
            self.respond_html(render_templates(parsed.query))
            return
        if parsed.path == "/export.csv":
            self.respond_csv(render_export_csv(parsed.query), "duplicate-checker-export.csv")
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        if not self.require_auth():
            return
        if self.path == "/submit":
            self.handle_submit()
            return
        if self.path == "/templates/create":
            self.handle_template_create()
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def handle_submit(self) -> None:
        fields, files = self.parse_form()
        document_key = (fields.get("document_key") or "").strip()
        display_name = (fields.get("display_name") or "").strip()
        pasted_text = (fields.get("pasted_text") or "").strip()
        google_docs_url = (fields.get("google_docs_url") or "").strip()
        template_choice = (fields.get("template_id") or "").strip()
        upload = files.get("upload")

        if not document_key:
            if google_docs_url:
                document_key = google_doc_key(google_docs_url)
            else:
                fallback_name = upload["filename"] if upload else display_name or "document"
                document_key = slugify(Path(fallback_name).stem)
        if not display_name:
            if upload:
                display_name = Path(upload["filename"]).stem
            elif google_docs_url:
                display_name = document_key
            else:
                display_name = document_key
        if not upload and not google_docs_url and not pasted_text:
            self.respond_html(
                render_dashboard("", error="Vui long upload file, dan plain text, hoac nhap Google Docs URL."),
                status=400,
            )
            return

        file_bytes = upload["content"] if upload else None
        source_name = upload["filename"] if upload else google_docs_url or f"{display_name}.txt"
        try:
            result = SERVICE.analyze_submission(
                document_key=document_key,
                display_name=display_name,
                source_name=source_name,
                file_bytes=file_bytes,
                pasted_text=pasted_text,
                remote_url=google_docs_url,
                forced_template_id=template_choice,
            )
            document_id = SERVICE.save_result(result)
        except Exception as exc:  # noqa: BLE001
            self.respond_html(render_dashboard("", error=f"Phan tich that bai: {exc}"), status=500)
            return
        self.redirect(f"/document?id={document_id}")

    def handle_template_create(self) -> None:
        fields, files = self.parse_form()
        template_name = (fields.get("template_name") or "").strip()
        strategy_id = (fields.get("strategy_id") or "").strip() or "generic_text_v1"
        samples: list[tuple[str, bytes]] = []
        for field_name in ("sample_1", "sample_2", "sample_3"):
            upload = files.get(field_name)
            if upload:
                samples.append((str(upload["filename"]), bytes(upload["content"])))

        if not template_name:
            self.respond_html(render_templates("", error="Vui long nhap ten template."), status=400)
            return
        if not samples:
            self.respond_html(
                render_templates("", error="Vui long upload it nhat 1 file mau. 2-3 file se giup template on dinh hon."),
                status=400,
            )
            return

        try:
            template = SERVICE.create_custom_template(
                name=template_name,
                strategy_id=strategy_id,
                samples=samples,
            )
        except Exception as exc:  # noqa: BLE001
            self.respond_html(render_templates("", error=f"Tao template that bai: {exc}"), status=500)
            return
        mode = "auto-detect" if template.get("auto_detect_enabled", True) else "chon tay"
        notice = urlencode({"notice": f"Da tao template {template['name']}. Template nay dang o che do {mode}."})
        self.redirect(f"/templates?{notice}")

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

    def respond_html(self, content: str, status: int = 200) -> None:
        encoded = content.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def respond_csv(self, payload: str, filename: str) -> None:
        encoded = payload.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
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

    def require_auth(self) -> bool:
        username = os.getenv("APP_USERNAME", "").strip()
        password = os.getenv("APP_PASSWORD", "").strip()
        if not username or not password:
            return True
        header = self.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            self.send_auth_challenge()
            return False
        token = header.split(" ", 1)[1].strip()
        try:
            decoded = base64.b64decode(token).decode("utf-8")
        except Exception:  # noqa: BLE001
            self.send_auth_challenge()
            return False
        if decoded != f"{username}:{password}":
            self.send_auth_challenge()
            return False
        return True

    def send_auth_challenge(self) -> None:
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="Duplicate Checker"')
        self.end_headers()


def render_dashboard(query_string: str, *, error: str = "") -> str:
    params = parse_qs(query_string)
    status_filter = (params.get("status", [""])[0] or "").strip()
    template_filter = (params.get("template_id", [""])[0] or "").strip()
    search = (params.get("search", [""])[0] or "").strip()
    notice = (params.get("notice", [""])[0] or "").strip()
    documents = SERVICE.storage.latest_documents(status=status_filter, template_id=template_filter, search=search)
    templates = SERVICE.list_templates()
    status_counts = SERVICE.storage.status_counts()
    total_documents = sum(status_counts.values())
    summary_html = """
      <div class="summary-grid">
        <div class="stat-card"><strong>{}</strong><span>Tong bai dang luu</span></div>
        <div class="stat-card"><strong>{}</strong><span>Green</span></div>
        <div class="stat-card"><strong>{}</strong><span>Yellow</span></div>
        <div class="stat-card"><strong>{}</strong><span>Red</span></div>
      </div>
    """.format(
        total_documents,
        status_counts.get("green", 0),
        status_counts.get("yellow", 0),
        status_counts.get("red", 0),
    )
    rows_html = "".join(render_document_row(row) for row in documents)
    table_html = (
        """
        <table>
          <thead>
            <tr>
              <th>Bai viet</th>
              <th>Template</th>
              <th>Trang thai</th>
              <th>Diem unique</th>
              <th>Version</th>
              <th>Thoi gian</th>
            </tr>
          </thead>
          <tbody>
        """
        + rows_html
        + "</tbody></table>"
        if rows_html
        else '<div class="empty">Khong co ket qua nao khop bo loc hien tai.</div>'
    )
    export_query = urlencode({key: value for key, value in {"status": status_filter, "template_id": template_filter, "search": search}.items() if value})
    export_link = f"/export.csv?{export_query}" if export_query else "/export.csv"
    return page(
        title="Duplicate Checker",
        current_nav="dashboard",
        body=f"""
        <section class="hero">
          <h1>Tool Check Duplicate Content Theo Template</h1>
          <p>Flow danh cho content team: nop bai, tu check voi kho bai cung template, xem canh bao xanh / vang / do, sua bai va nop lai.</p>
          {summary_html}
        </section>
        {render_flash(error, kind="error")}
        {render_flash(notice, kind="notice")}
        <section class="card">
          <h2>Nop Bai</h2>
          <p>Ban co the dung 1 trong 3 cach: upload file, dan plain text, hoac nhap Google Docs URL. Neu template auto-detect chua dung, hay chon template thu cong.</p>
          <form action="/submit" method="post" enctype="multipart/form-data" class="stack">
            <div class="grid2">
              <label>Ma bai (tuy chon)
                <input name="document_key" placeholder="pet-lids-150mm" />
              </label>
              <label>Ten hien thi (tuy chon)
                <input name="display_name" placeholder="PET Lids 150mm (300pcs)" />
              </label>
            </div>
            <div class="grid2">
              <label>Template
                <select name="template_id">
                  <option value="">Auto-detect (khuyen nghi)</option>
                  {render_template_options(templates)}
                </select>
              </label>
              <label>Google Docs URL
                <input name="google_docs_url" placeholder="https://docs.google.com/document/d/..." />
              </label>
            </div>
            <label>Tai file DOCX/TXT
              <input type="file" name="upload" accept=".docx,.txt" />
            </label>
            <label>Hoac dan plain text
              <textarea name="pasted_text" rows="10" placeholder="Dan noi dung bai viet vao day neu khong muon upload file."></textarea>
            </label>
            <button type="submit">Check Va Luu</button>
          </form>
        </section>
        <section class="card">
          <div class="card-head">
            <div>
              <h2>Kho Bai Da Luu</h2>
              <p>So sanh se uu tien cac bai trong cung template. Chi version moi nhat cua moi bai duoc giu active.</p>
            </div>
            <a class="button secondary" href="{escape(export_link)}">Export CSV</a>
          </div>
          <form method="get" action="/" class="filters">
            <label>Trang thai
              <select name="status">
                <option value="">Tat ca</option>
                <option value="green" {"selected" if status_filter == "green" else ""}>Green</option>
                <option value="yellow" {"selected" if status_filter == "yellow" else ""}>Yellow</option>
                <option value="red" {"selected" if status_filter == "red" else ""}>Red</option>
              </select>
            </label>
            <label>Template
              <select name="template_id">
                <option value="">Tat ca template</option>
                {render_template_options(templates, selected_id=template_filter)}
              </select>
            </label>
            <label>Tim kiem
              <input name="search" value="{escape(search)}" placeholder="Ten bai hoac ma bai" />
            </label>
            <button type="submit">Loc</button>
          </form>
          {table_html}
        </section>
        <section class="card">
          <h2>Huong Danh Nhanh</h2>
          <div class="legend-grid">
            <div><strong>Green</strong><span>Co the dat. Khong co overlap dang lo.</span></div>
            <div><strong>Yellow</strong><span>Can review. Thuong la co overlap y tuong hoac cau chua qua giong.</span></div>
            <div><strong>Red</strong><span>Can sua. Thuong la copy span, FAQ answer, feature copy hoac conclusion bi giong qua muc.</span></div>
          </div>
        </section>
        """,
    )


def render_templates(query_string: str, *, error: str = "") -> str:
    params = parse_qs(query_string)
    notice = (params.get("notice", [""])[0] or "").strip()
    templates = SERVICE.list_templates()
    strategy_options = SERVICE.list_strategy_options()
    rows = []
    for template in templates:
        source_label = "Custom" if template.get("custom_template") else "Built-in"
        auto_detect_label = "On" if template.get("auto_detect_enabled", True) else "Manual only"
        strategy_label = template.get("strategy_id", template["id"])
        rows.append(
            f"""
            <tr>
              <td>{escape(template['name'])}</td>
              <td><code>{escape(template['id'])}</code></td>
              <td>{escape(source_label)}</td>
              <td>{escape(auto_detect_label)}</td>
              <td>{escape(strategy_label)}</td>
              <td>{int(template.get('document_count', 0))}</td>
            </tr>
            """
        )
    table_html = (
        """
        <table>
          <thead>
            <tr><th>Template</th><th>ID</th><th>Loai</th><th>Auto-detect</th><th>Strategy</th><th>So bai</th></tr>
          </thead>
          <tbody>
        """
        + "".join(rows)
        + "</tbody></table>"
    )
    return page(
        title="Templates",
        current_nav="templates",
        body=f"""
        <section class="hero">
          <h1>Quan Ly Template</h1>
          <p>Template moi se duoc tao tu file mau va strategy co san. Mac dinh template custom o che do chon tay, phu hop cho giai doan on dinh hoa truoc khi bat auto-detect.</p>
        </section>
        {render_flash(error, kind="error")}
        {render_flash(notice, kind="notice")}
        <section class="card">
          <h2>Tao Template Moi</h2>
          <form action="/templates/create" method="post" enctype="multipart/form-data" class="stack">
            <div class="grid2">
              <label>Ten template
                <input name="template_name" placeholder="PandaPak Product Detail - Bowl Family" />
              </label>
              <label>Strategy goc
                <select name="strategy_id">
                  {render_strategy_options(strategy_options)}
                </select>
              </label>
            </div>
            <p>Upload 2-3 file mau neu co. Tool se rut heading signature tu nhung file nay va luu template vao registry.</p>
            <div class="grid3">
              <label>Sample 1
                <input type="file" name="sample_1" accept=".docx,.txt" />
              </label>
              <label>Sample 2
                <input type="file" name="sample_2" accept=".docx,.txt" />
              </label>
              <label>Sample 3
                <input type="file" name="sample_3" accept=".docx,.txt" />
              </label>
            </div>
            <button type="submit">Tao Template</button>
          </form>
        </section>
        <section class="card">
          <h2>Danh Sach Template</h2>
          {table_html}
        </section>
        """,
    )


def render_document(document_id: int) -> str:
    row = SERVICE.storage.get_document(document_id)
    if not row:
        return page(
            title="Not found",
            current_nav="dashboard",
            body='<section class="card"><p>Khong tim thay bai viet.</p><p><a href="/">Quay lai dashboard</a></p></section>',
        )
    parsed = json.loads(row["parsed_json"])
    findings = parsed.get("findings", [])
    sections = parsed.get("sections", {})
    section_risks = parsed.get("section_risks", {})
    finding_cards = []
    for finding in findings[:12]:
        finding_cards.append(
            f"""
            <article class="finding {escape(finding['severity'])}">
              <div class="finding-head">
                <strong>{escape(finding['section_name'])}</strong>
                <span class="pill">{escape(finding['rule'])}</span>
                <span class="pill">{finding['risk'] * 100:.1f}% risk</span>
                <a href="/document?id={int(finding['other_document_id'])}">so voi {escape(finding['other_display_name'])}</a>
              </div>
              <div class="grid2">
                <div>
                  <h4>Doan hien tai</h4>
                  <pre>{escape(finding['excerpt'])}</pre>
                </div>
                <div>
                  <h4>Doan bi giong</h4>
                  <pre>{escape(finding['other_excerpt'])}</pre>
                </div>
              </div>
            </article>
            """
        )
    if not finding_cards:
        finding_cards.append('<div class="empty">Khong phat hien overlap dang lo voi kho bai hien tai.</div>')

    section_rows = []
    for name, section in sections.items():
        text = section.get("text", "")
        section_rows.append(
            f"""
            <tr>
              <td>{escape(name)}</td>
              <td>{escape(section.get('mode', ''))}</td>
              <td>{float(section_risks.get(name, 0.0)):.1f}</td>
              <td><details><summary>Xem</summary><pre>{escape(text[:1800])}</pre></details></td>
            </tr>
            """
        )

    source_name = escape(row["source_name"])
    return page(
        title=str(row["display_name"]),
        current_nav="dashboard",
        body=f"""
        <section class="hero compact">
          <p><a href="/">Quay lai dashboard</a></p>
          <h1>{escape(row['display_name'])}</h1>
          <div class="summary">
            <span class="status {escape(row['status'])}">{escape(row['status']).upper()}</span>
            <span class="metric">Diem unique {row['unique_score']:.1f}</span>
            <span class="metric">Risk {row['total_risk']:.1f}</span>
            <span class="metric">Template {escape(row['template_name'])}</span>
            <span class="metric">Version {row['version']}</span>
            <span class="metric">Source {source_name}</span>
          </div>
        </section>
        <section class="card">
          <h2>Canh Bao Chinh</h2>
          {''.join(finding_cards)}
        </section>
        <section class="card">
          <h2>Chi Tiet Theo Section</h2>
          <table>
            <thead>
              <tr><th>Section</th><th>Mode</th><th>Risk</th><th>Preview</th></tr>
            </thead>
            <tbody>
              {''.join(section_rows)}
            </tbody>
          </table>
        </section>
        """,
    )


def render_export_csv(query_string: str) -> str:
    params = parse_qs(query_string)
    status_filter = (params.get("status", [""])[0] or "").strip()
    template_filter = (params.get("template_id", [""])[0] or "").strip()
    search = (params.get("search", [""])[0] or "").strip()
    rows = SERVICE.storage.latest_documents(status=status_filter, template_id=template_filter, search=search)
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["document_key", "display_name", "template_name", "status", "unique_score", "version", "created_at"])
    for row in rows:
        writer.writerow(
            [
                row["document_key"],
                row["display_name"],
                row["template_name"],
                row["status"],
                f"{row['unique_score']:.2f}",
                row["version"],
                row["created_at"],
            ]
        )
    return buffer.getvalue()


def render_document_row(row) -> str:
    return f"""
    <tr>
      <td><a href="/document?id={row['id']}">{escape(row['display_name'])}</a></td>
      <td>{escape(row['template_name'])}</td>
      <td><span class="status {escape(row['status'])}">{escape(row['status']).upper()}</span></td>
      <td>{row['unique_score']:.1f}</td>
      <td>{row['version']}</td>
      <td>{escape(row['created_at'][:19].replace('T', ' '))}</td>
    </tr>
    """


def render_template_options(templates: list[dict], selected_id: str = "") -> str:
    options = []
    for template in templates:
        selected = "selected" if template["id"] == selected_id else ""
        options.append(f'<option value="{escape(template["id"])}" {selected}>{escape(template["name"])}</option>')
    return "".join(options)


def render_strategy_options(strategies: list[dict[str, str]]) -> str:
    return "".join(
        f'<option value="{escape(strategy["id"])}">{escape(strategy["name"])}</option>'
        for strategy in strategies
    )


def render_flash(message: str, *, kind: str) -> str:
    if not message:
        return ""
    css = "error" if kind == "error" else "notice"
    return f'<div class="{css}">{escape(message)}</div>'


def render_nav(current_nav: str) -> str:
    links = [
        ("dashboard", "/", "Dashboard"),
        ("templates", "/templates", "Templates"),
    ]
    return "".join(
        f'<a class="nav-link {"active" if name == current_nav else ""}" href="{href}">{label}</a>'
        for name, href, label in links
    )


def page(*, title: str, current_nav: str, body: str) -> str:
    return f"""<!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>{escape(title)}</title>
      <style>
        :root {{
          --bg: #f4f1ea;
          --card: #fffdf8;
          --ink: #231f1a;
          --muted: #6f665d;
          --line: #d9d0c7;
          --green: #2d7a48;
          --yellow: #b98014;
          --red: #b33a30;
          --accent: #0f5c73;
          --notice: #245e34;
        }}
        * {{ box-sizing: border-box; }}
        body {{
          margin: 0;
          font-family: Georgia, "Times New Roman", serif;
          color: var(--ink);
          background:
            radial-gradient(circle at top left, #efe1cc 0%, transparent 28%),
            linear-gradient(180deg, #f8f5ef 0%, var(--bg) 100%);
        }}
        .shell {{
          max-width: 1180px;
          margin: 0 auto;
          padding: 20px;
        }}
        .topbar {{
          display: flex;
          gap: 10px;
          flex-wrap: wrap;
          align-items: center;
          margin-bottom: 16px;
        }}
        .nav-link {{
          display: inline-block;
          padding: 10px 14px;
          border-radius: 999px;
          border: 1px solid var(--line);
          background: rgba(255, 255, 255, 0.62);
          color: var(--ink);
          text-decoration: none;
          font-size: 14px;
        }}
        .nav-link.active {{
          background: var(--accent);
          color: #fff;
          border-color: var(--accent);
        }}
        .hero {{
          padding: 18px 0 10px;
        }}
        .hero.compact {{ padding-bottom: 0; }}
        h1, h2, h3, h4 {{
          margin: 0 0 12px;
          font-weight: 600;
          line-height: 1.15;
        }}
        p {{
          margin: 0 0 12px;
          line-height: 1.55;
          color: var(--muted);
        }}
        .card {{
          background: var(--card);
          border: 1px solid var(--line);
          border-radius: 18px;
          padding: 22px;
          margin-top: 18px;
          box-shadow: 0 8px 26px rgba(35, 31, 26, 0.06);
        }}
        .card-head {{
          display: flex;
          justify-content: space-between;
          gap: 12px;
          align-items: center;
          flex-wrap: wrap;
          margin-bottom: 12px;
        }}
        .stack {{ display: grid; gap: 14px; }}
        .grid2 {{
          display: grid;
          gap: 14px;
          grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
        }}
        .grid3 {{
          display: grid;
          gap: 14px;
          grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        }}
        label {{
          display: grid;
          gap: 8px;
          font-size: 14px;
          color: var(--ink);
        }}
        input, textarea, button, select {{
          font: inherit;
          border-radius: 12px;
        }}
        input, textarea, select {{
          width: 100%;
          border: 1px solid var(--line);
          padding: 12px 14px;
          background: #fff;
        }}
        button, .button {{
          display: inline-block;
          border: 0;
          padding: 13px 18px;
          background: var(--accent);
          color: #fff;
          cursor: pointer;
          width: fit-content;
          text-decoration: none;
          border-radius: 12px;
        }}
        .button.secondary {{
          background: rgba(15, 92, 115, 0.08);
          color: var(--accent);
          border: 1px solid rgba(15, 92, 115, 0.16);
        }}
        .filters {{
          display: grid;
          gap: 12px;
          grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
          align-items: end;
          margin-bottom: 16px;
        }}
        table {{
          width: 100%;
          border-collapse: collapse;
        }}
        th, td {{
          text-align: left;
          vertical-align: top;
          border-top: 1px solid var(--line);
          padding: 12px 10px;
          font-size: 14px;
        }}
        th {{
          color: var(--muted);
          font-weight: 600;
        }}
        .status {{
          display: inline-block;
          padding: 5px 10px;
          border-radius: 999px;
          font-size: 12px;
          font-weight: 700;
          letter-spacing: 0.04em;
        }}
        .status.green {{ background: rgba(45, 122, 72, 0.12); color: var(--green); }}
        .status.yellow {{ background: rgba(185, 128, 20, 0.12); color: var(--yellow); }}
        .status.red {{ background: rgba(179, 58, 48, 0.12); color: var(--red); }}
        .metric {{
          display: inline-block;
          margin-right: 10px;
          padding: 6px 10px;
          border-radius: 999px;
          background: rgba(15, 92, 115, 0.08);
          color: var(--accent);
          font-size: 13px;
        }}
        .summary {{
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
          margin-top: 10px;
        }}
        .summary-grid {{
          display: grid;
          gap: 12px;
          grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
          margin-top: 14px;
        }}
        .stat-card {{
          padding: 16px;
          border-radius: 16px;
          background: rgba(255, 255, 255, 0.78);
          border: 1px solid var(--line);
        }}
        .stat-card strong {{
          display: block;
          font-size: 28px;
          margin-bottom: 4px;
        }}
        .stat-card span {{
          color: var(--muted);
          font-size: 14px;
        }}
        .legend-grid {{
          display: grid;
          gap: 12px;
          grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        }}
        .legend-grid div {{
          padding: 14px;
          border-radius: 14px;
          background: #fcfaf6;
          border: 1px solid var(--line);
        }}
        .legend-grid strong {{
          display: block;
          margin-bottom: 6px;
        }}
        .legend-grid span {{
          color: var(--muted);
          font-size: 14px;
          line-height: 1.5;
        }}
        .finding {{
          border: 1px solid var(--line);
          border-radius: 16px;
          padding: 16px;
          margin-bottom: 14px;
        }}
        .finding.yellow {{ border-color: rgba(185, 128, 20, 0.35); }}
        .finding.red {{ border-color: rgba(179, 58, 48, 0.35); }}
        .finding-head {{
          display: flex;
          flex-wrap: wrap;
          gap: 8px;
          align-items: center;
          margin-bottom: 12px;
        }}
        .pill {{
          padding: 4px 9px;
          border-radius: 999px;
          background: rgba(15, 92, 115, 0.08);
          color: var(--accent);
          font-size: 12px;
        }}
        pre {{
          margin: 0;
          white-space: pre-wrap;
          word-break: break-word;
          padding: 12px;
          background: #fcfaf6;
          border: 1px solid var(--line);
          border-radius: 12px;
          font-family: "SFMono-Regular", Menlo, monospace;
          font-size: 12px;
          line-height: 1.5;
        }}
        .empty {{
          padding: 14px;
          border: 1px dashed var(--line);
          border-radius: 12px;
          color: var(--muted);
        }}
        .error, .notice {{
          margin-top: 18px;
          padding: 14px 16px;
          border-radius: 14px;
          border: 1px solid transparent;
        }}
        .error {{
          background: rgba(179, 58, 48, 0.1);
          color: var(--red);
          border-color: rgba(179, 58, 48, 0.2);
        }}
        .notice {{
          background: rgba(36, 94, 52, 0.1);
          color: var(--notice);
          border-color: rgba(36, 94, 52, 0.2);
        }}
        a {{ color: var(--accent); }}
        code {{
          font-family: "SFMono-Regular", Menlo, monospace;
          font-size: 12px;
        }}
        details summary {{ cursor: pointer; color: var(--accent); }}
      </style>
    </head>
    <body>
      <main class="shell">
        <nav class="topbar">{render_nav(current_nav)}</nav>
        {body}
      </main>
    </body>
    </html>"""


def escape(value: object) -> str:
    return html.escape(str(value))


def google_doc_key(url: str) -> str:
    google_doc_id = parse_google_doc_id(url)
    if google_doc_id:
        return f"google-doc-{google_doc_id}"
    return slugify(url)


def import_files(paths: list[str]) -> None:
    for raw_path in paths:
        path = Path(raw_path).expanduser().resolve()
        if not path.exists():
            print(f"Skipping missing file: {path}", file=sys.stderr)
            continue
        payload = path.read_bytes()
        result = SERVICE.analyze_submission(
            document_key=slugify(path.stem),
            display_name=path.stem,
            source_name=path.name,
            file_bytes=payload,
        )
        document_id = SERVICE.save_result(result)
        print(
            f"Imported #{document_id}: {result.display_name} | template={result.template_name} "
            f"| status={result.status} | unique_score={result.unique_score:.1f}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Template-aware duplicate content checker")
    subparsers = parser.add_subparsers(dest="command")

    serve_parser = subparsers.add_parser("serve", help="Run the local web app")
    default_host = "0.0.0.0" if os.getenv("PORT") else "127.0.0.1"
    default_port = int(os.getenv("PORT", "8765"))
    serve_parser.add_argument("--host", default=default_host)
    serve_parser.add_argument("--port", type=int, default=default_port)

    import_parser = subparsers.add_parser("import", help="Import sample documents into the corpus")
    import_parser.add_argument("paths", nargs="+")

    seo_serve_parser = subparsers.add_parser("seo-serve", help="Run the SEO Brain local web app")
    seo_serve_parser.add_argument("--host", default="127.0.0.1")
    seo_serve_parser.add_argument("--port", type=int, default=8876)

    seo_import_parser = subparsers.add_parser("seo-import", help="Import an SEO workbook into SEO Brain")
    seo_import_parser.add_argument("path")
    seo_import_parser.add_argument("--name", default="")

    args = parser.parse_args()
    if args.command == "import":
        import_files(args.paths)
        return
    if args.command == "seo-import":
        workbook_path = Path(args.path).expanduser().resolve()
        if not workbook_path.exists():
            raise SystemExit(f"Missing workbook: {workbook_path}")
        result = SEO_APP.service.import_workbook(
            workbook_path.name,
            workbook_path.read_bytes(),
            project_name=args.name,
        )
        print(
            f"Imported SEO project #{result['project_id']}: {result['project_name']} | "
            f"pages={result['pages_imported']} | tasks={result['tasks_generated']}"
        )
        return
    if args.command == "seo-serve":
        server = SEO_APP.create_server(args.host, args.port)
        print(f"Serving SEO Brain at http://{args.host}:{args.port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.")
        return

    host = getattr(args, "host", "127.0.0.1")
    port = getattr(args, "port", 8765)
    server = ThreadingHTTPServer((host, port), AppHandler)
    print(f"Serving duplicate checker at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")


if __name__ == "__main__":
    main()
