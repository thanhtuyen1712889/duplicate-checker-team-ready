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
import re
import sys
import traceback
from difflib import SequenceMatcher
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
    def do_HEAD(self) -> None:
        try:
            parsed = urlparse(self.path)
            if parsed.path == "/healthz":
                self.respond_head("text/plain; charset=utf-8")
                return
            if parsed.path == "/":
                self.respond_head("text/html; charset=utf-8")
                return
            if not self.require_auth():
                return
            if parsed.path in {"/document", "/templates", "/export.csv"}:
                content_type = "text/csv; charset=utf-8" if parsed.path == "/export.csv" else "text/html; charset=utf-8"
                self.respond_head(content_type)
                return
            self.send_error(HTTPStatus.NOT_FOUND)
        except Exception:  # noqa: BLE001
            self.handle_uncaught_exception()

    def do_GET(self) -> None:
        try:
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
                self.respond_html(render_document(doc_id, parsed.query))
                return
            if parsed.path == "/templates":
                self.respond_html(render_templates(parsed.query))
                return
            if parsed.path == "/export.csv":
                self.respond_csv(render_export_csv(parsed.query), "duplicate-checker-export.csv")
                return
            self.send_error(HTTPStatus.NOT_FOUND)
        except Exception:  # noqa: BLE001
            self.handle_uncaught_exception()

    def do_POST(self) -> None:
        try:
            if not self.require_auth():
                return
            if self.path == "/submit":
                self.handle_submit()
                return
            if self.path == "/document/delete":
                self.handle_document_delete()
                return
            if self.path == "/templates/create":
                self.handle_template_create()
                return
            self.send_error(HTTPStatus.NOT_FOUND)
        except Exception:  # noqa: BLE001
            self.handle_uncaught_exception()

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

    def handle_document_delete(self) -> None:
        fields, _files = self.parse_form()
        document_id = int((fields.get("document_id") or "0").strip() or 0)
        return_to = (fields.get("return_to") or "/").strip() or "/"
        deleted = SERVICE.storage.delete_document(document_id)
        if not deleted:
            notice = urlencode({"error": "Khong tim thay bai de xoa hoac bai da bi xoa truoc do."})
            self.redirect(f"/?{notice}")
            return
        if deleted.get("restored_document_id"):
            message = (
                f"Da xoa bai {deleted['display_name']}. "
                f"Version truoc do {deleted['restored_display_name']} da duoc kich hoat lai."
            )
        else:
            message = f"Da xoa bai {deleted['display_name']} khoi kho so sanh."
        separator = "&" if "?" in return_to else "?"
        self.redirect(f"{return_to}{separator}{urlencode({'notice': message})}")

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

    def respond_head(self, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def handle_uncaught_exception(self) -> None:
        self.log_error("Unhandled request error:\n%s", traceback.format_exc())
        try:
            self.respond_html(
                page(
                    title="Server error",
                    current_nav="dashboard",
                    body=(
                        '<section class="card"><h1>Server Error</h1>'
                        "<p>Tool vua gap loi noi bo. Vui long quay lai dashboard hoac thu lai sau it phut.</p>"
                        '<p><a href="/">Quay lai dashboard</a></p></section>'
                    ),
                ),
                status=500,
            )
        except BrokenPipeError:
            pass

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
    error = error or (params.get("error", [""])[0] or "").strip()
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
              <th>Nguon</th>
              <th>Trang thai</th>
              <th>Diem unique</th>
              <th>Version</th>
              <th>Thoi gian</th>
              <th>Thao tac</th>
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
          <p>Neu nop bang Google Docs URL, tool se luu snapshot text tai thoi diem check. Ve sau link goc co bi xoa hoac mat quyen truy cap thi ban scan cu van con trong tool.</p>
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


def render_document(document_id: int, query_string: str = "") -> str:
    row = SERVICE.storage.get_document(document_id)
    if not row:
        return page(
            title="Not found",
            current_nav="dashboard",
            body='<section class="card"><p>Khong tim thay bai viet.</p><p><a href="/">Quay lai dashboard</a></p></section>',
        )
    params = parse_qs(query_string)
    parsed = json.loads(row["parsed_json"])
    findings = parsed.get("findings", [])
    sections = parsed.get("sections", {})
    section_risks = parsed.get("section_risks", {})
    focus_section = (params.get("focus_section", [""])[0] or "").strip()
    focus_text = (params.get("focus_text", [""])[0] or "").strip()
    focus_terms = parse_focus_terms(params.get("focus_terms", [""])[0] or "")
    from_document_id = int(params.get("from_document_id", ["0"])[0] or 0)
    from_finding = (params.get("from_finding", [""])[0] or "").strip()
    source_meta = source_meta_from_record(row, parsed)
    priority_cards = render_priority_cards(findings)
    source_focus_card = render_source_focus_card(
        row=row,
        parsed=parsed,
        sections=sections,
        focus_section=focus_section,
        focus_text=focus_text,
        focus_terms=focus_terms,
        from_document_id=from_document_id,
        from_finding=from_finding,
    )
    finding_cards = []
    for index, finding in enumerate(findings[:12], start=1):
        current_section = escape(finding.get("section_name", ""))
        other_section = escape(finding.get("other_section_name", ""))
        scope = escape(finding.get("comparison_scope", "same_section"))
        section_label = current_section
        if scope == "cross_section" and other_section and other_section != current_section:
            section_label = f"{current_section} -> {other_section}"
        reason_label = escape(finding.get("reason_label") or finding.get("rule", "overlap"))
        source_label = escape(finding.get("other_display_name", ""))
        source_section = escape(finding.get("other_section_name", ""))
        finding_anchor = f"finding-{index}"
        source_link = build_source_focus_link(document_id, finding, anchor=f"section-{slugify(str(finding.get('other_section_name') or 'full_text'))}")
        current_excerpt_html = render_highlighted_excerpt(
            finding.get("excerpt", ""),
            finding.get("current_highlight_terms", []),
            variant="current",
        )
        other_excerpt_html = render_highlighted_excerpt(
            finding.get("other_excerpt", ""),
            finding.get("other_highlight_terms", []),
            variant="other",
        )
        finding_cards.append(
            f"""
            <article id="{finding_anchor}" class="finding {escape(finding['severity'])}">
              <div class="finding-head">
                <strong>{section_label}</strong>
                <span class="pill reason">{reason_label}</span>
                <span class="pill">{finding['risk'] * 100:.1f}% risk</span>
                <span class="pill">{scope}</span>
              </div>
              <div class="finding-source-bar">
                <div class="source-brief">
                  <strong>Bai nguon:</strong> {source_label}
                  <span class="muted-inline">| section {source_section or "-"}</span>
                </div>
                <a class="button secondary small" href="{escape(source_link)}">Mo dung section nguon</a>
              </div>
              <div class="grid2">
                <div>
                  <h4>Doan hien tai</h4>
                  <pre>{current_excerpt_html}</pre>
                </div>
                <div>
                  <h4>Doan bi giong</h4>
                  <pre>{other_excerpt_html}</pre>
                </div>
              </div>
            </article>
            """
        )
    if not finding_cards:
        finding_cards.append('<div class="empty">Khong phat hien overlap dang lo voi kho bai hien tai.</div>')

    section_cards = render_section_cards(
        document_id=document_id,
        sections=sections,
        section_risks=section_risks,
        findings=findings,
        focus_section=focus_section,
        focus_text=focus_text,
        focus_terms=focus_terms,
    )
    source_actions = []
    if source_meta["source_url"]:
        source_actions.append(
            f'<a class="button secondary small" target="_blank" rel="noreferrer" href="{escape(source_meta["source_url"])}">Mo link goc</a>'
        )
    source_actions.append(
        render_delete_form(
            document_id=document_id,
            return_to="/",
            label="Xoa bai nay",
            small=True,
        )
    )
    source_note = ""
    if source_meta["kind"] == "google_docs":
        source_note = """
        <p class="source-note">
          Bai nay dang dung snapshot da luu luc import. Neu link Google Docs ve sau bi xoa hoac mat quyen truy cap,
          ket qua hien tai va noi dung da luu van duoc giu trong tool.
        </p>
        """
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
            <span class="metric">Nguon {escape(source_meta['label'])}</span>
          </div>
          <div class="hero-actions">
            {''.join(source_actions)}
          </div>
          {source_note}
        </section>
        <section class="card">
          <h2>Tom Tat Can Sua</h2>
          <p>Uu tien sua theo section truoc. Moi card ben duoi se dan den doan overlap manh nhat ngay trong trang nay.</p>
          {priority_cards}
        </section>
        {source_focus_card}
        <section class="card">
          <h2>Bang Chung Overlap</h2>
          {''.join(finding_cards)}
        </section>
        <section class="card">
          <h2>Chi Tiet Theo Section</h2>
          <p>Mo tung section de xem full text da luu trong tool. Neu ban mo tu mot finding, section lien quan se duoc scroll den va to dam doan match.</p>
          {section_cards}
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
                format_timestamp(row["created_at"]),
            ]
        )
    return buffer.getvalue()


def render_document_row(row) -> str:
    parsed = json.loads(row["parsed_json"])
    source_meta = source_meta_from_record(row, parsed)
    return f"""
    <tr>
      <td><a href="/document?id={row['id']}">{escape(row['display_name'])}</a></td>
      <td>{escape(row['template_name'])}</td>
      <td>{escape(source_meta['short_label'])}</td>
      <td><span class="status {escape(row['status'])}">{escape(row['status']).upper()}</span></td>
      <td>{row['unique_score']:.1f}</td>
      <td>{row['version']}</td>
      <td>{escape(format_timestamp(row['created_at']))}</td>
      <td class="row-actions">{render_delete_form(document_id=int(row["id"]), return_to="/", label="Xoa", small=True)}</td>
    </tr>
    """


def format_timestamp(value) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if value is None:
        return ""
    text = str(value)
    return text[:19].replace("T", " ")


def parse_focus_terms(raw: str) -> list[str]:
    if not raw:
        return []
    return [term.strip() for term in raw.split(",") if term.strip()]


def source_meta_from_record(row, parsed: dict) -> dict[str, str]:
    source_name = str(row["source_name"])
    source_url = str(parsed.get("source_url", "") or "").strip()
    source_kind = str(parsed.get("source_kind", "") or "").strip()
    if not source_kind:
        if source_url or source_name.startswith("google-doc-"):
            source_kind = "google_docs"
        elif source_name.startswith("http://") or source_name.startswith("https://"):
            source_kind = "remote_url"
        elif source_name.endswith(".txt") and source_name == f"{row['display_name']}.txt":
            source_kind = "pasted_text"
        else:
            source_kind = "upload"
    short_label = Path(source_name).name or source_name
    label = short_label
    if source_kind == "google_docs":
        label = "Google Docs snapshot"
        short_label = "Google Docs"
    elif source_kind == "remote_url":
        label = "Remote URL snapshot"
        short_label = "Remote URL"
    elif source_kind == "pasted_text":
        label = "Pasted text snapshot"
        short_label = "Pasted text"
    return {
        "kind": source_kind,
        "label": label,
        "short_label": short_label,
        "source_name": source_name,
        "source_url": source_url,
    }


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


def render_highlighted_excerpt(text: str, terms: list[str], *, variant: str) -> str:
    if not text:
        return ""
    unique_terms = []
    seen = set()
    for term in terms:
        lowered = str(term).lower()
        if not lowered or lowered in seen:
            continue
        seen.add(lowered)
        unique_terms.append(str(term))
    if not unique_terms:
        return escape(text)
    pattern = re.compile("|".join(re.escape(term) for term in sorted(unique_terms, key=len, reverse=True)), re.IGNORECASE)
    parts = []
    cursor = 0
    for match in pattern.finditer(text):
        parts.append(escape(text[cursor:match.start()]))
        parts.append(f'<mark class="overlap-{variant}">{escape(match.group(0))}</mark>')
        cursor = match.end()
    parts.append(escape(text[cursor:]))
    return "".join(parts)


def render_priority_cards(findings: list[dict]) -> str:
    groups: dict[str, dict[str, object]] = {}
    for index, finding in enumerate(findings, start=1):
        section = str(finding.get("section_name", ""))
        group = groups.setdefault(
            section,
            {
                "section": section,
                "max_risk": 0.0,
                "reasons": {},
                "sources": {},
                "first_index": None,
            },
        )
        reason = str(finding.get("reason_label") or finding.get("rule", "overlap"))
        group["max_risk"] = max(float(group["max_risk"]), float(finding.get("risk", 0.0)))
        group["reasons"][reason] = group["reasons"].get(reason, 0) + 1
        source_id = int(finding.get("other_document_id", 0))
        group["sources"][source_id] = str(finding.get("other_display_name", ""))
        if group["first_index"] is None:
            group["first_index"] = index
    if not groups:
        return '<div class="empty">Khong co section nao can sua gap.</div>'
    cards = []
    ordered = sorted(
        groups.values(),
        key=lambda item: (float(item["max_risk"]), len(item["sources"])),
        reverse=True,
    )[:6]
    for item in ordered:
        severity = "red" if float(item["max_risk"]) >= 0.75 else "yellow"
        top_reason = max(item["reasons"].items(), key=lambda pair: pair[1])[0]
        source_names = list(item["sources"].values())[:2]
        source_line = ", ".join(source_names)
        if len(item["sources"]) > 2:
            source_line += f" va {len(item['sources']) - 2} bai khac"
        cards.append(
            f"""
            <article class="hotspot {severity}">
              <strong>{escape(str(item['section']))}</strong>
              <span>{escape(top_reason)}</span>
              <span>{len(item['sources'])} bai bi overlap | max {float(item['max_risk']) * 100:.1f}% risk</span>
              <span class="hotspot-sources">{escape(source_line)}</span>
              <a class="button secondary small" href="#finding-{int(item['first_index'])}">Xem doan can sua</a>
            </article>
            """
        )
    return f'<div class="hotspot-grid">{"".join(cards)}</div>'


def render_delete_form(*, document_id: int, return_to: str, label: str, small: bool = False) -> str:
    button_class = "button danger small" if small else "button danger"
    return f"""
    <form method="post" action="/document/delete" class="inline-form" onsubmit="return confirm('Xoa bai nay khoi kho so sanh?');">
      <input type="hidden" name="document_id" value="{document_id}" />
      <input type="hidden" name="return_to" value="{escape(return_to)}" />
      <button type="submit" class="{button_class}">{escape(label)}</button>
    </form>
    """


def build_source_focus_link(current_document_id: int, finding: dict, *, anchor: str = "") -> str:
    params = {
        "id": int(finding.get("other_document_id", 0)),
        "focus_section": str(finding.get("other_section_name", "")),
        "focus_text": str(finding.get("other_excerpt", "")),
        "focus_terms": ",".join(str(term) for term in finding.get("other_highlight_terms", [])),
        "from_document_id": current_document_id,
        "from_finding": str(finding.get("section_name", "")),
    }
    link = f"/document?{urlencode({key: value for key, value in params.items() if value})}"
    if anchor:
        link += f"#{anchor}"
    return link


def compact_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def compact_compare_text(text: str) -> str:
    lowered = compact_space(text).lower()
    lowered = re.sub(r"[^\w\s£%.-]", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def split_preview_sentences(text: str) -> list[str]:
    return [chunk for chunk in re.split(r"(?<=[.!?])\s+|\n+", compact_space(text)) if chunk]


def preview_windows(sentences: list[str], window: int) -> list[str]:
    if len(sentences) < window:
        return [" ".join(sentences)] if sentences else []
    return [" ".join(sentences[index : index + window]) for index in range(0, len(sentences) - window + 1)]


def best_matching_excerpt(text: str, focus_text: str) -> str:
    normalized = compact_space(text)
    if not normalized:
        return ""
    target = compact_space(focus_text)
    if not target:
        return normalized[:320]
    lower_text = normalized.lower()
    lower_target = target.lower()
    direct_index = lower_text.find(lower_target)
    if direct_index != -1:
        start = max(0, direct_index - 120)
        end = min(len(normalized), direct_index + len(target) + 120)
        return normalized[start:end]
    sentences = split_preview_sentences(normalized)
    best = normalized[:320]
    best_score = 0.0
    target_compare = compact_compare_text(target)
    for window in (4, 3, 2, 1):
        for candidate in preview_windows(sentences, window):
            score = SequenceMatcher(None, compact_compare_text(candidate), target_compare).ratio()
            if score > best_score:
                best_score = score
                best = candidate[:320]
    return best


def render_source_focus_card(
    *,
    row,
    parsed: dict,
    sections: dict[str, dict],
    focus_section: str,
    focus_text: str,
    focus_terms: list[str],
    from_document_id: int,
    from_finding: str,
) -> str:
    if not focus_section and not focus_text:
        return ""
    target_section = sections.get(focus_section, {}) if focus_section else {}
    source_text = str(target_section.get("text", "")) or str(row["raw_text"])
    matched_excerpt = best_matching_excerpt(source_text, focus_text)
    back_link = ""
    if from_document_id:
        back_link = f'<a class="button secondary small" href="/document?id={from_document_id}">Quay lai bai dang sua</a>'
    section_label = escape(focus_section or "full_text")
    matched_html = render_highlighted_excerpt(matched_excerpt, focus_terms, variant="other")
    return f"""
    <section class="card focus-card">
      <div class="card-head">
        <div>
          <h2>Doan Nguon Dang Duoc Doi Chieu</h2>
          <p>Tool da dua ban den dung section nguon va cat ra doan match gan nhat de de doi chieu.</p>
        </div>
        {back_link}
      </div>
      <div class="focus-meta">
        <span class="pill">Section nguon {section_label}</span>
        <span class="pill">Tham chieu tu {escape(from_finding or "-")}</span>
      </div>
      <pre>{matched_html}</pre>
    </section>
    """


def render_section_cards(
    *,
    document_id: int,
    sections: dict[str, dict],
    section_risks: dict[str, float],
    findings: list[dict],
    focus_section: str,
    focus_text: str,
    focus_terms: list[str],
) -> str:
    _ = document_id
    cards = []
    findings_by_section: dict[str, list[dict]] = {}
    for finding in findings:
        findings_by_section.setdefault(str(finding.get("section_name", "")), []).append(finding)
    for name, section in sections.items():
        text = str(section.get("text", ""))
        related = findings_by_section.get(name, [])
        open_attr = " open" if name == focus_section else ""
        section_focus = ""
        if name == focus_section:
            matched_excerpt = best_matching_excerpt(text, focus_text)
            if matched_excerpt:
                section_focus = f"""
                <div class="section-focus">
                  <strong>Doan duoc tham chieu trong section nay</strong>
                  <pre>{render_highlighted_excerpt(matched_excerpt, focus_terms, variant="other")}</pre>
                </div>
                """
        risk_value = float(section_risks.get(name, 0.0))
        badges = [f'<span class="pill">{escape(section.get("mode", ""))}</span>']
        if related:
            badges.append(f'<span class="pill">{len(related)} finding</span>')
        if risk_value:
            badges.append(f'<span class="pill risk">Risk {risk_value:.1f}</span>')
        cards.append(
            f"""
            <details id="section-{slugify(name)}" class="section-card"{open_attr}>
              <summary>
                <span class="section-card-title">{escape(name)}</span>
                <span class="section-card-meta">{''.join(badges)}</span>
              </summary>
              {section_focus}
              <pre class="section-pre">{render_highlighted_excerpt(text, focus_terms if name == focus_section else [], variant="other")}</pre>
            </details>
            """
        )
    return "".join(cards)


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
        button.danger, .button.danger {{
          background: rgba(179, 58, 48, 0.10);
          color: var(--red);
          border: 1px solid rgba(179, 58, 48, 0.22);
        }}
        .button.small {{
          padding: 8px 12px;
          font-size: 12px;
        }}
        .inline-form {{
          display: inline-flex;
          margin: 0;
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
        .hero-actions {{
          display: flex;
          gap: 10px;
          flex-wrap: wrap;
          margin-top: 12px;
        }}
        .source-note {{
          margin-top: 12px;
          max-width: 860px;
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
        .finding-source, .finding-source-bar {{
          display: flex;
          flex-wrap: wrap;
          gap: 10px;
          align-items: center;
          margin-bottom: 12px;
          color: var(--muted);
          font-size: 13px;
          justify-content: space-between;
        }}
        .source-brief {{
          color: var(--muted);
        }}
        .muted-inline {{
          color: var(--muted);
        }}
        .pill {{
          padding: 4px 9px;
          border-radius: 999px;
          background: rgba(15, 92, 115, 0.08);
          color: var(--accent);
          font-size: 12px;
        }}
        .pill.reason {{
          background: rgba(185, 128, 20, 0.12);
          color: var(--yellow);
        }}
        .pill.risk {{
          background: rgba(179, 58, 48, 0.08);
          color: var(--red);
        }}
        .hotspot-grid {{
          display: grid;
          gap: 12px;
          grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
          margin-bottom: 16px;
        }}
        .hotspot {{
          padding: 14px;
          border-radius: 14px;
          border: 1px solid var(--line);
          background: #fcfaf6;
        }}
        .hotspot.red {{ border-color: rgba(179, 58, 48, 0.35); }}
        .hotspot.yellow {{ border-color: rgba(185, 128, 20, 0.35); }}
        .hotspot strong {{
          display: block;
          margin-bottom: 6px;
        }}
        .hotspot span {{
          display: block;
          color: var(--muted);
          font-size: 13px;
          line-height: 1.5;
        }}
        .hotspot-sources {{
          min-height: 40px;
          margin-bottom: 10px;
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
        mark.overlap-current {{
          background: rgba(185, 128, 20, 0.22);
          color: inherit;
          padding: 0 2px;
          border-radius: 4px;
        }}
        mark.overlap-other {{
          background: rgba(15, 92, 115, 0.18);
          color: inherit;
          padding: 0 2px;
          border-radius: 4px;
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
        .section-card {{
          border: 1px solid var(--line);
          border-radius: 16px;
          padding: 14px 16px;
          background: #fff;
          margin-top: 12px;
        }}
        .section-card summary {{
          cursor: pointer;
          color: var(--ink);
          display: flex;
          gap: 12px;
          justify-content: space-between;
          align-items: center;
          list-style: none;
        }}
        .section-card summary::-webkit-details-marker {{
          display: none;
        }}
        .section-card-title {{
          font-size: 22px;
          color: var(--ink);
        }}
        .section-card-meta {{
          display: flex;
          gap: 8px;
          flex-wrap: wrap;
          justify-content: flex-end;
        }}
        .section-focus {{
          margin: 14px 0 12px;
        }}
        .section-focus strong {{
          display: block;
          margin-bottom: 8px;
        }}
        .section-pre {{
          margin-top: 12px;
          max-height: 420px;
          overflow: auto;
        }}
        .focus-meta {{
          display: flex;
          gap: 8px;
          flex-wrap: wrap;
          margin-bottom: 12px;
        }}
        .row-actions {{
          white-space: nowrap;
        }}
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
