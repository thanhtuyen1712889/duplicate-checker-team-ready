#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from smart_duplicate_core import RESULT_LEVELS, SmartDuplicateService, fetch_google_doc_text

FASTAPI_IMPORT_ERROR: Exception | None = None
try:
    from fastapi import Body, FastAPI, File, HTTPException, Query, UploadFile
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import HTMLResponse, JSONResponse, Response
except ModuleNotFoundError as exc:  # pragma: no cover - depends on environment
    FASTAPI_IMPORT_ERROR = exc
    FastAPI = None
    Body = None
    File = None
    HTTPException = RuntimeError
    Query = None
    UploadFile = None
    HTMLResponse = None
    JSONResponse = None
    Response = None

try:
    import uvicorn
except ModuleNotFoundError:  # pragma: no cover - depends on environment
    uvicorn = None


ROOT = Path(__file__).resolve().parent
SERVICE = SmartDuplicateService()


def api_error(message: str, status_code: int = 400) -> HTTPException:
    return HTTPException(status_code=status_code, detail=message)


def parse_payload(payload: dict[str, Any]) -> tuple[str, str, str]:
    title = (payload.get("title") or "").strip()
    google_doc_url = (payload.get("google_doc_url") or "").strip()
    raw_text = (payload.get("raw_text") or "").strip()
    return title, google_doc_url, raw_text


def result_summary(results: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"conflict": 0, "review": 0, "ok": 0}
    for item in results:
        level = item["level"]
        if level == "conflict":
            counts["conflict"] += 1
        elif level == "review":
            counts["review"] += 1
        else:
            counts["ok"] += 1
    return counts


def comparison_count(doc_count: int) -> int:
    if doc_count < 2:
        return 0
    return int(doc_count * (doc_count - 1) / 2)


if FastAPI is not None:
    app = FastAPI(
        title="Bộ kiểm tra trùng lặp nội dung",
        version="1.0.0",
        description="Ứng dụng kiểm tra trùng lặp thông minh cho nội dung trang sản phẩm.",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/", response_class=HTMLResponse)
    async def index() -> HTMLResponse:
        return HTMLResponse(render_spa())

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.head("/healthz")
    async def healthz_head() -> Response:
        return Response(status_code=200)

    @app.post("/api/project/create")
    async def create_project(payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
        name = (payload.get("name") or "").strip()
        if not name:
            raise api_error("Vui long nhap ten project")
        project = SERVICE.create_project(name)
        return {"project": project}

    @app.get("/api/project/list")
    async def list_projects() -> dict[str, Any]:
        projects = SERVICE.list_projects()
        return {"projects": projects}

    @app.post("/api/project/import")
    async def import_project(file: UploadFile = File(...)) -> dict[str, Any]:
        try:
            payload = await file.read()
            project = SERVICE.import_project_zip(payload)
            return {"project": project}
        except ValueError as exc:
            raise api_error(str(exc)) from exc

    @app.post("/api/project/{project_id}/template")
    async def upload_template(project_id: int, payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
        try:
            title, google_doc_url, raw_text = parse_payload(payload)
            if google_doc_url:
                text, warnings = fetch_google_doc_text(google_doc_url)
            else:
                text = raw_text
                warnings = []
            response = SERVICE.set_template_from_text(project_id, text, title=title)
            if payload.get("allowed_zone_override"):
                merged = response["allowed_zone"]
                override = payload["allowed_zone_override"]
                merged.update(override)
                SERVICE.update_allowed_zone(project_id, merged, recheck_all=False)
                response["allowed_zone"] = merged
            response["warnings"] = warnings + response.get("warnings", [])
            return response
        except ValueError as exc:
            raise api_error(str(exc)) from exc

    @app.post("/api/project/{project_id}/allowed-zone")
    async def update_allowed_zone(project_id: int, payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
        try:
            project = SERVICE.update_allowed_zone(project_id, payload.get("config") or {}, recheck_all=bool(payload.get("recheck_all")))
            return {"project": project}
        except ValueError as exc:
            raise api_error(str(exc)) from exc

    @app.post("/api/project/{project_id}/add-doc")
    async def add_doc(project_id: int, payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
        try:
            title, google_doc_url, raw_text = parse_payload(payload)
            if google_doc_url:
                response = SERVICE.add_document_from_url(project_id, google_doc_url, title=title)
            elif raw_text:
                response = SERVICE.add_document_from_text(project_id, title, raw_text)
            else:
                raise api_error("Vui long dan Google Doc URL hoac raw text")
            if response.get("queued"):
                return response
            doc_results = SERVICE.project_results(project_id, doc_id=int(response["document_id"]))
            response["summary"] = result_summary(doc_results)
            response["results"] = doc_results
            return response
        except ValueError as exc:
            raise api_error(str(exc)) from exc

    @app.get("/api/project/{project_id}/docs")
    async def list_docs(project_id: int) -> dict[str, Any]:
        try:
            docs = SERVICE.list_documents(project_id)
            return {"documents": docs}
        except ValueError as exc:
            raise api_error(str(exc), status_code=404) from exc

    @app.get("/api/project/{project_id}/results")
    async def get_results(
        project_id: int,
        doc_id: int | None = Query(default=None),
        level: str = Query(default=""),
        section: str = Query(default=""),
        other_doc_id: int | None = Query(default=None),
        search: str = Query(default=""),
        sort_by: str = Query(default="risk"),
    ) -> dict[str, Any]:
        try:
            results = SERVICE.project_results(
                project_id,
                doc_id=doc_id,
                level=level,
                section=section,
                other_doc_id=other_doc_id,
                search=search,
                sort_by=sort_by,
            )
            docs = SERVICE.list_documents(project_id)
            return {
                "results": results,
                "summary": result_summary(results),
                "doc_count": len(docs),
                "comparison_count": comparison_count(len(docs)),
            }
        except ValueError as exc:
            raise api_error(str(exc), status_code=404) from exc

    @app.get("/api/project/{project_id}/export")
    async def export_project(project_id: int, export_format: str = Query(default="xlsx", alias="format")) -> Response:
        try:
            if export_format == "zip":
                payload = SERVICE.export_project_zip(project_id)
                headers = {"Content-Disposition": 'attachment; filename="project-backup.zip"'}
                return Response(payload, media_type="application/zip", headers=headers)
            payload = SERVICE.export_excel(project_id)
            headers = {"Content-Disposition": 'attachment; filename="duplicate-report.xlsx"'}
            return Response(
                payload,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers=headers,
            )
        except ValueError as exc:
            raise api_error(str(exc), status_code=404) from exc
        except RuntimeError as exc:
            raise api_error(str(exc), status_code=500) from exc

    @app.delete("/api/project/{project_id}/delete")
    async def delete_project(project_id: int) -> dict[str, Any]:
        SERVICE.delete_project(project_id)
        return {"ok": True}

    @app.post("/api/project/{project_id}/rename")
    async def rename_project(project_id: int, payload: dict[str, Any] = Body(default={})) -> dict[str, Any]:
        try:
            project = SERVICE.rename_project(project_id, payload.get("name") or "")
            return {"project": project}
        except ValueError as exc:
            raise api_error(str(exc)) from exc

    @app.post("/api/project/{project_id}/duplicate")
    async def duplicate_project(project_id: int) -> dict[str, Any]:
        try:
            project = SERVICE.duplicate_project(project_id)
            return {"project": project}
        except ValueError as exc:
            raise api_error(str(exc)) from exc

    @app.get("/api/project/{project_id}/status")
    async def project_status(project_id: int) -> dict[str, Any]:
        return {"job": SERVICE.get_job(project_id)}

    @app.delete("/api/document/{document_id}")
    async def delete_document(document_id: int) -> dict[str, Any]:
        SERVICE.remove_document(document_id)
        return {"ok": True}

    @app.post("/api/document/{document_id}/recheck")
    async def recheck_document(document_id: int) -> dict[str, Any]:
        try:
            results = SERVICE.recheck_document(document_id)
            return {"results": results}
        except ValueError as exc:
            raise api_error(str(exc)) from exc
else:  # pragma: no cover - used only when dependencies are missing
    app = None


def render_spa() -> str:
    return """<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Bộ kiểm tra trùng lặp nội dung</title>
  <style>
    :root {
      --bg: #f5f1e8;
      --panel: #fffdf9;
      --ink: #201a16;
      --muted: #6e6258;
      --line: #ddcfbf;
      --brand: #1f6d78;
      --red: #E53935;
      --amber: #F9A825;
      --green: #43A047;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, #ead8ba 0%, transparent 22%),
        linear-gradient(180deg, #faf7f1 0%, var(--bg) 100%);
      font-family: "Georgia", "Times New Roman", serif;
    }
    .app {
      display: grid;
      grid-template-columns: 320px minmax(0, 1fr);
      min-height: 100vh;
    }
    .sidebar {
      border-right: 1px solid var(--line);
      padding: 22px 18px;
      background: rgba(255,255,255,0.76);
      backdrop-filter: blur(10px);
    }
    .main {
      padding: 24px;
    }
    h1, h2, h3 {
      margin: 0 0 14px;
      line-height: 1.12;
    }
    p {
      margin: 0 0 12px;
      color: var(--muted);
      line-height: 1.55;
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
      box-shadow: 0 10px 28px rgba(32, 26, 22, 0.06);
    }
    .stack {
      display: grid;
      gap: 14px;
    }
    .tabs {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 16px;
    }
    .tab-button, button, .link-button {
      border: 1px solid transparent;
      border-radius: 12px;
      padding: 10px 14px;
      background: var(--brand);
      color: #fff;
      font: inherit;
      cursor: pointer;
      text-decoration: none;
    }
    .tab-button.secondary, .link-button.secondary, button.secondary {
      background: rgba(31,109,120,0.08);
      color: var(--brand);
      border-color: rgba(31,109,120,0.16);
    }
    .tab-button.danger, button.danger {
      background: rgba(229,57,53,0.12);
      color: var(--red);
      border-color: rgba(229,57,53,0.22);
    }
    .tab-button.active {
      background: #123c43;
    }
    input, textarea, select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 12px 14px;
      font: inherit;
      background: #fff;
    }
    label {
      display: grid;
      gap: 8px;
      font-size: 14px;
    }
    .grid2 {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    }
    .grid3 {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    }
    .project-list {
      display: grid;
      gap: 10px;
      margin-top: 14px;
    }
    .project-item {
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      background: rgba(255,255,255,0.75);
      cursor: pointer;
    }
    .project-item.active {
      border-color: var(--brand);
      box-shadow: inset 0 0 0 1px rgba(31,109,120,0.16);
    }
    .project-meta {
      font-size: 13px;
      color: var(--muted);
      line-height: 1.5;
    }
    .toolbar {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 10px;
    }
    .banner {
      border-radius: 14px;
      padding: 14px 16px;
      margin-bottom: 14px;
      border: 1px solid transparent;
    }
    .banner.info {
      background: rgba(31,109,120,0.08);
      border-color: rgba(31,109,120,0.16);
      color: var(--brand);
    }
    .banner.error {
      background: rgba(229,57,53,0.10);
      border-color: rgba(229,57,53,0.22);
      color: var(--red);
    }
    .banner.success {
      background: rgba(67,160,71,0.10);
      border-color: rgba(67,160,71,0.22);
      color: var(--green);
    }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(31,109,120,0.08);
      color: var(--brand);
      font-size: 13px;
    }
    .badge.red { background: rgba(229,57,53,0.12); color: var(--red); }
    .badge.amber { background: rgba(249,168,37,0.14); color: var(--amber); }
    .badge.green { background: rgba(67,160,71,0.14); color: var(--green); }
    .summary-row {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 12px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    th, td {
      text-align: left;
      vertical-align: top;
      padding: 10px 8px;
      border-top: 1px solid var(--line);
      font-size: 14px;
    }
    th {
      color: var(--muted);
      font-weight: 600;
    }
    .risk-bar {
      height: 8px;
      border-radius: 999px;
      background: rgba(0,0,0,0.06);
      overflow: hidden;
      margin-top: 6px;
    }
    .risk-fill {
      height: 100%;
      border-radius: 999px;
    }
    .risk-fill.conflict { background: var(--red); }
    .risk-fill.review { background: var(--amber); }
    .risk-fill.ok, .risk-fill.allowed, .risk-fill.skip { background: var(--green); }
    .split {
      display: grid;
      gap: 16px;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    }
    pre {
      white-space: pre-wrap;
      word-break: break-word;
      margin: 0;
      padding: 14px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: #fcfaf6;
      font-size: 13px;
      line-height: 1.55;
      font-family: "SFMono-Regular", Menlo, monospace;
      max-height: 420px;
      overflow: auto;
    }
    mark.conflict { background: rgba(229,57,53,0.18); }
    mark.review { background: rgba(249,168,37,0.22); }
    mark.ok { background: rgba(67,160,71,0.18); }
    .empty-state {
      display: grid;
      gap: 10px;
      padding: 18px;
      border-radius: 18px;
      border: 1px dashed var(--line);
      background: rgba(255,255,255,0.42);
    }
    .hidden { display: none !important; }
    .filters {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      margin-bottom: 12px;
      align-items: end;
    }
    .pill-list {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 8px;
    }
    .pill {
      padding: 6px 10px;
      border-radius: 999px;
      background: rgba(31,109,120,0.08);
      color: var(--brand);
      font-size: 13px;
    }
    @media (max-width: 980px) {
      .app { grid-template-columns: 1fr; }
      .sidebar { border-right: 0; border-bottom: 1px solid var(--line); }
    }
  </style>
</head>
<body>
  <div class="app">
    <aside class="sidebar stack">
      <div>
        <h1>Kiểm tra trùng lặp thông minh</h1>
        <p>Bắt đầu: Tạo dự án → Thêm bài mẫu → Thêm bài cần check.</p>
      </div>
      <div class="card stack">
        <h3>Tạo dự án mới</h3>
        <label>Tên dự án
          <input id="new-project-name" placeholder="Ví dụ: PandaPak Bowl Team" />
        </label>
        <button title="Tạo dự án mới" onclick="createProject()">Tạo dự án</button>
      </div>
      <div class="card">
        <h3>Danh sách dự án</h3>
        <div id="project-list" class="project-list"></div>
      </div>
      <div class="card stack">
        <h3>Khôi phục từ ZIP</h3>
        <label>Chọn file sao lưu ZIP
          <input id="import-project-file" type="file" accept=".zip" />
        </label>
        <button class="secondary" title="Khôi phục dự án từ file ZIP" onclick="importProject()">Khôi phục dự án</button>
      </div>
    </aside>
    <main class="main stack">
      <div id="global-banner"></div>
      <section id="project-empty" class="empty-state hidden">
        <h2>Chưa chọn dự án</h2>
        <p>Hãy chọn một dự án ở cột trái hoặc tạo dự án mới để bắt đầu.</p>
      </section>
      <section id="project-shell" class="stack hidden">
        <div class="card">
          <div class="summary-row">
            <span id="project-name-badge" class="badge"></span>
            <span id="project-template-badge" class="badge"></span>
            <span id="project-doc-badge" class="badge"></span>
            <span id="project-conflict-badge" class="badge red"></span>
          </div>
          <div class="toolbar">
            <button class="secondary" title="Đổi tên dự án" onclick="renameProject()">Đổi tên</button>
            <button class="secondary" title="Nhân bản toàn bộ dự án" onclick="duplicateProject()">Nhân bản</button>
            <button class="danger" title="Xóa toàn bộ dự án" onclick="deleteProject()">Xóa dự án</button>
          </div>
        </div>
        <div class="tabs">
          <button class="tab-button active" data-tab="setup" onclick="switchTab('setup')">Thiết lập</button>
          <button class="tab-button secondary" data-tab="add-doc" onclick="switchTab('add-doc')">Thêm bài mới</button>
          <button class="tab-button secondary" data-tab="results" onclick="switchTab('results')">Kết quả</button>
          <button class="tab-button secondary" data-tab="export" onclick="switchTab('export')">Xuất báo cáo</button>
        </div>

        <section id="tab-setup" class="stack">
          <div class="card stack">
            <h2>Bài mẫu và vùng được phép trùng</h2>
            <p>Thêm bài mẫu để hệ thống tự phát hiện phần được phép trùng, ngưỡng theo section và câu boilerplate.</p>
            <div class="grid2">
              <label>Tên bài mẫu (tùy chọn)
                <input id="template-title" placeholder="Mẫu PandaPak Bowl" />
              </label>
              <label>Link Google Docs
                <input id="template-url" placeholder="https://docs.google.com/document/d/..." />
              </label>
            </div>
            <label>Hoặc dán nội dung bài mẫu
              <textarea id="template-text" rows="10" placeholder="Dán nội dung bài mẫu vào đây"></textarea>
            </label>
            <button title="Lưu bài mẫu và phân tích vùng được phép trùng" onclick="saveTemplate()">Lưu bài mẫu</button>
            <div id="template-result"></div>
          </div>
          <div id="allowed-zone-editor" class="card hidden stack">
            <h2>Vùng được phép trùng đã phát hiện</h2>
            <div id="allowed-zone-summary" class="summary-row"></div>
            <div class="grid2">
              <label>Cụm từ được phép trùng (mỗi dòng 1 cụm)
                <textarea id="allowed-phrases" rows="10"></textarea>
              </label>
              <label>Mục bỏ qua hoàn toàn (mỗi dòng 1 mục)
                <textarea id="allowed-sections" rows="10"></textarea>
              </label>
            </div>
            <label>Regex được phép trùng (JSON)
              <textarea id="allowed-patterns" rows="10"></textarea>
            </label>
            <label>Ngưỡng theo section (JSON)
              <textarea id="allowed-thresholds" rows="10"></textarea>
            </label>
            <button title="Lưu cấu hình vùng được phép trùng" onclick="saveAllowedZone()">Lưu cấu hình</button>
          </div>
        </section>

        <section id="tab-add-doc" class="stack hidden">
          <div class="card stack">
            <h2>Thêm & Check ngay</h2>
            <p>Dùng link Google Docs là luồng chính. Nếu bài đang private hoặc export bị chặn, hãy dán trực tiếp nội dung bài.</p>
            <div class="grid2">
              <label>Tiêu đề bài (tùy chọn)
                <input id="doc-title" placeholder="Kraft Round Bowl 900ml" />
              </label>
              <label>Link Google Docs
                <input id="doc-url" placeholder="https://docs.google.com/document/d/..." />
              </label>
            </div>
            <label>Hoặc dán nội dung bài
              <textarea id="doc-text" rows="12" placeholder="Dán bài cần check vào đây"></textarea>
            </label>
            <div class="toolbar">
              <button title="Thêm bài và kiểm tra ngay" onclick="addDocument()">Thêm & Check ngay</button>
              <button class="secondary" title="Xóa nội dung đang nhập" onclick="clearDocForm()">Xóa nội dung</button>
            </div>
            <div id="doc-progress"></div>
          </div>
          <div id="new-doc-inline-results" class="stack"></div>
        </section>

        <section id="tab-results" class="stack hidden">
          <div class="card">
            <h2>Kết quả</h2>
            <div id="results-summary" class="summary-row"></div>
            <div id="project-progress-banner"></div>
            <div class="filters">
              <label>Mức độ
                <select id="filter-level">
                  <option value="">Tất cả</option>
                  <option value="conflict">🔴 Xung đột</option>
                  <option value="review">🟡 Xem lại</option>
                  <option value="ok">✅ Ổn</option>
                </select>
              </label>
              <label>Bài xung đột
                <select id="filter-other-doc">
                  <option value="">Tất cả bài</option>
                </select>
              </label>
              <label>Mục
                <input id="filter-section" placeholder="features, faq..." />
              </label>
              <label>Tìm keyword
                <input id="filter-search" placeholder="keyword trong câu trùng" />
              </label>
              <label>Sắp xếp theo
                <select id="filter-sort">
                  <option value="risk">Điểm rủi ro</option>
                  <option value="section">Mục</option>
                <option value="doc">Bài</option>
              </select>
            </label>
              <button title="Áp dụng bộ lọc" onclick="loadResults()">Lọc</button>
            </div>
            <div id="results-table-wrap"></div>
          </div>
          <div id="result-detail"></div>
        </section>

        <section id="tab-export" class="stack hidden">
          <div class="card stack">
            <h2>Xuất báo cáo</h2>
            <p>Xuất file Excel 3 sheet hoặc gói sao lưu ZIP toàn bộ dữ liệu của dự án.</p>
            <div class="toolbar">
              <a class="link-button" id="export-xlsx-link" href="#" target="_blank">Tải Excel (.xlsx)</a>
              <a class="link-button secondary" id="export-zip-link" href="#" target="_blank">Tải sao lưu ZIP</a>
            </div>
          </div>
          <div class="card">
            <h2>Danh sách bài trong dự án</h2>
            <div id="doc-list-wrap"></div>
          </div>
        </section>
      </section>
    </main>
  </div>
  <script>
    const state = {
      projects: [],
      documents: [],
      currentProject: null,
      currentResults: [],
      lastAddedDocId: null,
    };

    function escapeHtml(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    function setBanner(message, kind = "info") {
      const root = document.getElementById("global-banner");
      if (!message) {
        root.innerHTML = "";
        return;
      }
      root.innerHTML = `<div class="banner ${kind}">${escapeHtml(message)}</div>`;
    }

    async function api(url, options = {}) {
      const response = await fetch(url, {
        headers: { "Content-Type": "application/json" },
        ...options,
      });
      if (!response.ok) {
        let message = "Co loi xay ra";
        try {
          const payload = await response.json();
          message = payload.detail || message;
        } catch (_error) {
          const text = await response.text();
          if (text) message = text;
        }
        throw new Error(message);
      }
      const contentType = response.headers.get("content-type") || "";
      if (contentType.includes("application/json")) {
        return response.json();
      }
      return response.text();
    }

    function formatDateTime(value) {
      if (!value) return "-";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      return date.toLocaleString("vi-VN");
    }

    function switchTab(tabName) {
      document.querySelectorAll(".tab-button").forEach((button) => {
        const active = button.dataset.tab === tabName;
        button.classList.toggle("active", active);
        button.classList.toggle("secondary", !active);
      });
      document.querySelectorAll("[id^='tab-']").forEach((node) => {
        node.classList.toggle("hidden", node.id !== `tab-${tabName}`);
      });
    }

    function riskBadge(level, score) {
      const css = level === "conflict" ? "red" : level === "review" ? "amber" : "green";
      const label = level === "conflict" ? "🔴 Xung đột" : level === "review" ? "🟡 Xem lại" : "✅ Ổn";
      return `<span class="badge ${css}">${label} · ${Number(score).toFixed(1)}%</span>`;
    }

    function riskBar(level, score) {
      return `<div class="risk-bar"><div class="risk-fill ${level}" style="width:${Math.min(100, Number(score))}%"></div></div>`;
    }

    function semanticModeLabel(mode) {
      if (mode === "semantic") return "mô hình tiếng Anh";
      if (mode === "semantic_only_multilingual") return "mô hình đa ngôn ngữ";
      if (mode === "fallback") return "chế độ cục bộ";
      return mode || "-";
    }

    function commonHighlight(text, reference, level) {
      const words = Array.from(new Set(reference.toLowerCase().match(/[a-z0-9]+/g) || []))
        .filter((word) => word.length > 3)
        .slice(0, 12);
      let output = escapeHtml(text);
      for (const word of words) {
        const pattern = new RegExp(`\\\\b(${word})\\\\b`, "gi");
        output = output.replace(pattern, `<mark class="${level}">$1</mark>`);
      }
      return output;
    }

    async function loadProjects(selectId = null) {
      try {
        const payload = await api("/api/project/list");
        state.projects = payload.projects;
        renderProjectList();
        if (selectId) {
          selectProject(selectId);
          return;
        }
        if (!state.currentProject && state.projects.length) {
          selectProject(state.projects[0].id);
        } else if (state.currentProject) {
          const updated = state.projects.find((project) => project.id === state.currentProject.id);
          if (updated) {
            state.currentProject = updated;
            renderCurrentProject();
          }
        }
      } catch (error) {
        setBanner(error.message, "error");
      }
    }

    function renderProjectList() {
      const root = document.getElementById("project-list");
      if (!state.projects.length) {
        root.innerHTML = `<div class="empty-state"><strong>Chưa có dự án</strong><span>Hãy tạo dự án đầu tiên để bắt đầu.</span></div>`;
        return;
      }
      root.innerHTML = state.projects.map((project) => `
        <div class="project-item ${state.currentProject && state.currentProject.id === project.id ? "active" : ""}" onclick="selectProject(${project.id})">
          <strong>${escapeHtml(project.name)}</strong>
          <div class="project-meta">
            Bài mẫu: ${escapeHtml(project.template_name)}<br/>
            ${project.doc_count} bài · ${project.conflict_count} xung đột<br/>
            Cập nhật: ${escapeHtml(formatDateTime(project.updated_at))}
          </div>
        </div>
      `).join("");
    }

    async function createProject() {
      const input = document.getElementById("new-project-name");
      try {
        const payload = await api("/api/project/create", {
          method: "POST",
          body: JSON.stringify({ name: input.value }),
        });
        input.value = "";
        setBanner("Đã tạo dự án mới", "success");
        await loadProjects(payload.project.id);
      } catch (error) {
        setBanner(error.message, "error");
      }
    }

    async function importProject() {
      const fileInput = document.getElementById("import-project-file");
      const file = fileInput.files?.[0];
      if (!file) {
        setBanner("Vui lòng chọn file ZIP để khôi phục", "error");
        return;
      }
      const formData = new FormData();
      formData.append("file", file);
      try {
        const response = await fetch("/api/project/import", { method: "POST", body: formData });
        if (!response.ok) {
          const payload = await response.json().catch(() => ({}));
          throw new Error(payload.detail || "Không thể khôi phục bản sao lưu");
        }
        const payload = await response.json();
        fileInput.value = "";
        setBanner("Đã khôi phục dự án từ file sao lưu ZIP", "success");
        await loadProjects(payload.project.id);
      } catch (error) {
        setBanner(error.message, "error");
      }
    }

    function selectProject(projectId) {
      const project = state.projects.find((item) => item.id === projectId);
      if (!project) return;
      state.currentProject = project;
      renderProjectList();
      renderCurrentProject();
      loadDocs();
      loadResults();
    }

    function renderCurrentProject() {
      const empty = document.getElementById("project-empty");
      const shell = document.getElementById("project-shell");
      if (!state.currentProject) {
        empty.classList.remove("hidden");
        shell.classList.add("hidden");
        return;
      }
      empty.classList.add("hidden");
      shell.classList.remove("hidden");
      document.getElementById("project-name-badge").textContent = state.currentProject.name;
      document.getElementById("project-template-badge").textContent = `Bài mẫu: ${state.currentProject.template_name}`;
      document.getElementById("project-doc-badge").textContent = `${state.currentProject.doc_count} bài`;
      document.getElementById("project-conflict-badge").textContent = `${state.currentProject.conflict_count} xung đột`;
      document.getElementById("export-xlsx-link").href = `/api/project/${state.currentProject.id}/export?format=xlsx`;
      document.getElementById("export-zip-link").href = `/api/project/${state.currentProject.id}/export?format=zip`;
      hydrateAllowedZoneEditor();
    }

    function hydrateAllowedZoneEditor() {
      const editor = document.getElementById("allowed-zone-editor");
      const config = state.currentProject?.allowed_zone_config || {};
      if (!config.allowed_patterns_regex && !state.currentProject?.has_template) {
        editor.classList.add("hidden");
        return;
      }
      editor.classList.remove("hidden");
      const patterns = config.allowed_patterns_regex || [];
      document.getElementById("allowed-zone-summary").innerHTML = `
        <span class="badge">${patterns.length} regex cho phép</span>
        <span class="badge">${(config.allowed_phrases || []).length} cụm cho phép</span>
        <span class="badge">${(config.fully_allowed_sections || []).length} section bỏ qua</span>
      `;
      document.getElementById("allowed-phrases").value = (config.allowed_phrases || []).join("\\n");
      document.getElementById("allowed-sections").value = (config.fully_allowed_sections || []).join("\\n");
      document.getElementById("allowed-patterns").value = JSON.stringify(patterns, null, 2);
      document.getElementById("allowed-thresholds").value = JSON.stringify(config.section_thresholds || {}, null, 2);
    }

    async function saveTemplate() {
      if (!state.currentProject) return;
      const target = document.getElementById("template-result");
      target.innerHTML = `<div class="banner info">Đang phân tích bài mẫu...</div>`;
      try {
        const payload = await api(`/api/project/${state.currentProject.id}/template`, {
          method: "POST",
          body: JSON.stringify({
            title: document.getElementById("template-title").value,
            google_doc_url: document.getElementById("template-url").value,
            raw_text: document.getElementById("template-text").value,
          }),
        });
        target.innerHTML = `
          <div class="banner success">
            Đã lưu bài mẫu: ${escapeHtml(payload.template_title || "Bài mẫu")}
            ${(payload.warnings || []).length ? `<div class="pill-list">${payload.warnings.map((item) => `<span class="pill">${escapeHtml(item)}</span>`).join("")}</div>` : ""}
          </div>
        `;
        await loadProjects(state.currentProject.id);
      } catch (error) {
        target.innerHTML = `<div class="banner error">${escapeHtml(error.message)}</div>`;
      }
    }

    async function saveAllowedZone() {
      if (!state.currentProject) return;
      try {
        const config = {
          allowed_phrases: document.getElementById("allowed-phrases").value.split("\\n").map((item) => item.trim()).filter(Boolean),
          fully_allowed_sections: document.getElementById("allowed-sections").value.split("\\n").map((item) => item.trim()).filter(Boolean),
          allowed_patterns_regex: JSON.parse(document.getElementById("allowed-patterns").value || "[]"),
          section_thresholds: JSON.parse(document.getElementById("allowed-thresholds").value || "{}"),
        };
        await api(`/api/project/${state.currentProject.id}/allowed-zone`, {
          method: "POST",
          body: JSON.stringify({ config, recheck_all: true }),
        });
        setBanner("Đã lưu vùng được phép trùng và chạy lại toàn dự án", "success");
        await loadProjects(state.currentProject.id);
        await loadResults();
      } catch (error) {
        setBanner(error.message, "error");
      }
    }

    function clearDocForm() {
      document.getElementById("doc-title").value = "";
      document.getElementById("doc-url").value = "";
      document.getElementById("doc-text").value = "";
      document.getElementById("doc-progress").innerHTML = "";
    }

    async function addDocument() {
      if (!state.currentProject) return;
      const progress = document.getElementById("doc-progress");
      progress.innerHTML = `<div class="banner info">Đang tải bài... → Đang phân tích cấu trúc... → Đang so sánh...</div>`;
      document.getElementById("new-doc-inline-results").innerHTML = "";
      try {
        const payload = await api(`/api/project/${state.currentProject.id}/add-doc`, {
          method: "POST",
          body: JSON.stringify({
            title: document.getElementById("doc-title").value,
            google_doc_url: document.getElementById("doc-url").value,
            raw_text: document.getElementById("doc-text").value,
          }),
        });
        state.lastAddedDocId = payload.document_id;
        if (payload.queued) {
          progress.innerHTML = `<div class="banner info">${escapeHtml(payload.message)}</div>`;
          await pollProjectStatus();
          await loadResults();
          await loadDocs();
          switchTab("results");
          return;
        }
        progress.innerHTML = `<div class="banner success">Đã thêm bài và kiểm tra xong</div>`;
        renderInlineResults(payload);
        await loadProjects(state.currentProject.id);
        await loadResults(payload.document_id);
        await loadDocs();
        switchTab("results");
      } catch (error) {
        progress.innerHTML = `<div class="banner error">${escapeHtml(error.message)}</div>`;
      }
    }

    async function pollProjectStatus() {
      if (!state.currentProject) return;
      const bannerRoot = document.getElementById("project-progress-banner");
      for (let index = 0; index < 40; index += 1) {
        const payload = await api(`/api/project/${state.currentProject.id}/status`);
        const job = payload.job;
        if (job.status === "done") {
          bannerRoot.innerHTML = `<div class="banner success">${escapeHtml(job.message || "Đã so sánh xong")}</div>`;
          return;
        }
        if (job.status === "error") {
          bannerRoot.innerHTML = `<div class="banner error">${escapeHtml(job.message || "Có lỗi khi so sánh")}</div>`;
          return;
        }
        if (job.status === "running") {
          bannerRoot.innerHTML = `<div class="banner info">${escapeHtml(job.message)} — Đã check ${job.progress_current}/${job.progress_total} bài</div>`;
        }
        await new Promise((resolve) => setTimeout(resolve, 1200));
      }
    }

    function renderInlineResults(payload) {
      const root = document.getElementById("new-doc-inline-results");
      const summary = payload.summary || { conflict: 0, review: 0, ok: 0 };
      root.innerHTML = `
        <div class="card">
          <div class="summary-row">
            <span class="badge red">${summary.conflict} xung đột nghiêm trọng</span>
            <span class="badge amber">${summary.review} cần xem lại</span>
            <span class="badge green">${summary.ok} mục ổn</span>
          </div>
          <p>Bài: ${escapeHtml(payload.title)} — Đã tạo ${(payload.results || []).length} dòng kết quả trong dự án.</p>
        </div>
      `;
    }

    async function loadResults(docId = state.lastAddedDocId) {
      if (!state.currentProject) return;
      try {
        const params = new URLSearchParams();
        if (docId) params.set("doc_id", docId);
        const level = document.getElementById("filter-level")?.value || "";
        const otherDoc = document.getElementById("filter-other-doc")?.value || "";
        const section = document.getElementById("filter-section")?.value || "";
        const search = document.getElementById("filter-search")?.value || "";
        const sortBy = document.getElementById("filter-sort")?.value || "risk";
        if (level) params.set("level", level);
        if (otherDoc) params.set("other_doc_id", otherDoc);
        if (section) params.set("section", section);
        if (search) params.set("search", search);
        params.set("sort_by", sortBy);
        const payload = await api(`/api/project/${state.currentProject.id}/results?${params.toString()}`);
        state.currentResults = payload.results;
        const summary = payload.summary;
        document.getElementById("results-summary").innerHTML = `
          <span class="badge red">${summary.conflict} xung đột</span>
          <span class="badge amber">${summary.review} cần xem lại</span>
          <span class="badge green">${summary.ok} ổn</span>
          <span class="badge">${payload.doc_count} bài</span>
          <span class="badge">${payload.comparison_count} cặp đã được so sánh</span>
        `;
        renderResultsTable(payload.results);
      } catch (error) {
        setBanner(error.message, "error");
      }
    }

    function renderResultsTable(results) {
      const root = document.getElementById("results-table-wrap");
      if (!results.length) {
        root.innerHTML = `<div class="empty-state"><strong>Chưa có kết quả phù hợp bộ lọc.</strong></div>`;
        document.getElementById("result-detail").innerHTML = "";
        return;
      }
      root.innerHTML = `
        <table>
          <thead>
            <tr>
              <th>Bài xung đột</th>
              <th>Mục</th>
              <th>Điểm rủi ro</th>
              <th>Mức độ</th>
              <th>Câu trùng tiêu biểu</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            ${results.map((result, index) => `
              <tr>
                <td>${escapeHtml(result.doc_b_title)}</td>
                <td>${escapeHtml(result.section)}</td>
                <td>
                  ${riskBadge(result.level, result.risk_score)}
                  ${riskBar(result.level, result.risk_score)}
                </td>
                <td>${RESULT_LEVELS_JSON[result.level] || result.level}</td>
                <td>${escapeHtml(result.detail.preview_a || result.detail.message || "")}</td>
                <td><button class="secondary" onclick="showResultDetail(${index})">Xem chi tiết</button></td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
    }

    function showResultDetail(index) {
      const result = state.currentResults[index];
      if (!result) return;
      const detail = result.detail || {};
      const highlightLeft = commonHighlight(detail.protected_a || detail.raw_a || "", detail.preview_b || "", result.level);
      const highlightRight = commonHighlight(detail.protected_b || detail.raw_b || "", detail.preview_a || "", result.level);
      document.getElementById("result-detail").innerHTML = `
        <div class="card stack">
          <div class="summary-row">
            ${riskBadge(result.level, result.risk_score)}
            <span class="badge">Mục: ${escapeHtml(result.section)}</span>
            <span class="badge">Ngữ nghĩa: ${Number(detail.layer_scores?.semantic || 0).toFixed(1)}%</span>
            <span class="badge">N-gram: ${Number(detail.layer_scores?.ngram || 0).toFixed(1)}%</span>
            <span class="badge">LCS: ${Number(detail.layer_scores?.lcs || 0).toFixed(1)}%</span>
          </div>
          <div class="split">
            <div class="stack">
              <h3>Bài mới</h3>
              <pre>${highlightLeft}</pre>
            </div>
            <div class="stack">
              <h3>Bài xung đột</h3>
              <pre>${highlightRight}</pre>
            </div>
          </div>
          <div class="split">
            <div class="card">
              <h3>Điểm theo từng lớp</h3>
              <p>N-gram: ${Number(detail.layer_scores?.ngram || 0).toFixed(1)}%</p>
              <p>Ngữ nghĩa: ${Number(detail.layer_scores?.semantic || 0).toFixed(1)}%</p>
              <p>LCS: ${Number(detail.layer_scores?.lcs || 0).toFixed(1)}%</p>
              <p>Chế độ semantic: ${escapeHtml(semanticModeLabel(detail.layer_scores?.semantic_mode))}</p>
            </div>
            <div class="card">
              <h3>Phần đã được bỏ qua</h3>
              <p>Doc A: ${escapeHtml(JSON.stringify(detail.strip_log_a || {}, null, 2))}</p>
              <p>Doc B: ${escapeHtml(JSON.stringify(detail.strip_log_b || {}, null, 2))}</p>
              ${(detail.notes || []).length ? `<div class="pill-list">${detail.notes.map((note) => `<span class="pill">${escapeHtml(note)}</span>`).join("")}</div>` : ""}
            </div>
          </div>
        </div>
      `;
    }

    async function loadDocs() {
      if (!state.currentProject) return;
      try {
        const payload = await api(`/api/project/${state.currentProject.id}/docs`);
        const docs = payload.documents;
        state.documents = docs;
        const root = document.getElementById("doc-list-wrap");
        const filterRoot = document.getElementById("filter-other-doc");
        if (filterRoot) {
          const currentValue = filterRoot.value;
          filterRoot.innerHTML = `<option value="">Tất cả bài</option>` + docs.map((doc) => `<option value="${doc.id}">${escapeHtml(doc.title)}</option>`).join("");
          filterRoot.value = currentValue;
        }
        if (!docs.length) {
          root.innerHTML = `<div class="empty-state"><strong>Chưa có bài nào trong dự án.</strong></div>`;
          return;
        }
        root.innerHTML = `
          <table>
            <thead>
              <tr>
                <th>Bài</th>
                <th>Ngày thêm</th>
                <th>Xung đột</th>
                <th>Xem lại</th>
                <th>Trạng thái</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              ${docs.map((doc) => `
                <tr>
                  <td>${escapeHtml(doc.title)}</td>
                  <td>${escapeHtml(formatDateTime(doc.added_at))}</td>
                  <td>${doc.conflict_count}</td>
                  <td>${doc.review_count}</td>
                  <td>${doc.status}</td>
                  <td>
                    <div class="toolbar">
                      ${doc.source_url ? `<a class="link-button secondary" href="${escapeHtml(doc.source_url)}" target="_blank" rel="noreferrer">Mở nguồn</a>` : ""}
                      <button class="secondary" title="Kiểm tra lại bài này" onclick="recheckDoc(${doc.id})">Kiểm tra lại</button>
                      <button class="danger" title="Xóa bài khỏi dự án" onclick="deleteDoc(${doc.id})">Xóa</button>
                    </div>
                  </td>
                </tr>
              `).join("")}
            </tbody>
          </table>
        `;
      } catch (error) {
        setBanner(error.message, "error");
      }
    }

    async function recheckDoc(docId) {
      try {
        await api(`/api/document/${docId}/recheck`, { method: "POST", body: JSON.stringify({}) });
        setBanner("Đã kiểm tra lại bài", "success");
        await loadProjects(state.currentProject.id);
        await loadResults(docId);
        await loadDocs();
      } catch (error) {
        setBanner(error.message, "error");
      }
    }

    async function deleteDoc(docId) {
      if (!confirm("Xóa bài này khỏi dự án?")) return;
      try {
        await api(`/api/document/${docId}`, { method: "DELETE" });
        setBanner("Đã xóa bài", "success");
        await loadProjects(state.currentProject.id);
        await loadResults();
        await loadDocs();
      } catch (error) {
        setBanner(error.message, "error");
      }
    }

    async function renameProject() {
      if (!state.currentProject) return;
      const name = prompt("Tên mới của dự án", state.currentProject.name);
      if (!name) return;
      try {
        await api(`/api/project/${state.currentProject.id}/rename`, {
          method: "POST",
          body: JSON.stringify({ name }),
        });
        setBanner("Đã đổi tên dự án", "success");
        await loadProjects(state.currentProject.id);
      } catch (error) {
        setBanner(error.message, "error");
      }
    }

    async function duplicateProject() {
      if (!state.currentProject) return;
      try {
        const payload = await api(`/api/project/${state.currentProject.id}/duplicate`, {
          method: "POST",
          body: JSON.stringify({}),
        });
        setBanner("Đã nhân bản dự án", "success");
        await loadProjects(payload.project.id);
      } catch (error) {
        setBanner(error.message, "error");
      }
    }

    async function deleteProject() {
      if (!state.currentProject) return;
      if (!confirm("Xóa toàn bộ dự án này?")) return;
      try {
        await api(`/api/project/${state.currentProject.id}/delete`, { method: "DELETE" });
        setBanner("Đã xóa dự án", "success");
        state.currentProject = null;
        await loadProjects();
      } catch (error) {
        setBanner(error.message, "error");
      }
    }

    const RESULT_LEVELS_JSON = """ + json.dumps(RESULT_LEVELS, ensure_ascii=False) + """;
    loadProjects();
  </script>
</body>
</html>"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Smart duplicate content checker")
    sub = parser.add_subparsers(dest="command")

    serve = sub.add_parser("serve", help="Chay FastAPI web app")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=int(os.getenv("PORT", "8000")))

    sub.add_parser("self-test", help="Chay self-test 3 tai lieu mau")

    args = parser.parse_args()
    if args.command == "self-test":
        summary = SERVICE.self_test()
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return
    if FASTAPI_IMPORT_ERROR is not None or uvicorn is None or app is None:
        raise SystemExit(
            "Chua cai dependencies. Hay chay `pip install -r requirements.txt` roi thu lai."
        )
    uvicorn.run("app:app", host=args.host, port=args.port, reload=False)


if __name__ == "__main__":
    main()
