"""File-backed registry for built-in and custom templates."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .template_catalog import FALLBACK_TEMPLATE, builtin_template_map, builtin_templates, deep_copy_template

try:
    import psycopg
    from psycopg.rows import dict_row
except ModuleNotFoundError:  # pragma: no cover - optional in local-only mode
    psycopg = None
    dict_row = None


class TemplateRegistry:
    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)
        self.templates_dir = self.base_dir / "data" / "templates"
        self.templates_dir.mkdir(parents=True, exist_ok=True)
        self.database_url = os.getenv("DATABASE_URL", "").strip()
        self.use_postgres = bool(self.database_url)
        if self.use_postgres and psycopg is None:
            raise RuntimeError("DATABASE_URL is set but psycopg is not installed.")
        if self.use_postgres:
            self._init_db()
        self.reload()

    def reload(self) -> None:
        self._builtin_map = builtin_template_map()
        self._custom_templates = self._load_custom_templates()
        generic_template = deep_copy_template(FALLBACK_TEMPLATE)
        generic_template["auto_detect_enabled"] = False
        self._templates = (
            builtin_templates()
            + [generic_template]
            + [deep_copy_template(template) for template in self._custom_templates]
        )

    def list_templates(self) -> list[dict]:
        return [deep_copy_template(template) for template in self._templates]

    def get(self, template_id: str | None) -> dict | None:
        if not template_id:
            return None
        for template in self._templates:
            if template["id"] == template_id:
                return deep_copy_template(template)
        return None

    def builtin_strategy_options(self) -> list[dict[str, str]]:
        return [
            {"id": template["id"], "name": template["name"]}
            for template in builtin_templates()
            if template["id"] != FALLBACK_TEMPLATE["id"]
        ] + [{"id": FALLBACK_TEMPLATE["id"], "name": FALLBACK_TEMPLATE["name"]}]

    def save_custom_template(self, template: dict) -> Path:
        if self.use_postgres:
            with self._connect_postgres() as conn:
                conn.execute(
                    """
                    INSERT INTO templates (id, name, payload_json, created_at, updated_at)
                    VALUES (%s, %s, %s, NOW(), NOW())
                    ON CONFLICT (id)
                    DO UPDATE SET
                      name = EXCLUDED.name,
                      payload_json = EXCLUDED.payload_json,
                      updated_at = NOW()
                    """,
                    (
                        template["id"],
                        template["name"],
                        json.dumps(template, ensure_ascii=True),
                    ),
                )
            self.reload()
            return Path(f"postgres://templates/{template['id']}")
        path = self.templates_dir / f"{template['id']}.json"
        path.write_text(json.dumps(template, ensure_ascii=True, indent=2), encoding="utf-8")
        self.reload()
        return path

    def _load_custom_templates(self) -> list[dict]:
        if self.use_postgres:
            with self._connect_postgres() as conn:
                rows = conn.execute(
                    "SELECT payload_json FROM templates ORDER BY name ASC"
                ).fetchall()
            templates: list[dict] = []
            for row in rows:
                try:
                    payload = json.loads(row["payload_json"])
                except json.JSONDecodeError:
                    continue
                if "id" not in payload or "name" not in payload:
                    continue
                payload.setdefault("custom_template", True)
                payload.setdefault("auto_detect_enabled", False)
                templates.append(payload)
            return templates
        templates: list[dict] = []
        for path in sorted(self.templates_dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if "id" not in payload or "name" not in payload:
                continue
            payload.setdefault("custom_template", True)
            payload.setdefault("auto_detect_enabled", False)
            templates.append(payload)
        return templates

    def clone_from_strategy(
        self,
        *,
        strategy_id: str,
        template_id: str,
        name: str,
        heading_patterns: list[str],
        template_signature: dict[str, Any],
        auto_detect_enabled: bool,
    ) -> dict:
        base = self._builtin_map.get(strategy_id, deep_copy_template(FALLBACK_TEMPLATE))
        template = deep_copy_template(base)
        template["id"] = template_id
        template["name"] = name
        template["heading_patterns"] = heading_patterns or template.get("heading_patterns", [])
        template["detection_threshold"] = 0.99
        template["custom_template"] = True
        template["auto_detect_enabled"] = auto_detect_enabled
        template["strategy_id"] = strategy_id
        template["template_signature"] = template_signature
        return template

    def _connect_postgres(self):
        return psycopg.connect(self.database_url, autocommit=True, row_factory=dict_row)

    def _init_db(self) -> None:
        with self._connect_postgres() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS templates (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
