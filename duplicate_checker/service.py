"""Core parsing, storage, and scoring logic for the duplicate checker."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import urllib.error
import urllib.request
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse
from xml.etree import ElementTree as ET

from .template_catalog import FALLBACK_TEMPLATE
from .template_registry import TemplateRegistry

try:
    import psycopg
    from psycopg.rows import dict_row
except ModuleNotFoundError:  # pragma: no cover - optional in local-only mode
    psycopg = None
    dict_row = None

DATA_DIR_NAME = "data"
DB_FILE_NAME = "duplicate_checker.sqlite3"

WORD_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
NS = {"w": WORD_NS}
W_VAL = f"{{{WORD_NS}}}val"
WORD_RE = re.compile(r"[a-z0-9]+(?:[.-][a-z0-9]+)?", re.IGNORECASE)
URL_RE = re.compile(r"^https?://", re.IGNORECASE)
STANDALONE_URL_RE = re.compile(r"^https?://\S+$", re.IGNORECASE)
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "they",
    "them",
    "their",
    "this",
    "to",
    "with",
    "can",
}
LOW_SIGNAL_STEMS = {
    "fit-for",
    "pack-unit",
    "serv",
    "these",
    "this",
    "that",
    "those",
    "mak",
    "keep",
    "help",
    "work",
    "use",
    "provid",
    "illustrat",
    "scenar",
    "situation",
    "typic",
    "profession",
    "workflow",
    "foodservic",
    "without",
}
FAQ_SHORT_ACK_RE = re.compile(
    r"^\s*(yes|no|certainly|of course|absolutely|indeed|sure|okay|ok)\s*[.!?]+\s*",
    re.IGNORECASE,
)
GENERIC_FRAMING_PATTERNS = [
    re.compile(r"\bthe following (scenarios|examples) illustrate\b", re.IGNORECASE),
    re.compile(r"\bthe examples below show\b", re.IGNORECASE),
    re.compile(r"\bthis section (outlines|shows|illustrates)\b", re.IGNORECASE),
    re.compile(r"\btypical foodservice (operations|workflows)\b", re.IGNORECASE),
]
CAPACITY_SIGNAL_STEMS = {"space", "volum", "capacity", "capacity-fit", "overfill", "enough", "suffici", "accommodat"}
DELIVERY_SIGNAL_STEMS = {"takeaway-delivery", "leak-resistant", "secure-closure", "transit", "delivery", "spill", "leak"}
STACKING_SIGNAL_STEMS = {"stackable", "storage-space", "stack", "lightweight"}
VISIBILITY_SIGNAL_STEMS = {"clear-visibility", "deli-display", "display", "transparent"}
FRAMING_SIGNAL_STEMS = {"lunch-service", "workflow", "scenar", "illustrat", "operation"}


@dataclass
class Block:
    kind: str
    text: str
    style: str = ""
    is_list: bool = False


@dataclass
class Section:
    name: str
    text: str
    heading: str = ""
    mode: str = "mixed"
    weight: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Finding:
    other_document_id: int
    other_document_key: str
    other_display_name: str
    section_name: str
    rule: str
    severity: str
    risk: float
    lexical_similarity: float
    semantic_similarity: float
    exact_span_tokens: int
    excerpt: str
    other_excerpt: str
    other_section_name: str = ""
    comparison_scope: str = "same_section"
    other_source_name: str = ""
    reason_label: str = ""
    current_highlight_terms: list[str] = field(default_factory=list)
    other_highlight_terms: list[str] = field(default_factory=list)


@dataclass
class AnalysisResult:
    document_key: str
    display_name: str
    version: int
    template_id: str
    template_name: str
    unique_score: float
    total_risk: float
    status: str
    sections: dict[str, Section]
    section_risks: dict[str, float]
    findings: list[Finding]
    source_name: str
    content_hash: str
    raw_text: str
    signature: dict[str, Any]
    source_url: str = ""
    source_kind: str = "upload"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_data_dir(base_dir: str | Path) -> Path:
    data_dir = Path(base_dir) / DATA_DIR_NAME
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def db_path_for(base_dir: str | Path) -> Path:
    return ensure_data_dir(base_dir) / DB_FILE_NAME


class Storage:
    def __init__(self, base_dir: str | Path):
        self.db_path = db_path_for(base_dir)
        self.database_url = os.getenv("DATABASE_URL", "").strip()
        self.use_postgres = bool(self.database_url)
        if self.use_postgres and psycopg is None:
            raise RuntimeError("DATABASE_URL is set but psycopg is not installed.")
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _connect_postgres(self):
        return psycopg.connect(self.database_url, autocommit=True, row_factory=dict_row)

    def _init_db(self) -> None:
        if self.use_postgres:
            with self._connect_postgres() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS documents (
                        id BIGSERIAL PRIMARY KEY,
                        document_key TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        display_name TEXT NOT NULL,
                        template_id TEXT NOT NULL,
                        template_name TEXT NOT NULL,
                        source_name TEXT NOT NULL,
                        content_hash TEXT NOT NULL,
                        raw_text TEXT NOT NULL,
                        parsed_json TEXT NOT NULL,
                        unique_score DOUBLE PRECISION NOT NULL,
                        total_risk DOUBLE PRECISION NOT NULL,
                        status TEXT NOT NULL,
                        superseded BOOLEAN NOT NULL DEFAULT FALSE,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_documents_template ON documents(template_id)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_documents_key ON documents(document_key)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_documents_superseded ON documents(superseded)"
                )
            return
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    document_key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    display_name TEXT NOT NULL,
                    template_id TEXT NOT NULL,
                    template_name TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    raw_text TEXT NOT NULL,
                    parsed_json TEXT NOT NULL,
                    unique_score REAL NOT NULL,
                    total_risk REAL NOT NULL,
                    status TEXT NOT NULL,
                    superseded INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_documents_template ON documents(template_id);
                CREATE INDEX IF NOT EXISTS idx_documents_key ON documents(document_key);
                CREATE INDEX IF NOT EXISTS idx_documents_superseded ON documents(superseded);
                """
            )

    def next_version(self, document_key: str) -> int:
        if self.use_postgres:
            with self._connect_postgres() as conn:
                row = conn.execute(
                    "SELECT COALESCE(MAX(version), 0) AS version FROM documents WHERE document_key = %s",
                    (document_key,),
                ).fetchone()
            return int(row["version"]) + 1
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(version), 0) AS version FROM documents WHERE document_key = ?",
                (document_key,),
            ).fetchone()
        return int(row["version"]) + 1

    def supersede_existing(self, document_key: str) -> None:
        if self.use_postgres:
            with self._connect_postgres() as conn:
                conn.execute(
                    "UPDATE documents SET superseded = TRUE WHERE document_key = %s AND superseded = FALSE",
                    (document_key,),
                )
            return
        with self._connect() as conn:
            conn.execute(
                "UPDATE documents SET superseded = 1 WHERE document_key = ? AND superseded = 0",
                (document_key,),
            )

    def save_result(self, result: AnalysisResult) -> int:
        payload = {
            "signature": result.signature,
            "section_risks": result.section_risks,
            "sections": {name: asdict(section) for name, section in result.sections.items()},
            "findings": [asdict(finding) for finding in result.findings],
            "source_url": result.source_url,
            "source_kind": result.source_kind,
        }
        self.supersede_existing(result.document_key)
        if self.use_postgres:
            with self._connect_postgres() as conn:
                row = conn.execute(
                    """
                    INSERT INTO documents (
                        document_key,
                        version,
                        display_name,
                        template_id,
                        template_name,
                        source_name,
                        content_hash,
                        raw_text,
                        parsed_json,
                        unique_score,
                        total_risk,
                        status,
                        superseded,
                        created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, %s)
                    RETURNING id
                    """,
                    (
                        result.document_key,
                        result.version,
                        result.display_name,
                        result.template_id,
                        result.template_name,
                        result.source_name,
                        result.content_hash,
                        result.raw_text,
                        json.dumps(payload, ensure_ascii=True),
                        result.unique_score,
                        result.total_risk,
                        result.status,
                        utc_now(),
                    ),
                ).fetchone()
            return int(row["id"])
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO documents (
                    document_key,
                    version,
                    display_name,
                    template_id,
                    template_name,
                    source_name,
                    content_hash,
                    raw_text,
                    parsed_json,
                    unique_score,
                    total_risk,
                    status,
                    superseded,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    result.document_key,
                    result.version,
                    result.display_name,
                    result.template_id,
                    result.template_name,
                    result.source_name,
                    result.content_hash,
                    result.raw_text,
                    json.dumps(payload, ensure_ascii=True),
                    result.unique_score,
                    result.total_risk,
                    result.status,
                    utc_now(),
                ),
            )
            return int(cursor.lastrowid)

    def active_documents_for_template(self, template_id: str, exclude_key: str | None = None) -> list[sqlite3.Row]:
        if self.use_postgres:
            query = """
                SELECT *
                FROM documents
                WHERE template_id = %s
                  AND superseded = FALSE
            """
            params: list[Any] = [template_id]
            if exclude_key:
                query += " AND document_key != %s"
                params.append(exclude_key)
            query += " ORDER BY created_at DESC"
            with self._connect_postgres() as conn:
                rows = conn.execute(query, params).fetchall()
            return rows
        query = """
            SELECT *
            FROM documents
            WHERE template_id = ?
              AND superseded = 0
        """
        params: list[Any] = [template_id]
        if exclude_key:
            query += " AND document_key != ?"
            params.append(exclude_key)
        query += " ORDER BY created_at DESC"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return rows

    def latest_documents(
        self,
        *,
        status: str = "",
        template_id: str = "",
        search: str = "",
    ) -> list[sqlite3.Row]:
        if self.use_postgres:
            query = """
                SELECT *
                FROM documents
                WHERE superseded = FALSE
            """
            params: list[Any] = []
            if status:
                query += " AND status = %s"
                params.append(status)
            if template_id:
                query += " AND template_id = %s"
                params.append(template_id)
            if search:
                query += " AND (LOWER(display_name) LIKE %s OR LOWER(document_key) LIKE %s)"
                token = f"%{search.lower()}%"
                params.extend([token, token])
            query += " ORDER BY created_at DESC"
            with self._connect_postgres() as conn:
                rows = conn.execute(query, params).fetchall()
            return rows
        query = """
            SELECT *
            FROM documents
            WHERE superseded = 0
        """
        params: list[Any] = []
        if status:
            query += " AND status = ?"
            params.append(status)
        if template_id:
            query += " AND template_id = ?"
            params.append(template_id)
        if search:
            query += " AND (LOWER(display_name) LIKE ? OR LOWER(document_key) LIKE ?)"
            token = f"%{search.lower()}%"
            params.extend([token, token])
        query += " ORDER BY created_at DESC"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return rows

    def status_counts(self) -> dict[str, int]:
        if self.use_postgres:
            with self._connect_postgres() as conn:
                rows = conn.execute(
                    """
                    SELECT status, COUNT(*) AS total
                    FROM documents
                    WHERE superseded = FALSE
                    GROUP BY status
                    """
                ).fetchall()
            return {str(row["status"]): int(row["total"]) for row in rows}
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS total
                FROM documents
                WHERE superseded = 0
                GROUP BY status
                """
            ).fetchall()
        return {str(row["status"]): int(row["total"]) for row in rows}

    def template_counts(self) -> dict[str, int]:
        if self.use_postgres:
            with self._connect_postgres() as conn:
                rows = conn.execute(
                    """
                    SELECT template_id, COUNT(*) AS total
                    FROM documents
                    WHERE superseded = FALSE
                    GROUP BY template_id
                    """
                ).fetchall()
            return {str(row["template_id"]): int(row["total"]) for row in rows}
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT template_id, COUNT(*) AS total
                FROM documents
                WHERE superseded = 0
                GROUP BY template_id
                """
            ).fetchall()
        return {str(row["template_id"]): int(row["total"]) for row in rows}

    def get_document(self, document_id: int) -> sqlite3.Row | None:
        if self.use_postgres:
            with self._connect_postgres() as conn:
                row = conn.execute("SELECT * FROM documents WHERE id = %s", (document_id,)).fetchone()
            return row
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM documents WHERE id = ?", (document_id,)).fetchone()
        return row

    def delete_document(self, document_id: int) -> dict[str, Any] | None:
        row = self.get_document(document_id)
        if not row:
            return None
        document_key = str(row["document_key"])
        display_name = str(row["display_name"])
        was_superseded = bool(row["superseded"])
        restored_row = None
        if self.use_postgres:
            with self._connect_postgres() as conn:
                conn.execute("DELETE FROM documents WHERE id = %s", (document_id,))
                if not was_superseded:
                    restored_row = conn.execute(
                        """
                        SELECT id, display_name
                        FROM documents
                        WHERE document_key = %s
                        ORDER BY version DESC, created_at DESC
                        LIMIT 1
                        """,
                        (document_key,),
                    ).fetchone()
                    if restored_row:
                        conn.execute("UPDATE documents SET superseded = FALSE WHERE id = %s", (restored_row["id"],))
            return {
                "document_key": document_key,
                "display_name": display_name,
                "restored_document_id": int(restored_row["id"]) if restored_row else 0,
                "restored_display_name": str(restored_row["display_name"]) if restored_row else "",
            }
        with self._connect() as conn:
            conn.execute("DELETE FROM documents WHERE id = ?", (document_id,))
            if not was_superseded:
                restored_row = conn.execute(
                    """
                    SELECT id, display_name
                    FROM documents
                    WHERE document_key = ?
                    ORDER BY version DESC, created_at DESC
                    LIMIT 1
                    """,
                    (document_key,),
                ).fetchone()
                if restored_row:
                    conn.execute("UPDATE documents SET superseded = 0 WHERE id = ?", (restored_row["id"],))
        return {
            "document_key": document_key,
            "display_name": display_name,
            "restored_document_id": int(restored_row["id"]) if restored_row else 0,
            "restored_display_name": str(restored_row["display_name"]) if restored_row else "",
        }


def hash_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def normalize_compact(text: str) -> str:
    lowered = normalize_space(text).lower()
    lowered = re.sub(r"[^\w\s£%.-]", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def tokenize(text: str, *, drop_stopwords: bool = False) -> list[str]:
    tokens = [token.lower() for token in WORD_RE.findall(text or "")]
    if drop_stopwords:
        return [token for token in tokens if token not in STOPWORDS]
    return tokens


def semantic_normalize(text: str, template: dict[str, Any]) -> str:
    normalized = normalize_compact(text)
    for rule in template.get("semantic_alias_regex", []):
        if isinstance(rule, dict):
            pattern = rule.get("pattern", "")
            replacement = rule.get("replacement", " ")
        else:
            pattern, replacement = rule
        if not pattern:
            continue
        normalized = re.sub(pattern, replacement, normalized, flags=re.IGNORECASE)
    return normalize_space(normalized)


def stem_token(token: str) -> str:
    def _stem_simple(value: str) -> str:
        if len(value) <= 4:
            return value
        for suffix in (
            "ization",
            "isation",
            "ational",
            "fulness",
            "ousness",
            "iveness",
            "ability",
            "ibility",
            "ments",
            "ment",
            "ingly",
            "edly",
            "able",
            "ible",
            "tion",
            "sion",
            "ance",
            "ence",
            "ness",
            "less",
            "ship",
            "ings",
            "ing",
            "ers",
            "ies",
            "ied",
            "est",
            "ism",
            "ist",
            "ous",
            "ive",
            "ize",
            "ise",
            "ed",
            "er",
            "ly",
            "es",
            "s",
        ):
            if value.endswith(suffix) and len(value) - len(suffix) >= 3:
                if suffix in {"ies", "ied"}:
                    return value[: -len(suffix)] + "y"
                return value[: -len(suffix)]
        return value

    parts = token.split("-")
    return "-".join(_stem_simple(part) for part in parts if part)


def semantic_tokens(text: str, template: dict[str, Any], *, drop_stopwords: bool = True) -> list[str]:
    tokens = tokenize(semantic_normalize(text, template), drop_stopwords=drop_stopwords)
    return [stem_token(token) for token in tokens if len(token) > 1]


def trim_short_faq_opener(text: str) -> str:
    trimmed = FAQ_SHORT_ACK_RE.sub("", text or "", count=1)
    return normalize_space(trimmed)


def informative_signal_stems(text: str, template: dict[str, Any]) -> set[str]:
    return {
        token
        for token in semantic_tokens(text, template, drop_stopwords=True)
        if token not in LOW_SIGNAL_STEMS and len(token) >= 4
    }


def informative_token_count(text: str, template: dict[str, Any]) -> int:
    return len(informative_signal_stems(text, template))


def best_informative_excerpt(text: str, template: dict[str, Any]) -> str:
    normalized = normalize_space(text)
    if not normalized:
        return ""
    sentences = split_sentences(normalized)
    best_excerpt = normalized[:240]
    best_score = informative_token_count(best_excerpt, template)
    for window in (3, 2, 1):
        for candidate in sentence_windows(sentences, window):
            score = informative_token_count(candidate, template)
            if score > best_score:
                best_score = score
                best_excerpt = candidate[:240]
    return normalize_space(best_excerpt)


def choose_display_excerpt(candidate: str, full_text: str, template: dict[str, Any]) -> str:
    normalized_candidate = normalize_space(candidate)
    if informative_token_count(normalized_candidate, template) >= 4:
        return normalized_candidate[:240]
    informative_excerpt = best_informative_excerpt(full_text, template)
    if informative_token_count(informative_excerpt, template) >= 4:
        return informative_excerpt[:240]
    return normalized_candidate[:240] or normalize_space(full_text)[:240]


def looks_like_generic_framing(text: str) -> bool:
    normalized = normalize_compact(text)
    return any(pattern.search(normalized) for pattern in GENERIC_FRAMING_PATTERNS)


def shared_signal_stems(text_a: str, text_b: str, template: dict[str, Any]) -> set[str]:
    return informative_signal_stems(text_a, template) & informative_signal_stems(text_b, template)


def highlight_terms_for_text(text: str, template: dict[str, Any], shared_stems: set[str], *, limit: int = 6) -> list[str]:
    if not shared_stems:
        return []
    seen: set[str] = set()
    terms: list[str] = []

    def add(raw: str) -> None:
        lowered = raw.lower()
        if lowered in seen:
            return
        seen.add(lowered)
        terms.append(raw)

    for raw in WORD_RE.findall(text):
        stem = stem_token(raw.lower())
        if stem in shared_stems and stem not in LOW_SIGNAL_STEMS:
            add(raw)
        if len(terms) >= limit:
            return terms

    normalized = normalize_space(text)
    fallback_patterns: list[str] = []
    if "secure-closure" in shared_stems:
        fallback_patterns.append(r"\b(secure|tight|snug|seal|closure|lid)\b")
    if "takeaway-delivery" in shared_stems:
        fallback_patterns.append(r"\b(takeaway|delivery|to-go|transit|go)\b")
    if "leak-resistant" in shared_stems:
        fallback_patterns.append(r"\b(leak|leaks|spill|spills)\b")
    if "capacity-fit" in shared_stems or CAPACITY_SIGNAL_STEMS & shared_stems:
        fallback_patterns.append(r"\b(volume|space|capacity|accommodate|fit|enough|sufficient)\b")
    if "clear-visibility" in shared_stems:
        fallback_patterns.append(r"\b(clear|transparent|display|show|showcase|present)\b")
    if "stackable" in shared_stems or "storage-space" in shared_stems:
        fallback_patterns.append(r"\b(stack|stackable|storage|space|light|lightweight)\b")
    for pattern in fallback_patterns:
        for match in re.finditer(pattern, normalized, re.IGNORECASE):
            add(match.group(0))
            if len(terms) >= limit:
                return terms
    return terms


def reason_label_for_overlap(section_name: str, rule: str, shared_stems: set[str]) -> str:
    if section_name == "faq":
        if shared_stems & DELIVERY_SIGNAL_STEMS:
            return "same delivery-safe faq"
        return "same faq answer intent"
    if shared_stems & CAPACITY_SIGNAL_STEMS:
        return "same capacity-fit idea"
    if shared_stems & DELIVERY_SIGNAL_STEMS:
        return "same delivery-safe claim"
    if shared_stems & STACKING_SIGNAL_STEMS:
        return "same storage/stacking angle"
    if shared_stems & VISIBILITY_SIGNAL_STEMS:
        return "same visibility/display angle"
    if section_name == "use_cases" or shared_stems & FRAMING_SIGNAL_STEMS:
        return "same use-case framing"
    if rule in {"near_copy", "exact_span"}:
        return "same duplicate expression"
    return "same product-fit idea"


def split_sentences(text: str) -> list[str]:
    sentences = [normalize_space(part) for part in SENTENCE_SPLIT_RE.split(text or "")]
    return [sentence for sentence in sentences if sentence]


def sentence_windows(sentences: list[str], window: int) -> list[str]:
    if len(sentences) < window:
        return [" ".join(sentences)] if sentences else []
    return [" ".join(sentences[index : index + window]) for index in range(0, len(sentences) - window + 1)]


def ngrams(tokens: list[str], size: int) -> set[tuple[str, ...]]:
    if len(tokens) < size:
        return {tuple(tokens)} if tokens else set()
    return {tuple(tokens[index : index + size]) for index in range(0, len(tokens) - size + 1)}


def jaccard(a: set[Any], b: set[Any]) -> float:
    if not a and not b:
        return 0.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union else 0.0


def cosine_from_maps(map_a: dict[str, float], map_b: dict[str, float]) -> float:
    if not map_a or not map_b:
        return 0.0
    shared = set(map_a) & set(map_b)
    numerator = sum(map_a[token] * map_b[token] for token in shared)
    norm_a = math.sqrt(sum(value * value for value in map_a.values()))
    norm_b = math.sqrt(sum(value * value for value in map_b.values()))
    if not norm_a or not norm_b:
        return 0.0
    return numerator / (norm_a * norm_b)


def term_frequency(tokens: list[str]) -> dict[str, float]:
    counts: dict[str, float] = {}
    for token in tokens:
        counts[token] = counts.get(token, 0.0) + 1.0
    return counts


class OptionalEmbeddingClient:
    """Uses OpenAI embeddings if configured, otherwise returns None."""

    def __init__(self) -> None:
        self.api_key = os.getenv("OPENAI_API_KEY", "").strip()
        self.model = os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small").strip()
        self.cache: dict[str, list[float]] = {}

    def available(self) -> bool:
        return bool(self.api_key)

    def similarity(self, text_a: str, text_b: str) -> float | None:
        if not self.available():
            return None
        try:
            emb_a = self._embedding(text_a)
            emb_b = self._embedding(text_b)
        except Exception:
            return None
        numerator = sum(left * right for left, right in zip(emb_a, emb_b))
        norm_a = math.sqrt(sum(value * value for value in emb_a))
        norm_b = math.sqrt(sum(value * value for value in emb_b))
        if not norm_a or not norm_b:
            return None
        return numerator / (norm_a * norm_b)

    def _embedding(self, text: str) -> list[float]:
        payload_text = normalize_space(text)
        cache_key = hashlib.sha256(f"{self.model}:{payload_text}".encode("utf-8")).hexdigest()
        if cache_key in self.cache:
            return self.cache[cache_key]
        payload = json.dumps({"model": self.model, "input": payload_text}).encode("utf-8")
        request = urllib.request.Request(
            "https://api.openai.com/v1/embeddings",
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=25) as response:
                data = json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError) as exc:
            raise RuntimeError("embedding request failed") from exc
        embedding = data["data"][0]["embedding"]
        self.cache[cache_key] = embedding
        return embedding


class DuplicateCheckerService:
    def __init__(self, base_dir: str | Path):
        self.base_dir = Path(base_dir)
        self.storage = Storage(base_dir)
        self.template_registry = TemplateRegistry(base_dir)
        self.embedding_client = OptionalEmbeddingClient()

    def analyze_submission(
        self,
        *,
        document_key: str,
        display_name: str,
        source_name: str,
        file_bytes: bytes | None = None,
        pasted_text: str = "",
        remote_url: str = "",
        forced_template_id: str = "",
    ) -> AnalysisResult:
        source_name = source_name or display_name or document_key
        source_url = ""
        source_kind = "upload"
        if file_bytes:
            raw_text, blocks = extract_blocks_from_source(source_name, file_bytes)
            content_hash = hash_bytes(file_bytes)
        elif remote_url:
            source_url = remote_url.strip()
            source_kind = "google_docs" if parse_google_doc_id(source_url) else "remote_url"
            source_name, file_bytes = fetch_remote_source(remote_url)
            raw_text, blocks = extract_blocks_from_source(source_name, file_bytes)
            content_hash = hash_bytes(file_bytes)
        else:
            source_kind = "pasted_text"
            raw_text, blocks = extract_blocks_from_text(pasted_text)
            content_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

        template, signature = detect_template(
            blocks,
            self.template_registry.list_templates(),
            forced_template_id=forced_template_id,
        )
        sections, maybe_display_name = extract_sections(blocks, template)
        if maybe_display_name and not display_name:
            display_name = maybe_display_name
        display_name = display_name or document_key
        version = self.storage.next_version(document_key)

        comparison_rows = self.storage.active_documents_for_template(template["id"], exclude_key=document_key)
        findings: list[Finding] = []
        section_risks: dict[str, float] = {name: 0.0 for name in sections}
        for row in comparison_rows:
            parsed = json.loads(row["parsed_json"])
            other_sections = {
                name: Section(**section_data)
                for name, section_data in parsed.get("sections", {}).items()
            }
            findings.extend(
                compare_section_sets(
                    sections=sections,
                    other_sections=other_sections,
                    other_row=row,
                    template=template,
                    embedding_client=self.embedding_client,
                )
            )

        for finding in findings:
            current = section_risks.get(finding.section_name, 0.0)
            section_risks[finding.section_name] = max(current, finding.risk)

        weighted_total = weighted_risk(section_risks, sections)
        unique_score = max(0.0, 100.0 - weighted_total)
        status = classify_status(weighted_total, findings, template)
        return AnalysisResult(
            document_key=document_key,
            display_name=display_name,
            version=version,
            template_id=template["id"],
            template_name=template["name"],
            unique_score=round(unique_score, 2),
            total_risk=round(weighted_total, 2),
            status=status,
            sections=sections,
            section_risks={key: round(value * 100.0, 2) for key, value in section_risks.items()},
            findings=sorted(findings, key=lambda item: item.risk, reverse=True)[:30],
            source_name=source_name,
            content_hash=content_hash,
            raw_text=raw_text,
            signature=signature,
            source_url=source_url,
            source_kind=source_kind,
        )

    def save_result(self, result: AnalysisResult) -> int:
        return self.storage.save_result(result)

    def list_templates(self) -> list[dict]:
        counts = self.storage.template_counts()
        templates = self.template_registry.list_templates()
        for template in templates:
            template["document_count"] = counts.get(template["id"], 0)
        return templates

    def list_strategy_options(self) -> list[dict[str, str]]:
        return self.template_registry.builtin_strategy_options()

    def create_custom_template(
        self,
        *,
        name: str,
        strategy_id: str,
        samples: list[tuple[str, bytes]],
    ) -> dict:
        template_id = slugify(name)
        heading_patterns, signature = infer_custom_template_signature(samples)
        template = self.template_registry.clone_from_strategy(
            strategy_id=strategy_id,
            template_id=template_id,
            name=name,
            heading_patterns=heading_patterns,
            template_signature=signature,
            auto_detect_enabled=len(samples) >= 2,
        )
        self.template_registry.save_custom_template(template)
        return template


def weighted_risk(section_risks: dict[str, float], sections: dict[str, Section]) -> float:
    weights = [section.weight for section in sections.values() if section.weight > 0]
    if not weights:
        return 0.0
    numerator = 0.0
    denominator = 0.0
    for name, section in sections.items():
        if section.weight <= 0:
            continue
        numerator += section_risks.get(name, 0.0) * section.weight
        denominator += section.weight
    return (numerator / denominator) * 100.0 if denominator else 0.0


def classify_status(total_risk: float, findings: list[Finding], template: dict[str, Any]) -> str:
    thresholds = template["status_thresholds"]
    high_findings = [finding for finding in findings if finding.severity == "red"]
    if total_risk >= thresholds["yellow_risk_max"] or len(high_findings) >= 2:
        return "red"
    if total_risk >= thresholds["green_risk_max"] or high_findings:
        return "yellow"
    return "green"


def extract_blocks_from_source(source_name: str, payload: bytes) -> tuple[str, list[Block]]:
    suffix = Path(source_name).suffix.lower()
    if suffix == ".docx":
        return extract_docx_blocks(payload)
    text = payload.decode("utf-8", errors="replace")
    return extract_blocks_from_text(text)


def fetch_remote_source(remote_url: str) -> tuple[str, bytes]:
    url = remote_url.strip()
    if not url:
        raise RuntimeError("Remote URL is empty.")
    google_doc_id = parse_google_doc_id(url)
    candidates: list[tuple[str, str]] = []
    if google_doc_id:
        candidates.append(
            (
                f"https://docs.google.com/document/d/{google_doc_id}/export?format=docx",
                f"google-doc-{google_doc_id}.docx",
            )
        )
        candidates.append(
            (
                f"https://docs.google.com/document/d/{google_doc_id}/export?format=txt",
                f"google-doc-{google_doc_id}.txt",
            )
        )
    parsed = urlparse(url)
    fallback_name = Path(parsed.path).name or "remote.txt"
    candidates.append((url, fallback_name))

    last_error = ""
    for candidate_url, source_name in candidates:
        request = urllib.request.Request(
            candidate_url,
            headers={"User-Agent": "duplicate-checker/1.0"},
        )
        try:
            with urllib.request.urlopen(request, timeout=25) as response:
                payload = response.read()
        except (urllib.error.URLError, urllib.error.HTTPError) as exc:
            last_error = str(exc)
            continue
        if payload:
            return source_name, payload

    raise RuntimeError(
        "Khong tai duoc noi dung tu URL. Neu day la Google Docs, hay share 'Anyone with the link' "
        "hoac export DOCX roi upload. "
        f"Chi tiet: {last_error}"
    )


def parse_google_doc_id(url: str) -> str:
    match = re.search(r"docs\.google\.com/document/d/([a-zA-Z0-9_-]+)", url)
    if match:
        return match.group(1)
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    if "id" in query and query["id"]:
        return query["id"][0]
    return ""


def extract_blocks_from_text(text: str) -> tuple[str, list[Block]]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    blocks: list[Block] = []
    for index, chunk in enumerate(re.split(r"\n\s*\n", normalized)):
        paragraph = normalize_space(chunk)
        if not paragraph:
            continue
        style = ""
        if index == 0 and len(paragraph) < 120:
            style = "Title"
        blocks.append(Block(kind="paragraph", text=paragraph, style=style))
    return normalized, blocks


def infer_custom_template_signature(samples: list[tuple[str, bytes]]) -> tuple[list[str], dict[str, Any]]:
    heading_groups: list[list[str]] = []
    heading_counts: list[int] = []
    table_flags: list[bool] = []
    title_flags: list[bool] = []
    sample_names: list[str] = []

    for source_name, payload in samples:
        _, blocks = extract_blocks_from_source(source_name, payload)
        headings = [
            normalize_compact(block.text)
            for block in blocks
            if block.style.lower().startswith("heading")
        ]
        if not headings:
            continue
        heading_groups.append(headings)
        heading_counts.append(len(headings))
        table_flags.append(any(block.kind == "table" for block in blocks))
        title_flags.append(any(block.style == "Title" for block in blocks))
        sample_names.append(source_name)

    if not heading_groups:
        return [], {
            "sample_names": sample_names,
            "min_heading_count": 0,
            "max_heading_count": 0,
            "has_table_ratio": 0.0,
            "has_title_ratio": 0.0,
        }

    patterns: list[str] = []
    max_positions = max(len(headings) for headings in heading_groups)
    for position in range(max_positions):
        same_position = [headings[position] for headings in heading_groups if len(headings) > position]
        pattern = infer_heading_pattern(same_position)
        if pattern and pattern not in patterns:
            patterns.append(pattern)
    if not patterns:
        patterns = [re.escape(heading) for heading in heading_groups[0][:4]]
    signature = {
        "sample_names": sample_names,
        "min_heading_count": min(heading_counts),
        "max_heading_count": max(heading_counts),
        "has_table_ratio": round(sum(1 for flag in table_flags if flag) / len(table_flags), 3),
        "has_title_ratio": round(sum(1 for flag in title_flags if flag) / len(title_flags), 3),
        "sample_heading_examples": heading_groups[:3],
    }
    return patterns[:8], signature


def infer_heading_pattern(headings: list[str]) -> str:
    normalized_headings = [normalize_compact(text) for text in headings if normalize_compact(text)]
    if not normalized_headings:
        return ""
    token_lists = [
        [token for token in tokenize(heading) if len(token) > 2 and not token.isdigit()]
        for heading in normalized_headings
    ]
    for size in (4, 3, 2):
        common = None
        for tokens in token_lists:
            grams = {" ".join(gram) for gram in ngrams(tokens, size)}
            common = grams if common is None else common & grams
        if common:
            phrase = sorted(common, key=len, reverse=True)[0]
            if len(phrase) >= 8:
                return re.escape(phrase).replace(r"\ ", r".*")
    ordered_common = common_tokens_in_order(token_lists)
    if len(ordered_common) >= 2:
        return r".*".join(re.escape(token) for token in ordered_common[:6])
    return ""


def common_tokens_in_order(token_lists: list[list[str]]) -> list[str]:
    if not token_lists:
        return []
    first = token_lists[0]
    commons = set(first)
    for tokens in token_lists[1:]:
        commons &= set(tokens)
    return [token for token in first if token in commons]


def extract_docx_blocks(payload: bytes) -> tuple[str, list[Block]]:
    with zipfile.ZipFile(io_from_bytes(payload), "r") as archive:
        xml_content = archive.read("word/document.xml")
    root = ET.fromstring(xml_content)
    body = root.find("w:body", NS)
    blocks: list[Block] = []
    if body is None:
        return "", blocks
    for child in body:
        tag = child.tag.rsplit("}", 1)[-1]
        if tag == "p":
            block = paragraph_block(child)
            if block.text:
                blocks.append(block)
        elif tag == "tbl":
            table_text = table_block_text(child)
            if table_text:
                blocks.append(Block(kind="table", text=table_text, style="Table"))
    raw_text = "\n\n".join(block.text for block in blocks if block.text)
    return raw_text, blocks


def io_from_bytes(payload: bytes):
    from io import BytesIO

    return BytesIO(payload)


def paragraph_block(node: ET.Element) -> Block:
    style = ""
    style_node = node.find("./w:pPr/w:pStyle", NS)
    if style_node is not None:
        style = style_node.attrib.get(W_VAL, "")
    is_list = node.find("./w:pPr/w:numPr", NS) is not None
    parts: list[str] = []
    for item in node.iter():
        tag = item.tag.rsplit("}", 1)[-1]
        if tag == "t":
            parts.append(item.text or "")
        elif tag == "br":
            parts.append("\n")
    text = normalize_space("".join(parts))
    return Block(kind="paragraph", text=text, style=style, is_list=is_list)


def table_block_text(node: ET.Element) -> str:
    rows: list[str] = []
    for row in node.findall("./w:tr", NS):
        cells: list[str] = []
        for cell in row.findall("./w:tc", NS):
            cell_parts: list[str] = []
            for paragraph in cell.findall(".//w:p", NS):
                block = paragraph_block(paragraph)
                if block.text:
                    cell_parts.append(block.text)
            if cell_parts:
                cells.append(" / ".join(cell_parts))
        if cells:
            rows.append(" | ".join(cells))
    return "\n".join(rows)


def detect_template(
    blocks: list[Block],
    templates: list[dict[str, Any]],
    *,
    forced_template_id: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    heading_texts = [
        normalize_compact(block.text)
        for block in blocks
        if block.style.lower().startswith("heading")
    ]
    signature = {
        "heading_count": len(heading_texts),
        "headings": heading_texts,
        "has_table": any(block.kind == "table" for block in blocks),
        "has_title": any(block.style == "Title" for block in blocks),
    }
    if forced_template_id:
        for template in templates:
            if template["id"] == forced_template_id:
                signature["template_score"] = 1.0
                signature["template_id"] = template["id"]
                signature["forced_template"] = True
                return template, signature
    best_template = FALLBACK_TEMPLATE
    best_score = 0.0
    all_block_texts = [normalize_compact(block.text) for block in blocks if block.text]
    merged_text = " \n ".join(all_block_texts)
    for template in templates:
        if template.get("auto_detect_enabled", True) is False:
            continue
        hits = 0
        for pattern in template["heading_patterns"]:
            matcher = re.compile(pattern, re.IGNORECASE)
            if any(matcher.search(heading) for heading in heading_texts):
                hits += 1
        score = hits / len(template["heading_patterns"]) if template["heading_patterns"] else 0.0
        if not heading_texts and template["heading_patterns"]:
            content_hits = 0
            for pattern in template["heading_patterns"]:
                matcher = re.compile(pattern, re.IGNORECASE)
                if matcher.search(merged_text):
                    content_hits += 1
            content_score = content_hits / len(template["heading_patterns"])
            score = max(score, content_score)
        if signature["has_table"] and "related_products_table" in template["section_patterns"]:
            score += 0.1
        if signature["has_title"]:
            score += 0.05
        score = min(score, 1.0)
        threshold = float(template.get("unstyled_detection_threshold", template["detection_threshold"])) if not heading_texts else float(template["detection_threshold"])
        if score >= threshold and score > best_score:
            best_template = template
            best_score = score
    signature["template_score"] = round(best_score, 3)
    signature["template_id"] = best_template["id"]
    return best_template, signature


def extract_sections(blocks: list[Block], template: dict[str, Any]) -> tuple[dict[str, Section], str]:
    if template["id"] == "generic_text_v1":
        return extract_generic_sections(blocks, template)
    return extract_pandapak_sections(blocks, template)


def extract_generic_sections(blocks: list[Block], template: dict[str, Any]) -> tuple[dict[str, Section], str]:
    display_name = blocks[0].text if blocks and blocks[0].style == "Title" else ""
    sections: dict[str, Section] = {}
    if display_name:
        sections["title"] = make_section("title", display_name, template)
    body_blocks = blocks[1:] if display_name else blocks
    full_text = "\n\n".join(block.text for block in body_blocks if block.text)
    sections["full_text"] = make_section("full_text", full_text, template)
    return sections, display_name


def extract_pandapak_sections(blocks: list[Block], template: dict[str, Any]) -> tuple[dict[str, Section], str]:
    sections: dict[str, Section] = {}
    display_name = ""
    cursor = 0
    if blocks and blocks[0].style == "Title":
        display_name = blocks[0].text
        sections["title"] = make_section("title", blocks[0].text, template)
        cursor = 1
    if cursor < len(blocks) and looks_like_standalone_url(blocks[cursor].text):
        sections["source_url"] = make_section("source_url", blocks[cursor].text, template)
        cursor += 1

    heading_indices = [
        index for index, block in enumerate(blocks[cursor:], start=cursor) if block.style.lower().startswith("heading")
    ]
    if not heading_indices:
        return extract_pandapak_sections_from_flat_text(blocks[cursor:], template, existing_sections=sections, display_name=display_name)

    first_heading = heading_indices[0]
    hero_heading_text = blocks[first_heading].text
    sections["hero_heading"] = make_section("hero_heading", hero_heading_text, template, heading=hero_heading_text)

    second_heading = heading_indices[1] if len(heading_indices) > 1 else len(blocks)
    intro_blocks = blocks[first_heading + 1 : second_heading]
    intro_text = "\n\n".join(block.text for block in intro_blocks if block.text and block.kind == "paragraph")
    sections["intro"] = make_section("intro", intro_text, template)

    for offset, heading_index in enumerate(heading_indices[1:], start=1):
        next_index = heading_indices[offset + 1] if offset + 1 < len(heading_indices) else len(blocks)
        heading_block = blocks[heading_index]
        heading_text = heading_block.text
        section_name = classify_heading(heading_text, template)
        body_blocks = blocks[heading_index + 1 : next_index]
        if section_name == "faq":
            faq_section, conclusion_section = split_faq_and_conclusion(body_blocks, template)
            faq_section.heading = heading_text
            sections["faq"] = faq_section
            if conclusion_section and conclusion_section.text:
                sections["conclusion"] = conclusion_section
            continue
        body_text = "\n\n".join(block.text for block in body_blocks if block.text)
        sections[section_name] = make_section(section_name, body_text, template, heading=heading_text)
    return sections, display_name


def extract_pandapak_sections_from_flat_text(
    blocks: list[Block],
    template: dict[str, Any],
    *,
    existing_sections: dict[str, Section] | None = None,
    display_name: str = "",
) -> tuple[dict[str, Section], str]:
    sections = dict(existing_sections or {})
    full_text = "\n\n".join(block.text for block in blocks if block.text)
    normalized = normalize_space(full_text)
    if not normalized:
        if display_name and "title" not in sections:
            sections["title"] = make_section("title", display_name, template)
        return sections, display_name

    url_match = URL_RE.match(normalized)
    if url_match:
        first_space = normalized.find(" ")
        first_url = normalized if first_space == -1 else normalized[:first_space]
        if first_url and "source_url" not in sections:
            sections["source_url"] = make_section("source_url", first_url, template)
        normalized = normalize_space(normalized[len(first_url) :])

    marker_patterns = [
        ("intro", r"\bproduct overview\b"),
        ("features", r"\b(?:key features(?: of [^.!?\n]{1,120})?|features of [^.!?\n]{1,120})\b"),
        ("use_cases", r"\b(?:key use cases(?: of [^.!?\n]{1,120})?|use cases of [^.!?\n]{1,120})\b"),
        ("related_products_table", r"\b(?:compatible|related products)\b"),
        ("supplier", r"\b(?:trusted supplier|supplier for professional foodservice packaging)\b"),
        ("faq", r"\bfrequently asked questions\b"),
    ]
    positions: list[tuple[int, str, str]] = []
    for section_name, pattern in marker_patterns:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match:
            positions.append((match.start(), section_name, match.group(0)))
    positions.sort(key=lambda item: item[0])

    title_candidate = ""
    if positions:
        title_candidate = normalize_space(normalized[: positions[0][0]].strip(" :-\n"))
    elif len(normalized) <= 180:
        title_candidate = normalized
    if title_candidate and "title" not in sections:
        sections["title"] = make_section("title", title_candidate, template)
        display_name = display_name or title_candidate

    if not positions:
        sections["full_text"] = make_section("full_text", normalized, template)
        return sections, display_name

    if "hero_heading" not in sections and title_candidate:
        sections["hero_heading"] = make_section("hero_heading", title_candidate, template, heading=title_candidate)

    for index, (start, section_name, marker_text) in enumerate(positions):
        end = positions[index + 1][0] if index + 1 < len(positions) else len(normalized)
        segment = normalize_space(normalized[start:end])
        segment_body = normalize_space(segment[len(marker_text) :])
        if section_name == "intro":
            if segment_body:
                sections["intro"] = make_section("intro", segment_body, template, heading="Product overview")
            continue
        if section_name == "faq":
            faq_items = recover_faq_items_from_flat_text(segment_body)
            faq_text = "\n\n".join(
                f"{item['question']}\n{item['answer']}".strip() for item in faq_items if item["question"] or item["answer"]
            )
            sections["faq"] = make_section("faq", faq_text or segment_body, template, heading="Frequently Asked Questions", metadata={"items": faq_items})
            continue
        sections[section_name] = make_section(section_name, segment_body, template, heading=marker_text)

    if "intro" not in sections:
        first_start = positions[0][0]
        leading = normalize_space(normalized[:first_start])
        if leading and leading != title_candidate:
            sections["intro"] = make_section("intro", leading, template)
    if "full_text" not in sections:
        sections["full_text"] = make_section("full_text", normalized, template)
    return sections, display_name


def recover_faq_items_from_flat_text(text: str) -> list[dict[str, str]]:
    normalized = normalize_space(text)
    if not normalized:
        return []
    candidates = re.split(r"(?=(?:q[:.]|question[:.]|what|when|why|how|can|are|is|do|does)\s)", normalized, flags=re.IGNORECASE)
    items: list[dict[str, str]] = []
    for chunk in candidates:
        chunk = normalize_space(chunk)
        if not chunk:
            continue
        question, answer = split_question_answer(chunk)
        if question or answer:
            items.append({"question": question, "answer": answer})
    return items


def looks_like_standalone_url(text: str) -> bool:
    return bool(STANDALONE_URL_RE.fullmatch(normalize_space(text)))


def split_faq_and_conclusion(body_blocks: list[Block], template: dict[str, Any]) -> tuple[Section, Section | None]:
    faq_parts: list[dict[str, str]] = []
    trailing: list[str] = []
    for block in body_blocks:
        text = block.text
        if looks_like_faq_item(text):
            question, answer = split_question_answer(text)
            faq_parts.append({"question": question, "answer": answer})
        elif text:
            trailing.append(text)
    faq_text = "\n\n".join(
        f"{item['question']}\n{item['answer']}" if item["answer"] else item["question"] for item in faq_parts
    )
    faq_section = make_section("faq", faq_text, template, metadata={"items": faq_parts})
    conclusion_section = None
    if trailing:
        conclusion_text = "\n\n".join(trailing)
        conclusion_section = make_section("conclusion", conclusion_text, template)
    return faq_section, conclusion_section


def looks_like_faq_item(text: str) -> bool:
    lowered = normalize_compact(text)
    return "?" in text or bool(re.match(r"^\d+\.", lowered))


def split_question_answer(text: str) -> tuple[str, str]:
    normalized = text.replace("\u2028", "\n")
    if "\n" in normalized:
        first, rest = normalized.split("\n", 1)
        return normalize_space(first), normalize_space(rest)
    question_match = re.search(r"\?", normalized)
    if question_match:
        index = question_match.end()
        return normalize_space(normalized[:index]), normalize_space(normalized[index:])
    return normalize_space(normalized), ""


def classify_heading(heading_text: str, template: dict[str, Any]) -> str:
    lowered = normalize_compact(heading_text)
    for name, patterns in template["section_patterns"].items():
        for pattern in patterns:
            if re.search(pattern, lowered, re.IGNORECASE):
                return name
    return "full_text"


def make_section(
    name: str,
    text: str,
    template: dict[str, Any],
    *,
    heading: str = "",
    metadata: dict[str, Any] | None = None,
) -> Section:
    config = template["sections"].get(name, template["sections"]["full_text"])
    return Section(
        name=name,
        text=normalize_space(text),
        heading=heading,
        mode=config["mode"],
        weight=float(config["weight"]),
        metadata=metadata or {},
    )


def compare_section_sets(
    *,
    sections: dict[str, Section],
    other_sections: dict[str, Section],
    other_row: sqlite3.Row,
    template: dict[str, Any],
    embedding_client: OptionalEmbeddingClient,
) -> list[Finding]:
    findings: list[Finding] = []
    for name, section in sections.items():
        if section.weight <= 0:
            continue
        other_section = other_sections.get(name)
        if not other_section:
            continue
        finding = compare_sections(
            section=section,
            other_section=other_section,
            template=template,
            other_row=other_row,
            embedding_client=embedding_client,
        )
        if finding:
            findings.append(finding)
    for name, section in sections.items():
        if section.weight <= 0 or not is_cross_section_source(section, template):
            continue
        for other_name, other_section in other_sections.items():
            if other_name == name or not is_cross_section_target(other_section, template):
                continue
            finding = compare_sections(
                section=section,
                other_section=other_section,
                template=template,
                other_row=other_row,
                embedding_client=embedding_client,
            )
            if finding:
                findings.append(finding)
    return findings


def compare_sections(
    *,
    section: Section,
    other_section: Section,
    template: dict[str, Any],
    other_row: sqlite3.Row,
    embedding_client: OptionalEmbeddingClient,
) -> Finding | None:
    config = template["sections"].get(section.name, template["sections"]["full_text"])
    if config["mode"] == "ignore":
        return None
    if config["mode"] == "faq":
        return compare_faq_sections(
            section=section,
            other_section=other_section,
            config=config,
            template=template,
            other_row=other_row,
            embedding_client=embedding_client,
        )

    left_text = preprocess_section_text(section, template)
    right_text = preprocess_section_text(other_section, template)
    if not left_text or not right_text:
        return None

    left_tokens = tokenize(left_text, drop_stopwords=True)
    right_tokens = tokenize(right_text, drop_stopwords=True)
    left_semantic_tokens = semantic_tokens(left_text, template, drop_stopwords=True)
    right_semantic_tokens = semantic_tokens(right_text, template, drop_stopwords=True)
    lexical = lexical_similarity(left_tokens, right_tokens)
    semantic = semantic_similarity(
        left_text,
        right_text,
        left_tokens,
        right_tokens,
        embedding_client,
        template,
    )
    concept = concept_overlap_score(left_semantic_tokens, right_semantic_tokens)
    longest_exact = exact_token_span(left_tokens, right_tokens)
    window_ratio, excerpt, other_excerpt = best_window_similarity(left_text, right_text)
    window_semantic, semantic_excerpt, semantic_other_excerpt = best_window_semantic(left_text, right_text, template)
    profile = scoring_profile(config, template)
    idea_strength = idea_overlap_strength(semantic, concept, window_semantic)
    idea_support = max(concept, window_semantic, semantic * 0.9)

    risk = 0.0
    rule = ""
    if longest_exact >= int(profile["exact_span_tokens"]) or window_ratio >= 0.92:
        risk = 0.95
        rule = "exact_span"
    if lexical >= float(profile["near_copy_lexical"]):
        candidate = min(0.85, 0.45 + lexical * 0.5)
        if candidate > risk:
            risk = candidate
            rule = "near_copy"

    semantic_red = float(profile["semantic_red"])
    semantic_yellow = float(profile["semantic_yellow"])
    red_gate = (
        lexical >= float(profile["semantic_lexical_floor_red"])
        or idea_strength >= float(profile["idea_overlap_red"])
        or concept >= float(profile["idea_overlap_red"])
        or window_semantic >= float(profile["window_semantic_red"])
    )
    yellow_gate = (
        lexical >= float(profile["semantic_lexical_floor_yellow"])
        or idea_strength >= float(profile["idea_overlap_yellow"])
        or concept >= float(profile["idea_overlap_yellow"])
        or window_semantic >= float(profile["window_semantic_yellow"])
    )

    if semantic >= semantic_red and red_gate:
        candidate = min(0.93, 0.30 + semantic * 0.38 + concept * 0.18 + window_semantic * 0.18)
        if candidate > risk:
            risk = candidate
            rule = "idea_overlap" if lexical < float(profile["near_copy_lexical"]) * 0.55 else "semantic_paraphrase"
    elif semantic >= semantic_yellow and yellow_gate:
        candidate = min(0.76, 0.22 + semantic * 0.32 + concept * 0.14 + window_semantic * 0.14)
        if candidate > risk:
            risk = candidate
            rule = "idea_overlap" if lexical < float(profile["near_copy_lexical"]) * 0.45 else "semantic_overlap"

    if idea_strength >= float(profile["idea_overlap_red"]) and idea_support >= max(0.55, float(profile["idea_overlap_yellow"])):
        candidate = min(0.88, 0.28 + idea_strength * 0.62 + idea_support * 0.10)
        if candidate > risk:
            risk = candidate
            rule = "idea_overlap"
    elif idea_strength >= float(profile["idea_overlap_yellow"]) and idea_support >= max(0.45, float(profile["idea_overlap_yellow"]) - 0.05):
        candidate = min(0.76, 0.20 + idea_strength * 0.52 + idea_support * 0.08)
        if candidate > risk:
            risk = candidate
            rule = "idea_overlap"

    fact_ratio = max(fact_heavy_ratio(left_text, template), fact_heavy_ratio(right_text, template))
    if config["mode"] in {"allow_high_overlap", "low_weight"}:
        risk *= 0.35
    elif config["mode"] == "mixed" and fact_ratio >= template["global_thresholds"]["fact_heavy_ratio"]:
        risk *= 0.65
    if (
        section.name in {"use_cases", "conclusion", "full_text"}
        and rule in {"idea_overlap", "semantic_overlap"}
        and looks_like_generic_framing(excerpt)
        and looks_like_generic_framing(other_excerpt)
    ):
        risk *= 0.52

    if risk < 0.30:
        return None
    severity = "red" if risk >= 0.75 else "yellow"
    if rule in {"semantic_paraphrase", "semantic_overlap", "idea_overlap"} and window_semantic >= window_ratio:
        excerpt = semantic_excerpt
        other_excerpt = semantic_other_excerpt
    excerpt = choose_display_excerpt(excerpt, left_text, template)
    other_excerpt = choose_display_excerpt(other_excerpt, right_text, template)
    shared_stems = shared_signal_stems(excerpt, other_excerpt, template) or shared_signal_stems(left_text, right_text, template)
    reason_label = reason_label_for_overlap(section.name, rule or "overlap", shared_stems)
    current_terms = highlight_terms_for_text(excerpt, template, shared_stems)
    other_terms = highlight_terms_for_text(other_excerpt, template, shared_stems)
    return Finding(
        other_document_id=int(other_row["id"]),
        other_document_key=str(other_row["document_key"]),
        other_display_name=str(other_row["display_name"]),
        section_name=section.name,
        rule=rule or "overlap",
        severity=severity,
        risk=round(risk, 4),
        lexical_similarity=round(lexical, 4),
        semantic_similarity=round(max(semantic, concept, window_semantic, idea_strength), 4),
        exact_span_tokens=longest_exact,
        excerpt=excerpt,
        other_excerpt=other_excerpt,
        other_section_name=other_section.name,
        comparison_scope="cross_section" if section.name != other_section.name else "same_section",
        other_source_name=str(other_row["source_name"]) if "source_name" in other_row.keys() else "",
        reason_label=reason_label,
        current_highlight_terms=current_terms,
        other_highlight_terms=other_terms,
    )


def compare_faq_sections(
    *,
    section: Section,
    other_section: Section,
    config: dict[str, Any],
    template: dict[str, Any],
    other_row: sqlite3.Row,
    embedding_client: OptionalEmbeddingClient,
) -> Finding | None:
    items = section.metadata.get("items") or []
    other_items = other_section.metadata.get("items") or []
    profile = scoring_profile(config, template)
    best_finding: Finding | None = None
    for item in items:
        for other_item in other_items:
            question_left = normalize_space(item.get("question", ""))
            question_right = normalize_space(other_item.get("question", ""))
            question_tokens_left = tokenize(question_left, drop_stopwords=True)
            question_tokens_right = tokenize(question_right, drop_stopwords=True)
            question_similarity = lexical_similarity(
                question_tokens_left,
                question_tokens_right,
            )
            question_semantic = semantic_similarity(
                question_left,
                question_right,
                question_tokens_left,
                question_tokens_right,
                embedding_client,
                template,
            )
            question_concept = concept_overlap_score(
                semantic_tokens(question_left, template, drop_stopwords=True),
                semantic_tokens(question_right, template, drop_stopwords=True),
            )
            question_window_semantic, _, _ = best_window_semantic(question_left, question_right, template)
            question_strength = idea_overlap_strength(question_semantic, question_concept, question_window_semantic)
            answer_left = trim_short_faq_opener(item.get("answer", ""))
            answer_right = trim_short_faq_opener(other_item.get("answer", ""))
            if not answer_left or not answer_right:
                continue
            answer_tokens_left = tokenize(answer_left, drop_stopwords=True)
            answer_tokens_right = tokenize(answer_right, drop_stopwords=True)
            lexical = lexical_similarity(answer_tokens_left, answer_tokens_right)
            semantic = semantic_similarity(
                answer_left,
                answer_right,
                answer_tokens_left,
                answer_tokens_right,
                embedding_client,
                template,
            )
            concept = concept_overlap_score(
                semantic_tokens(answer_left, template, drop_stopwords=True),
                semantic_tokens(answer_right, template, drop_stopwords=True),
            )
            window_semantic, excerpt, other_excerpt = best_window_semantic(answer_left, answer_right, template)
            longest_exact = exact_token_span(answer_tokens_left, answer_tokens_right)
            idea_strength = idea_overlap_strength(semantic, concept, window_semantic)
            idea_support = max(concept, window_semantic, semantic * 0.9)
            risk = 0.0
            rule = ""
            question_gate = (
                question_similarity >= float(profile["question_lexical_gate"])
                or question_semantic >= float(profile["question_semantic_gate"])
                or question_concept >= float(profile["question_concept_gate"])
                or question_strength >= max(
                    float(profile["question_concept_gate"]) + 0.08,
                    float(profile["question_semantic_gate"]) - 0.08,
                )
            )
            if question_gate:
                if longest_exact >= int(profile["exact_span_tokens"]):
                    risk = 0.92
                    rule = "faq_exact_span"
                red_gate = (
                    lexical >= float(profile["semantic_lexical_floor_red"])
                    or idea_strength >= float(profile["idea_overlap_red"])
                    or concept >= float(profile["idea_overlap_red"])
                    or window_semantic >= float(profile["window_semantic_red"])
                )
                yellow_gate = (
                    lexical >= float(profile["semantic_lexical_floor_yellow"])
                    or idea_strength >= float(profile["idea_overlap_yellow"])
                    or concept >= float(profile["idea_overlap_yellow"])
                    or window_semantic >= float(profile["window_semantic_yellow"])
                )
                if semantic >= float(profile["semantic_red"]) and red_gate:
                    risk = max(risk, 0.87)
                    rule = rule or "faq_intent_overlap"
                elif semantic >= float(profile["semantic_yellow"]) and yellow_gate:
                    risk = max(risk, 0.70)
                    rule = rule or "faq_intent_overlap"
                elif idea_strength >= float(profile["idea_overlap_red"]) and idea_support >= max(
                    0.55, float(profile["idea_overlap_yellow"])
                ):
                    risk = max(risk, min(0.88, 0.26 + idea_strength * 0.64 + idea_support * 0.10))
                    rule = rule or "faq_idea_overlap"
                elif idea_strength >= float(profile["idea_overlap_yellow"]) and idea_support >= max(
                    0.45, float(profile["idea_overlap_yellow"]) - 0.05
                ):
                    risk = max(risk, min(0.76, 0.22 + idea_strength * 0.54 + idea_support * 0.08))
                    rule = rule or "faq_idea_overlap"
            if risk < 0.30:
                continue
            severity = "red" if risk >= 0.75 else "yellow"
            excerpt = choose_display_excerpt(excerpt, answer_left, template)
            other_excerpt = choose_display_excerpt(other_excerpt, answer_right, template)
            shared_stems = shared_signal_stems(excerpt, other_excerpt, template) or shared_signal_stems(
                answer_left, answer_right, template
            )
            reason_label = reason_label_for_overlap(section.name, rule or "faq_overlap", shared_stems)
            current_terms = highlight_terms_for_text(excerpt, template, shared_stems)
            other_terms = highlight_terms_for_text(other_excerpt, template, shared_stems)
            candidate = Finding(
                other_document_id=int(other_row["id"]),
                other_document_key=str(other_row["document_key"]),
                other_display_name=str(other_row["display_name"]),
                section_name=section.name,
                rule=rule or "faq_overlap",
                severity=severity,
                risk=round(risk, 4),
                lexical_similarity=round(lexical, 4),
                semantic_similarity=round(
                    max(
                        semantic,
                        concept,
                        question_semantic,
                        question_concept,
                        question_window_semantic,
                        window_semantic,
                        idea_strength,
                        question_strength,
                    ),
                    4,
                ),
                exact_span_tokens=longest_exact,
                excerpt=excerpt[:240],
                other_excerpt=other_excerpt[:240],
                other_section_name=other_section.name,
                comparison_scope="cross_section" if section.name != other_section.name else "same_section",
                other_source_name=str(other_row["source_name"]) if "source_name" in other_row.keys() else "",
                reason_label=reason_label,
                current_highlight_terms=current_terms,
                other_highlight_terms=other_terms,
            )
            if not best_finding or candidate.risk > best_finding.risk:
                best_finding = candidate
    return best_finding


def preprocess_section_text(section: Section, template: dict[str, Any]) -> str:
    text = comparable_section_text(section)
    for pattern in template.get("approved_reuse_regex", []):
        text = re.sub(pattern, " ", text, flags=re.IGNORECASE)
    text = normalize_space(text)
    if section.name == "hero_heading":
        text = re.sub(r"\bfor professional takeaway and delivery\b", " ", text, flags=re.IGNORECASE)
        text = normalize_space(text)
    return text


def comparable_section_text(section: Section) -> str:
    text = normalize_space(section.text or "")
    if text:
        return text
    items = section.metadata.get("items") or []
    if not items:
        return ""
    parts: list[str] = []
    for item in items:
        question = normalize_space(str(item.get("question", "")))
        answer = normalize_space(str(item.get("answer", "")))
        if question and answer:
            parts.append(f"{question} {answer}")
        elif question:
            parts.append(question)
        elif answer:
            parts.append(answer)
    return normalize_space("\n\n".join(parts))


def is_cross_section_source(section: Section, template: dict[str, Any]) -> bool:
    config = template["sections"].get(section.name, template["sections"]["full_text"])
    return bool(config.get("cross_check_source"))


def is_cross_section_target(section: Section, template: dict[str, Any]) -> bool:
    config = template["sections"].get(section.name, template["sections"]["full_text"])
    return bool(config.get("cross_check_target")) and bool(comparable_section_text(section))


def scoring_profile(config: dict[str, Any], template: dict[str, Any]) -> dict[str, float]:
    thresholds = template["global_thresholds"]
    profile = {
        "exact_span_tokens": float(config.get("exact_span_tokens", thresholds["exact_span_tokens"])),
        "near_copy_lexical": float(thresholds["near_copy_lexical"]),
        "semantic_yellow": float(config.get("semantic_yellow", thresholds["semantic_yellow"])),
        "semantic_red": float(config.get("semantic_red", thresholds["semantic_red"])),
        "idea_overlap_yellow": float(config.get("idea_overlap_yellow", thresholds.get("idea_overlap_yellow", 0.55))),
        "idea_overlap_red": float(config.get("idea_overlap_red", thresholds.get("idea_overlap_red", 0.65))),
        "window_semantic_yellow": float(config.get("window_semantic_yellow", thresholds.get("window_semantic_yellow", 0.70))),
        "window_semantic_red": float(config.get("window_semantic_red", thresholds.get("window_semantic_red", 0.80))),
        "semantic_lexical_floor_yellow": float(
            config.get("semantic_lexical_floor_yellow", thresholds.get("semantic_lexical_floor_yellow", 0.08))
        ),
        "semantic_lexical_floor_red": float(
            config.get("semantic_lexical_floor_red", thresholds.get("semantic_lexical_floor_red", 0.10))
        ),
        "question_lexical_gate": float(config.get("question_lexical_gate", thresholds.get("question_lexical_gate", 0.30))),
        "question_semantic_gate": float(config.get("question_semantic_gate", thresholds.get("question_semantic_gate", 0.72))),
        "question_concept_gate": float(config.get("question_concept_gate", thresholds.get("question_concept_gate", 0.48))),
    }
    mode = config.get("mode", "mixed")
    if mode == "strict":
        profile["semantic_lexical_floor_yellow"] = min(profile["semantic_lexical_floor_yellow"], 0.05)
        profile["semantic_lexical_floor_red"] = min(profile["semantic_lexical_floor_red"], 0.08)
        profile["idea_overlap_yellow"] = min(profile["idea_overlap_yellow"], 0.48)
        profile["idea_overlap_red"] = min(profile["idea_overlap_red"], 0.58)
    elif mode == "faq":
        profile["semantic_lexical_floor_yellow"] = min(profile["semantic_lexical_floor_yellow"], 0.05)
        profile["semantic_lexical_floor_red"] = min(profile["semantic_lexical_floor_red"], 0.08)
        profile["idea_overlap_yellow"] = min(profile["idea_overlap_yellow"], 0.50)
        profile["idea_overlap_red"] = min(profile["idea_overlap_red"], 0.60)
    elif mode == "mixed":
        profile["semantic_lexical_floor_yellow"] = min(profile["semantic_lexical_floor_yellow"], 0.08)
        profile["semantic_lexical_floor_red"] = min(profile["semantic_lexical_floor_red"], 0.10)
    return profile


def lexical_similarity(tokens_a: list[str], tokens_b: list[str]) -> float:
    if not tokens_a or not tokens_b:
        return 0.0
    jaccard_score = jaccard(set(tokens_a), set(tokens_b))
    trigram_score = jaccard(ngrams(tokens_a, 3), ngrams(tokens_b, 3))
    return round((jaccard_score * 0.35) + (trigram_score * 0.65), 4)


def semantic_similarity(
    text_a: str,
    text_b: str,
    tokens_a: list[str],
    tokens_b: list[str],
    embedding_client: OptionalEmbeddingClient,
    template: dict[str, Any],
) -> float:
    semantic_tokens_a = semantic_tokens(text_a, template, drop_stopwords=True)
    semantic_tokens_b = semantic_tokens(text_b, template, drop_stopwords=True)
    tf_cosine = cosine_from_maps(term_frequency(semantic_tokens_a), term_frequency(semantic_tokens_b))
    sentence_cosine = best_sentence_semantic(text_a, text_b, template)
    window_cosine, _, _ = best_window_semantic(text_a, text_b, template)
    concept_score = concept_overlap_score(semantic_tokens_a, semantic_tokens_b)
    local_semantic = round(
        (tf_cosine * 0.35) + (sentence_cosine * 0.20) + (window_cosine * 0.25) + (concept_score * 0.20),
        4,
    )
    embedding_score = None
    if embedding_client.available() and (len(tokens_a) + len(tokens_b) >= 12):
        embedding_score = embedding_client.similarity(text_a, text_b)
    if embedding_score is not None:
        combined = (float(embedding_score) * 0.70) + (local_semantic * 0.30)
        return max(0.0, min(1.0, combined))
    return local_semantic


def best_sentence_semantic(text_a: str, text_b: str, template: dict[str, Any]) -> float:
    left_sentences = split_sentences(text_a)
    right_sentences = split_sentences(text_b)
    best = 0.0
    for left in left_sentences:
        left_tokens = semantic_tokens(left, template, drop_stopwords=True)
        if not left_tokens:
            continue
        left_tf = term_frequency(left_tokens)
        for right in right_sentences:
            right_tokens = semantic_tokens(right, template, drop_stopwords=True)
            if not right_tokens:
                continue
            score = cosine_from_maps(left_tf, term_frequency(right_tokens))
            if score > best:
                best = score
    return best


def concept_overlap_score(tokens_a: list[str], tokens_b: list[str]) -> float:
    if not tokens_a or not tokens_b:
        return 0.0
    token_jaccard = jaccard(set(tokens_a), set(tokens_b))
    token_cosine = cosine_from_maps(term_frequency(tokens_a), term_frequency(tokens_b))
    return round((token_jaccard * 0.45) + (token_cosine * 0.55), 4)


def idea_overlap_strength(semantic: float, concept: float, window_semantic: float) -> float:
    return round((semantic * 0.40) + (concept * 0.35) + (window_semantic * 0.25), 4)


def exact_token_span(tokens_a: list[str], tokens_b: list[str]) -> int:
    if not tokens_a or not tokens_b:
        return 0
    matcher = SequenceMatcher(a=tokens_a, b=tokens_b, autojunk=False)
    match = matcher.find_longest_match(0, len(tokens_a), 0, len(tokens_b))
    return int(match.size)


def best_window_similarity(text_a: str, text_b: str) -> tuple[float, str, str]:
    left_sentences = split_sentences(text_a)
    right_sentences = split_sentences(text_b)
    best_ratio = 0.0
    best_left = normalize_space(text_a)[:240]
    best_right = normalize_space(text_b)[:240]
    for window in (2, 1):
        left_windows = sentence_windows(left_sentences, window)
        right_windows = sentence_windows(right_sentences, window)
        for left in left_windows:
            for right in right_windows:
                ratio = SequenceMatcher(None, normalize_compact(left), normalize_compact(right)).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_left = left[:240]
                    best_right = right[:240]
    return best_ratio, best_left, best_right


def best_window_semantic(text_a: str, text_b: str, template: dict[str, Any]) -> tuple[float, str, str]:
    left_sentences = split_sentences(text_a)
    right_sentences = split_sentences(text_b)
    best_score = 0.0
    best_left = normalize_space(text_a)[:240]
    best_right = normalize_space(text_b)[:240]
    for window in (4, 3, 2, 1):
        left_windows = sentence_windows(left_sentences, window)
        right_windows = sentence_windows(right_sentences, window)
        for left in left_windows:
            left_tokens = semantic_tokens(left, template, drop_stopwords=True)
            if not left_tokens:
                continue
            left_tf = term_frequency(left_tokens)
            left_set = set(left_tokens)
            for right in right_windows:
                right_tokens = semantic_tokens(right, template, drop_stopwords=True)
                if not right_tokens:
                    continue
                score = (cosine_from_maps(left_tf, term_frequency(right_tokens)) * 0.6) + (
                    jaccard(left_set, set(right_tokens)) * 0.4
                )
                if score > best_score:
                    best_score = score
                    best_left = left[:240]
                    best_right = right[:240]
    return round(best_score, 4), best_left, best_right


def fact_heavy_ratio(text: str, template: dict[str, Any]) -> float:
    tokens = tokenize(text)
    if not tokens:
        return 0.0
    hits = 0
    lowered = normalize_compact(text)
    for pattern in template.get("fact_patterns", []):
        hits += len(re.findall(pattern, lowered, flags=re.IGNORECASE))
    return hits / max(len(tokens), 1)


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", normalize_compact(value))
    return slug.strip("-") or "template"
