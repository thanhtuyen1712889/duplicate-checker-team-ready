"""Runtime template definitions for the duplicate checker."""

from __future__ import annotations

import copy

PANDAPAK_TEMPLATE = {
    "id": "pandapak_product_detail_v1",
    "name": "PandaPak Product Detail",
    "detection_threshold": 0.85,
    "heading_patterns": [
        r"for professional takeaway and delivery",
        r"key features|features of",
        r"key use cases|use cases of",
        r"compatible|related products",
        r"trusted supplier|supplier for professional foodservice packaging",
        r"frequently asked questions",
    ],
    "section_patterns": {
        "features": [r"key features", r"features of"],
        "use_cases": [r"key use cases", r"use cases of"],
        "related_products_table": [r"compatible", r"related products"],
        "supplier": [r"trusted supplier", r"supplier for professional foodservice packaging"],
        "faq": [r"frequently asked questions"],
    },
    "approved_reuse_regex": [
        r"free mainland uk delivery.*above £99",
        r"produced in compliance with uk and eu food contact",
        r"supplied in cases of \d+",
        r"no fixed minimum order quantity",
        r"samples are typically dispatched within 1.?2 working days",
    ],
    "fact_patterns": [
        r"\b\d+(?:[.-]\d+)?\s?(ml|mm|cm|gsm|pcs|pc|units|kg|g)\b",
        r"\b(150mm|183mm|500ml|750ml|900ml|1000ml)\b",
        r"\b(bpa-free|pfas|pet|pp|pe-coated|uk|eu)\b",
    ],
    "global_thresholds": {
        "exact_span_tokens": 35,
        "near_copy_lexical": 0.72,
        "semantic_yellow": 0.82,
        "semantic_red": 0.88,
        "fact_heavy_ratio": 0.16,
    },
    "sections": {
        "title": {
            "mode": "exact_only",
            "weight": 0.10,
            "semantic_red": 0.97,
            "exact_span_tokens": 99,
        },
        "source_url": {"mode": "ignore", "weight": 0.0},
        "hero_heading": {
            "mode": "low_weight",
            "weight": 0.20,
            "semantic_red": 0.93,
            "semantic_yellow": 0.88,
            "exact_span_tokens": 99,
        },
        "intro": {
            "mode": "mixed",
            "weight": 0.80,
            "semantic_red": 0.87,
            "semantic_yellow": 0.81,
            "exact_span_tokens": 30,
        },
        "features": {
            "mode": "strict",
            "weight": 1.00,
            "semantic_red": 0.88,
            "semantic_yellow": 0.83,
            "exact_span_tokens": 28,
        },
        "use_cases": {
            "mode": "strict",
            "weight": 1.00,
            "semantic_red": 0.86,
            "semantic_yellow": 0.80,
            "exact_span_tokens": 28,
        },
        "related_products_table": {
            "mode": "allow_high_overlap",
            "weight": 0.15,
            "semantic_red": 0.96,
            "semantic_yellow": 0.90,
            "exact_span_tokens": 999,
        },
        "supplier": {
            "mode": "ignore",
            "weight": 0.0,
            "semantic_red": 1.00,
            "semantic_yellow": 1.00,
            "exact_span_tokens": 999,
        },
        "faq": {
            "mode": "faq",
            "weight": 0.90,
            "semantic_red": 0.87,
            "semantic_yellow": 0.82,
            "exact_span_tokens": 24,
        },
        "conclusion": {
            "mode": "strict",
            "weight": 0.70,
            "semantic_red": 0.86,
            "semantic_yellow": 0.80,
            "exact_span_tokens": 24,
        },
        "full_text": {
            "mode": "mixed",
            "weight": 1.00,
            "semantic_red": 0.88,
            "semantic_yellow": 0.82,
            "exact_span_tokens": 30,
        },
    },
    "status_thresholds": {
        "green_risk_max": 25.0,
        "yellow_risk_max": 50.0,
    },
}

GENERIC_TEMPLATE = {
    "id": "generic_text_v1",
    "name": "Generic Text",
    "detection_threshold": 0.0,
    "heading_patterns": [],
    "section_patterns": {},
    "approved_reuse_regex": [],
    "fact_patterns": [
        r"\b\d+(?:[.-]\d+)?\s?(ml|mm|cm|gsm|pcs|pc|units|kg|g)\b",
    ],
    "global_thresholds": {
        "exact_span_tokens": 35,
        "near_copy_lexical": 0.76,
        "semantic_yellow": 0.84,
        "semantic_red": 0.90,
        "fact_heavy_ratio": 0.20,
    },
    "sections": {
        "title": {
            "mode": "exact_only",
            "weight": 0.10,
            "semantic_red": 0.98,
            "semantic_yellow": 0.94,
            "exact_span_tokens": 999,
        },
        "full_text": {
            "mode": "mixed",
            "weight": 1.00,
            "semantic_red": 0.90,
            "semantic_yellow": 0.84,
            "exact_span_tokens": 35,
        },
    },
    "status_thresholds": {
        "green_risk_max": 25.0,
        "yellow_risk_max": 50.0,
    },
}

BUILTIN_TEMPLATES = [PANDAPAK_TEMPLATE]
FALLBACK_TEMPLATE = GENERIC_TEMPLATE


def deep_copy_template(template: dict) -> dict:
    return copy.deepcopy(template)


def builtin_templates() -> list[dict]:
    return [deep_copy_template(template) for template in BUILTIN_TEMPLATES]


def builtin_template_map() -> dict[str, dict]:
    return {template["id"]: deep_copy_template(template) for template in BUILTIN_TEMPLATES + [FALLBACK_TEMPLATE]}
