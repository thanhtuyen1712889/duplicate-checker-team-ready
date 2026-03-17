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
    "semantic_alias_regex": [
        {"pattern": r"\b(ideal|perfect|great|suitable|suited|designed|made)\s+for\b", "replacement": " fit-for "},
        {"pattern": r"\b(use|used)\s+for\b", "replacement": " fit-for "},
        {"pattern": r"\b(suits?|works?\s+for)\b", "replacement": " fit-for "},
        {"pattern": r"\b(works?\s+well\s+for|good choice for)\b", "replacement": " fit-for "},
        {"pattern": r"\b(take[\s-]?away|take[\s-]?out|to[\s-]?go|grab[\s-]?and[\s-]?go|delivery|courier|on the go|in transit|transit)\b", "replacement": " takeaway-delivery "},
        {"pattern": r"\b(busy lunch services?|peak lunch trade|lunchtime rush)\b", "replacement": " lunch-service "},
        {"pattern": r"\b(containers?|bowls?|trays?|boxes?|pots?|tubs?|cups?|cartons?)\b", "replacement": " pack-unit "},
        {"pattern": r"\b(hot|warm)\b", "replacement": " hot-service "},
        {"pattern": r"\b(cold|chilled)\b", "replacement": " cold-service "},
        {"pattern": r"\b(meals?|dishes?|food)\b", "replacement": " serving "},
        {"pattern": r"\b(leak[\s-]?(resistant|proof)|spill[\s-]?(resistant|proof)|helps? prevent leaks?|helps? avoid spills?|reduces? spills?|reduces? leaks?)\b", "replacement": " leak-resistant "},
        {"pattern": r"\b(secure closure|secure seal|reliable seal|secure lid|tight[\s-]?fitting lid|tight lid|snug lid)\b", "replacement": " secure-closure "},
        {"pattern": r"\b(sturdy|robust|durable|hardwearing)\b", "replacement": " durable "},
        {"pattern": r"\b(light[\s-]?weight|light)\b", "replacement": " lightweight "},
        {"pattern": r"\b(stackable|easy[\s-]?to[\s-]?stack|space[\s-]?saving)\b", "replacement": " stackable "},
        {"pattern": r"\b(save shelf space|storage space|free up storage space|saves? space)\b", "replacement": " storage-space "},
        {"pattern": r"\b(easier to carry|easy to carry|simple to transport|easy to transport)\b", "replacement": " easy-transport "},
        {"pattern": r"\b(enough|sufficient)\s+(internal\s+)?(volume|space|capacity)\b", "replacement": " capacity-fit "},
        {"pattern": r"\b(accommodate|holds?|fit[s]?)\b", "replacement": " capacity-fit "},
        {"pattern": r"\b(show(?:s|ing)? (?:the )?(?:food|contents?) (?:well|clearly)|showcases? contents?|showcases?|display(?:s|ed)? contents?|presents? (?:the )?(?:contents?|food) clearly|clear visibility|crystal clear|transparent (?:finish|look|body)|see[-\s]?through)\b", "replacement": " clear-visibility "},
        {"pattern": r"\b(deli counters?|deli displays?|deli service)\b", "replacement": " deli-display "},
        {"pattern": r"\b(food[\s-]?safe|food contact compliant|food contact approved)\b", "replacement": " food-safe "},
        {"pattern": r"\b(recyclable|easy to recycle|widely recycled)\b", "replacement": " recyclable "},
        {"pattern": r"\b(hot and cold|hot or cold|warm and chilled)\b", "replacement": " temperature-flexible "},
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
        "idea_overlap_yellow": 0.52,
        "idea_overlap_red": 0.64,
        "window_semantic_yellow": 0.68,
        "window_semantic_red": 0.78,
        "semantic_lexical_floor_yellow": 0.08,
        "semantic_lexical_floor_red": 0.10,
        "question_lexical_gate": 0.30,
        "question_semantic_gate": 0.72,
        "question_concept_gate": 0.48,
    },
    "sections": {
        "title": {
            "mode": "exact_only",
            "weight": 0.10,
            "semantic_red": 0.97,
            "exact_span_tokens": 99,
            "cross_check_source": False,
            "cross_check_target": False,
        },
        "source_url": {
            "mode": "ignore",
            "weight": 0.0,
            "cross_check_source": False,
            "cross_check_target": False,
        },
        "hero_heading": {
            "mode": "low_weight",
            "weight": 0.20,
            "semantic_red": 0.93,
            "semantic_yellow": 0.88,
            "exact_span_tokens": 99,
            "cross_check_source": False,
            "cross_check_target": False,
        },
        "intro": {
            "mode": "mixed",
            "weight": 0.80,
            "semantic_red": 0.85,
            "semantic_yellow": 0.80,
            "exact_span_tokens": 26,
            "cross_check_source": False,
            "cross_check_target": True,
        },
        "features": {
            "mode": "strict",
            "weight": 1.00,
            "semantic_red": 0.85,
            "semantic_yellow": 0.80,
            "exact_span_tokens": 24,
            "cross_check_source": True,
            "cross_check_target": True,
        },
        "use_cases": {
            "mode": "strict",
            "weight": 1.00,
            "semantic_red": 0.84,
            "semantic_yellow": 0.78,
            "exact_span_tokens": 24,
            "cross_check_source": True,
            "cross_check_target": True,
        },
        "related_products_table": {
            "mode": "allow_high_overlap",
            "weight": 0.15,
            "semantic_red": 0.96,
            "semantic_yellow": 0.90,
            "exact_span_tokens": 999,
            "cross_check_source": False,
            "cross_check_target": False,
        },
        "supplier": {
            "mode": "ignore",
            "weight": 0.0,
            "semantic_red": 1.00,
            "semantic_yellow": 1.00,
            "exact_span_tokens": 999,
            "cross_check_source": False,
            "cross_check_target": False,
        },
        "faq": {
            "mode": "faq",
            "weight": 0.90,
            "semantic_red": 0.83,
            "semantic_yellow": 0.78,
            "exact_span_tokens": 20,
            "question_lexical_gate": 0.30,
            "question_semantic_gate": 0.70,
            "question_concept_gate": 0.42,
            "cross_check_source": True,
            "cross_check_target": True,
        },
        "conclusion": {
            "mode": "strict",
            "weight": 0.70,
            "semantic_red": 0.84,
            "semantic_yellow": 0.78,
            "exact_span_tokens": 20,
            "cross_check_source": True,
            "cross_check_target": True,
        },
        "full_text": {
            "mode": "mixed",
            "weight": 1.00,
            "semantic_red": 0.86,
            "semantic_yellow": 0.80,
            "exact_span_tokens": 26,
            "cross_check_source": True,
            "cross_check_target": True,
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
    "semantic_alias_regex": [
        {"pattern": r"\b(ideal|perfect|great|suitable|suited|designed|made)\s+for\b", "replacement": " fit-for "},
        {"pattern": r"\b(use|used)\s+for\b", "replacement": " fit-for "},
        {"pattern": r"\b(suits?|works?\s+for)\b", "replacement": " fit-for "},
        {"pattern": r"\b(reduces?|helps? reduce|minimises?|minimizes?)\b", "replacement": " reduce "},
        {"pattern": r"\b(improves?|boosts?|enhances?)\b", "replacement": " improve "},
        {"pattern": r"\b(enough|sufficient)\s+(internal\s+)?(volume|space|capacity)\b", "replacement": " capacity-fit "},
        {"pattern": r"\b(accommodate|holds?|fit[s]?)\b", "replacement": " capacity-fit "},
        {"pattern": r"\b(containers?|bowls?|trays?|boxes?|pots?|tubs?|cups?|cartons?)\b", "replacement": " pack-unit "},
        {"pattern": r"\b(sturdy|robust|durable|hardwearing)\b", "replacement": " durable "},
        {"pattern": r"\b(light[\s-]?weight|light)\b", "replacement": " lightweight "},
    ],
    "fact_patterns": [
        r"\b\d+(?:[.-]\d+)?\s?(ml|mm|cm|gsm|pcs|pc|units|kg|g)\b",
    ],
    "global_thresholds": {
        "exact_span_tokens": 35,
        "near_copy_lexical": 0.76,
        "semantic_yellow": 0.84,
        "semantic_red": 0.90,
        "fact_heavy_ratio": 0.20,
        "idea_overlap_yellow": 0.58,
        "idea_overlap_red": 0.68,
        "window_semantic_yellow": 0.72,
        "window_semantic_red": 0.82,
        "semantic_lexical_floor_yellow": 0.10,
        "semantic_lexical_floor_red": 0.12,
        "question_lexical_gate": 0.32,
        "question_semantic_gate": 0.74,
        "question_concept_gate": 0.50,
    },
    "sections": {
        "title": {
            "mode": "exact_only",
            "weight": 0.10,
            "semantic_red": 0.98,
            "semantic_yellow": 0.94,
            "exact_span_tokens": 999,
            "cross_check_source": False,
            "cross_check_target": False,
        },
        "full_text": {
            "mode": "mixed",
            "weight": 1.00,
            "semantic_red": 0.90,
            "semantic_yellow": 0.84,
            "exact_span_tokens": 35,
            "cross_check_source": True,
            "cross_check_target": True,
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
