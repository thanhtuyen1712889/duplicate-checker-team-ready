import json
import os
import tempfile
import unittest

from duplicate_checker.service import (
    AnalysisResult,
    OptionalEmbeddingClient,
    Section,
    Storage,
    compare_section_sets,
    compare_sections,
    detect_template,
    extract_blocks_from_text,
    extract_sections,
    make_section,
)
from duplicate_checker.template_catalog import builtin_template_map, builtin_templates, deep_copy_template


def other_row() -> dict:
    return {
        "id": 99,
        "document_key": "reference-doc",
        "display_name": "Reference Doc",
    }


class DuplicateRuleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.template = deep_copy_template(builtin_template_map()["pandapak_product_detail_v1"])
        self.embedding_client = OptionalEmbeddingClient()

    def test_features_detects_idea_overlap_with_low_wording_match(self) -> None:
        left = make_section(
            "features",
            (
                "Ideal for takeaway salads and cold rice dishes, this bowl uses a secure closure "
                "to help prevent leaks during delivery while remaining lightweight and stackable."
            ),
            self.template,
        )
        right = make_section(
            "features",
            (
                "Perfect for to-go salads and chilled grain meals, the container has a tight-fitting lid "
                "that reduces spills in transit and stays light and easy to stack."
            ),
            self.template,
        )

        finding = compare_sections(
            section=left,
            other_section=right,
            template=self.template,
            other_row=other_row(),
            embedding_client=self.embedding_client,
        )

        self.assertIsNotNone(finding)
        assert finding is not None
        self.assertIn(finding.rule, {"idea_overlap", "semantic_paraphrase", "semantic_overlap"})
        self.assertGreaterEqual(finding.risk, 0.68)

    def test_faq_detects_same_intent_even_when_question_wording_changes(self) -> None:
        left = Section(
            name="faq",
            text="",
            heading="Frequently Asked Questions",
            mode="faq",
            weight=0.9,
            metadata={
                "items": [
                    {
                        "question": "Can these containers be used for warm takeaway meals?",
                        "answer": (
                            "Yes. They work well for hot food to go, and the secure seal "
                            "helps keep the contents protected during delivery."
                        ),
                    }
                ]
            },
        )
        right = Section(
            name="faq",
            text="",
            heading="Frequently Asked Questions",
            mode="faq",
            weight=0.9,
            metadata={
                "items": [
                    {
                        "question": "Are they suitable for hot food on the go?",
                        "answer": (
                            "Yes. They are a good choice for warm takeaway dishes, and the tight lid "
                            "keeps the food secure in transit."
                        ),
                    }
                ]
            },
        )

        finding = compare_sections(
            section=left,
            other_section=right,
            template=self.template,
            other_row=other_row(),
            embedding_client=self.embedding_client,
        )

        self.assertIsNotNone(finding)
        assert finding is not None
        self.assertIn(finding.rule, {"faq_intent_overlap", "faq_idea_overlap", "faq_exact_span"})
        self.assertGreaterEqual(finding.risk, 0.68)

    def test_intro_still_flags_idea_overlap_when_fact_heavy(self) -> None:
        left = make_section(
            "intro",
            (
                "Made from BPA-free PP, these 750ml bowls are suitable for cold noodles and deli counters. "
                "The clear finish helps showcase contents on display."
            ),
            self.template,
        )
        right = make_section(
            "intro",
            (
                "This 750ml PP bowl is BPA-free and designed for chilled noodles or deli service. "
                "Its transparent look presents the contents clearly."
            ),
            self.template,
        )

        finding = compare_sections(
            section=left,
            other_section=right,
            template=self.template,
            other_row=other_row(),
            embedding_client=self.embedding_client,
        )

        self.assertIsNotNone(finding)
        assert finding is not None
        self.assertIn(finding.rule, {"idea_overlap", "semantic_paraphrase", "semantic_overlap"})
        self.assertGreaterEqual(finding.risk, 0.30)

    def test_use_cases_detects_same_sales_angle_across_short_sentences(self) -> None:
        left = make_section(
            "use_cases",
            (
                "Great for busy lunch services. The stackable bowls save shelf space. "
                "A secure lid makes delivery orders easier to carry."
            ),
            self.template,
        )
        right = make_section(
            "use_cases",
            (
                "Designed for peak lunch trade. These easy-to-stack containers free up storage space. "
                "The snug lid keeps courier orders simple to transport."
            ),
            self.template,
        )

        finding = compare_sections(
            section=left,
            other_section=right,
            template=self.template,
            other_row=other_row(),
            embedding_client=self.embedding_client,
        )

        self.assertIsNotNone(finding)
        assert finding is not None
        self.assertIn(finding.rule, {"idea_overlap", "semantic_paraphrase", "semantic_overlap"})
        self.assertGreaterEqual(finding.risk, 0.68)

    def test_full_text_fallback_detects_semantic_overlap(self) -> None:
        left = make_section(
            "full_text",
            (
                "This BPA-free PP container suits chilled noodle portions and deli counters. "
                "Its clear finish shows the food well and the lid helps prevent spills during takeaway delivery."
            ),
            self.template,
        )
        right = make_section(
            "full_text",
            (
                "Made from BPA-free PP, the bowl works for cold noodle servings and deli displays. "
                "The transparent body presents the contents clearly while the tight lid reduces leaks for to-go orders."
            ),
            self.template,
        )

        finding = compare_sections(
            section=left,
            other_section=right,
            template=self.template,
            other_row=other_row(),
            embedding_client=self.embedding_client,
        )

        self.assertIsNotNone(finding)
        assert finding is not None
        self.assertIn(finding.rule, {"idea_overlap", "semantic_paraphrase", "semantic_overlap"})
        self.assertGreaterEqual(finding.risk, 0.45)

    def test_cross_section_check_flags_strict_copy_against_other_section(self) -> None:
        sections = {
            "features": make_section(
                "features",
                (
                    "Perfect for takeaway salad orders, this bowl stays lightweight, stacks neatly, "
                    "and uses a tight lid to reduce spills during delivery."
                ),
                self.template,
            )
        }
        other_sections = {
            "intro": make_section(
                "intro",
                (
                    "Designed for to-go salad servings, the container remains light, easy to stack, "
                    "and has a secure closure that helps avoid leaks in transit."
                ),
                self.template,
            )
        }

        findings = compare_section_sets(
            sections=sections,
            other_sections=other_sections,
            other_row=other_row(),
            template=self.template,
            embedding_client=self.embedding_client,
        )

        self.assertTrue(findings)
        finding = findings[0]
        self.assertEqual(finding.comparison_scope, "cross_section")
        self.assertEqual(finding.other_section_name, "intro")
        self.assertIn(finding.rule, {"idea_overlap", "semantic_paraphrase", "semantic_overlap"})

    def test_faq_skips_short_yes_opener_in_excerpt(self) -> None:
        left = Section(
            name="faq",
            text="",
            heading="Frequently Asked Questions",
            mode="faq",
            weight=0.9,
            metadata={
                "items": [
                    {
                        "question": "Can these containers be used for warm takeaway meals?",
                        "answer": "Yes. They work well for hot food to go, and the secure seal helps keep the contents protected during delivery.",
                    }
                ]
            },
        )
        right = Section(
            name="faq",
            text="",
            heading="Frequently Asked Questions",
            mode="faq",
            weight=0.9,
            metadata={
                "items": [
                    {
                        "question": "Are they suitable for hot food on the go?",
                        "answer": "Yes. They are a good choice for warm takeaway dishes, and the tight lid keeps the food secure in transit.",
                    }
                ]
            },
        )

        finding = compare_sections(
            section=left,
            other_section=right,
            template=self.template,
            other_row=other_row(),
            embedding_client=self.embedding_client,
        )

        self.assertIsNotNone(finding)
        assert finding is not None
        self.assertNotEqual(finding.excerpt, "Yes.")
        self.assertNotEqual(finding.other_excerpt, "Yes.")

    def test_use_case_generic_framing_only_is_softened(self) -> None:
        left = make_section(
            "use_cases",
            "The following scenarios illustrate situations where the 900ml bowl format fits typical foodservice operations.",
            self.template,
        )
        right = make_section(
            "use_cases",
            "The following scenarios illustrate how the 183mm PP lid fits into professional foodservice workflows.",
            self.template,
        )

        finding = compare_sections(
            section=left,
            other_section=right,
            template=self.template,
            other_row=other_row(),
            embedding_client=self.embedding_client,
        )

        self.assertIsNone(finding)

    def test_use_case_exact_duplicate_framing_still_flags(self) -> None:
        text = "The following scenarios illustrate situations where the 900ml bowl format fits typical foodservice operations."
        left = make_section("use_cases", text, self.template)
        right = make_section("use_cases", text, self.template)

        finding = compare_sections(
            section=left,
            other_section=right,
            template=self.template,
            other_row=other_row(),
            embedding_client=self.embedding_client,
        )

        self.assertIsNotNone(finding)
        assert finding is not None
        self.assertIn(finding.rule, {"exact_span", "near_copy", "semantic_overlap", "idea_overlap"})

    def test_capacity_fit_overlap_gets_reason_label(self) -> None:
        left = make_section(
            "use_cases",
            "The 900ml bowl provides enough internal volume to accommodate these elements without overfilling the container before lid closure.",
            self.template,
        )
        right = make_section(
            "use_cases",
            "A 12oz bowl provides sufficient space for these supplementary portions without using containers intended for full meals.",
            self.template,
        )

        finding = compare_sections(
            section=left,
            other_section=right,
            template=self.template,
            other_row=other_row(),
            embedding_client=self.embedding_client,
        )

        self.assertIsNotNone(finding)
        assert finding is not None
        self.assertEqual(finding.reason_label, "same capacity-fit idea")

    def test_unstyled_google_doc_text_still_detects_template_and_sections(self) -> None:
        text = (
            "https://pandapak.ai/kraft-round-bowl-1090ml-300pcs.html "
            "Kraft Round Bowls 1090ml Product overview "
            "The PandaPak 1090ml kraft round bowl is a high-capacity paper container intended for professional takeaway and delivery use. "
            "Key Features of Kraft Round Bowls 1090ml "
            "High-capacity bowl format for large takeaway portions. Moisture-resistant inner lining for sauce-rich foods. "
            "Key Use Cases of Kraft Round Bowls 1090ml "
            "Suitable for rice bowls, noodle meals, and larger deli portions. "
            "Frequently Asked Questions "
            "Can these bowls handle takeaway delivery? Yes. The sturdy structure helps support food during transport."
        )

        _, blocks = extract_blocks_from_text(text)
        template, _signature = detect_template(blocks, builtin_templates())
        sections, display_name = extract_sections(blocks, template)

        self.assertEqual(template["id"], "pandapak_product_detail_v1")
        self.assertEqual(display_name, "Kraft Round Bowls 1090ml")
        self.assertIn("intro", sections)
        self.assertIn("features", sections)
        self.assertIn("use_cases", sections)
        self.assertIn("faq", sections)
        self.assertTrue(sections["features"].text)
        self.assertEqual(sections["source_url"].text, "https://pandapak.ai/kraft-round-bowl-1090ml-300pcs.html")


class StorageBehaviorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.template = deep_copy_template(builtin_template_map()["pandapak_product_detail_v1"])
        self.temp_dir = tempfile.TemporaryDirectory()
        self.previous_database_url = os.environ.pop("DATABASE_URL", None)
        self.storage = Storage(self.temp_dir.name)

    def tearDown(self) -> None:
        if self.previous_database_url is not None:
            os.environ["DATABASE_URL"] = self.previous_database_url
        self.temp_dir.cleanup()

    def _result(self, *, version: int, text: str, source_url: str = "") -> AnalysisResult:
        sections = {"full_text": make_section("full_text", text, self.template)}
        return AnalysisResult(
            document_key="google-doc-abc123",
            display_name=f"Doc version {version}",
            version=version,
            template_id=self.template["id"],
            template_name=self.template["name"],
            unique_score=88.0,
            total_risk=12.0,
            status="green",
            sections=sections,
            section_risks={"full_text": 12.0},
            findings=[],
            source_name="google-doc-abc123.docx",
            content_hash=f"hash-{version}",
            raw_text=text,
            signature={"score": 1.0},
            source_url=source_url,
            source_kind="google_docs" if source_url else "upload",
        )

    def test_delete_document_restores_previous_version(self) -> None:
        first_id = self.storage.save_result(
            self._result(version=1, text="first snapshot", source_url="https://docs.google.com/document/d/abc123/edit")
        )
        second_id = self.storage.save_result(
            self._result(version=2, text="second snapshot", source_url="https://docs.google.com/document/d/abc123/edit")
        )

        deleted = self.storage.delete_document(second_id)

        self.assertIsNotNone(deleted)
        assert deleted is not None
        self.assertEqual(deleted["restored_document_id"], first_id)
        latest = self.storage.latest_documents()
        self.assertEqual(len(latest), 1)
        self.assertEqual(int(latest[0]["id"]), first_id)

    def test_source_metadata_is_persisted_in_parsed_payload(self) -> None:
        document_id = self.storage.save_result(
            self._result(version=1, text="stored snapshot", source_url="https://docs.google.com/document/d/abc123/edit")
        )

        row = self.storage.get_document(document_id)

        self.assertIsNotNone(row)
        assert row is not None
        payload = json.loads(row["parsed_json"])
        self.assertEqual(payload["source_kind"], "google_docs")
        self.assertEqual(payload["source_url"], "https://docs.google.com/document/d/abc123/edit")


if __name__ == "__main__":
    unittest.main()
