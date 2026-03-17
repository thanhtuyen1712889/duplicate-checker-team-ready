from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from smart_duplicate_core import SmartDuplicateService, default_allowed_zone_config, parse_sections, strip_allowed_content


class SmartDuplicateCoreTests(unittest.TestCase):
    def make_service(self) -> SmartDuplicateService:
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self.temp_dir.name) / "test.sqlite3"
        return SmartDuplicateService(db_path)

    def tearDown(self) -> None:
        temp_dir = getattr(self, "temp_dir", None)
        if temp_dir is not None:
            temp_dir.cleanup()

    def test_allowed_phrase_detector_does_not_capture_short_pronouns(self) -> None:
        text = """
        PRODUCT TITLE

        INTRO
        This product is designed for professional takeaway and delivery use.
        It supports fast-moving kitchens with a dependable packaging format.
        """
        title, sections, _warnings = parse_sections(text)
        config = default_allowed_zone_config(text, sections, title)
        self.assertNotIn("It", config["allowed_phrases"])

        stripped = strip_allowed_content(
            "intro",
            "It supports high-volume kitchen lines and suits retail packing workflows.",
            config,
        )
        self.assertIn("suits retail packing workflows", stripped["protected_text"])

    def test_backup_zip_roundtrip_restores_project(self) -> None:
        service = self.make_service()
        project = service.create_project("Roundtrip")
        service.set_template_from_text(
            project["id"],
            """
            PRODUCT TITLE

            INTRO
            This product is designed for professional takeaway and delivery use.

            FEATURES
            The container stays stackable and supports secure lid closure during delivery service.
            """,
        )
        service.add_document_from_text(
            project["id"],
            "Doc 1",
            """
            Product A

            Intro
            This product is designed for professional takeaway and delivery use.

            Features
            The container stays stackable and supports secure lid closure during delivery service.
            """,
        )
        backup = service.export_project_zip(project["id"])
        restored = service.import_project_zip(backup)

        restored_docs = service.list_documents(restored["id"])
        self.assertEqual(restored["doc_count"], 1)
        self.assertEqual(len(restored_docs), 1)

    def test_self_test_expectations_hold(self) -> None:
        service = self.make_service()
        summary = service.self_test()
        self.assertTrue(summary["identical_expected"])
        self.assertTrue(summary["paraphrase_expected"])
        self.assertTrue(summary["different_expected"])
        self.assertTrue(summary["spec_guard_expected"])


if __name__ == "__main__":
    unittest.main()
