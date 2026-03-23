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

    def test_import_source_only_is_saved_in_library(self) -> None:
        service = self.make_service()
        project = service.create_project("Source Library")
        service.set_template_from_text(
            project["id"],
            """
            PRODUCT TITLE

            FEATURES
            The container keeps food protected during takeaway delivery.
            """,
        )
        import_result = service.add_document_from_text(
            project["id"],
            "Nguon da pass",
            """
            Product A

            Features
            The container keeps food protected during takeaway delivery.
            """,
            source_only=True,
        )
        self.assertEqual(import_result["results"], [])
        docs = service.list_documents(project["id"])
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["doc_role"], "source")

    def test_duplicate_message_points_to_existing_library_doc(self) -> None:
        service = self.make_service()
        project = service.create_project("Duplicate Message")
        service.set_template_from_text(
            project["id"],
            """
            PRODUCT TITLE

            INTRO
            This product is designed for professional takeaway use.
            """,
        )
        service.add_document_from_text(
            project["id"],
            "Bai da luu",
            """
            Product A

            Intro
            This product is designed for professional takeaway use.
            """,
            source_url="https://docs.google.com/document/d/example-id/edit",
            source_only=True,
        )
        with self.assertRaises(ValueError) as raised:
            service.add_document_from_text(
                project["id"],
                "Bai trung",
                """
                Product B

                Intro
                This product is designed for professional takeaway use.
                """,
                source_url="https://docs.google.com/document/d/example-id/edit",
            )
        self.assertIn("Kho bài đã import", str(raised.exception))
        self.assertIn("Bai da luu", str(raised.exception))

    def test_pending_docs_are_not_used_as_compare_source_until_approved(self) -> None:
        service = self.make_service()
        project = service.create_project("Approval Gate")
        service.set_template_from_text(
            project["id"],
            """
            PRODUCT TITLE

            FEATURES
            The container stays stackable and supports secure lid closure.
            """,
        )
        first = service.add_document_from_text(
            project["id"],
            "Pending 1",
            """
            Product One

            Features
            The container stays stackable and supports secure lid closure.
            """,
        )
        second = service.add_document_from_text(
            project["id"],
            "Pending 2",
            """
            Product Two

            Features
            The container stays stackable and supports secure lid closure.
            """,
        )
        self.assertEqual(first["results"], [])
        self.assertEqual(second["results"], [])
        service.approve_document(first["document_id"])
        rechecked = service.recheck_document(second["document_id"])
        self.assertTrue(any(item["doc_b_title"] == "Pending 1" for item in rechecked))

    def test_get_document_returns_text_for_pasted_doc(self) -> None:
        service = self.make_service()
        project = service.create_project("Pasted Detail")
        service.set_template_from_text(
            project["id"],
            """
            PRODUCT TITLE

            FEATURES
            The container keeps food protected during takeaway delivery.
            """,
        )
        created = service.add_document_from_text(
            project["id"],
            "Pasted only",
            """
            Pasted title

            Features
            A pasted paragraph that should be visible from detail API.
            """,
        )
        detail = service.get_document(created["document_id"])
        self.assertEqual(detail["title"], "Pasted only")
        self.assertIn("should be visible", detail["raw_text"])
        self.assertEqual(detail["source_url"], "")


if __name__ == "__main__":
    unittest.main()
