import json
import shutil
import tempfile
import unittest
from pathlib import Path
from uuid import UUID
from zipfile import ZipFile

from rocraters import (
    PyRoCrate,
    PyRoCrateContext,
    prefix_object_id,
    read,
    read_object,
    read_zip,
    validate,
    validate_object,
    validate_zip,
    zip as zip_crate,
)


FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
MINIMAL_CRATE = FIXTURES / "_ro-crate-metadata-minimal.json"
BROKEN_SCHEMA_CRATE = FIXTURES / "_ro-crate-metadata-broken-schema.json"
INVALID_JSON = FIXTURES / "invalid.json"
ZIPPED_CRATE = FIXTURES / "zip_test" / "fixtures.zip"

RO_CRATE_CONTEXT = "https://w3id.org/ro/crate/1.1/context"
REPORT_KEYS = {
    "is_valid",
    "invalid_keys",
    "invalid_ids",
    "invalid_types",
    "error_type",
    "error_message",
}

EXPECTED_ROOT = {
    "id": "./",
    "identifier": "https://doi.org/10.4225/59/59672c09f4a4b",
    "type": "Dataset",
    "datePublished": "2017",
    "name": "Data files associated with the manuscript:Effects of facilitated family case conferencing for ...",
    "description": "Palliative care planning for nursing home residents with advanced dementia ...",
    "license": {
        "id": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/"
    },
}


class TestContextAndConstruction(unittest.TestCase):
    def test_string_context_creates_empty_crate(self):
        context = PyRoCrateContext.from_string(RO_CRATE_CONTEXT)
        crate = PyRoCrate(context)

        self.assertEqual(crate.get_all_context(), [{"@context": RO_CRATE_CONTEXT}])
        self.assertEqual(crate.to_list(), [])
        self.assertIn("PyRoCrate", repr(crate))

    def test_extended_context_lookup_and_uuid(self):
        term_url = "https://example.test/terms/name"
        context = PyRoCrateContext.from_list(
            [RO_CRATE_CONTEXT, {"exampleName": term_url}]
        )
        crate = PyRoCrate(context)

        self.assertEqual(crate.get_specific_context("exampleName"), term_url)

        uuid = crate.add_urn_uuid()

        self.assertEqual(UUID(uuid).version, 7)
        self.assertEqual(crate.get_urn_uuid(), uuid)
        self.assertEqual(
            crate.get_all_context(),
            [
                {"@context": RO_CRATE_CONTEXT},
                {
                    "@context": {
                        "exampleName": term_url,
                        "@base": f"urn:uuid:{uuid}",
                    }
                },
            ],
        )

    def test_extended_context_rejects_unsupported_items(self):
        with self.assertRaises(ValueError):
            PyRoCrateContext.from_list([RO_CRATE_CONTEXT, 42])

    def test_default_crate_contains_required_entities(self):
        crate = PyRoCrate.new_default()
        entities = {entity["id"]: entity for entity in crate.to_list()}

        self.assertEqual(
            crate.get_all_context(),
            [{"@context": "https://w3id.org/ro/crate/1.2/context"}],
        )
        self.assertEqual(set(entities), {"ro-crate-metadata.json", "./"})
        self.assertEqual(entities["ro-crate-metadata.json"]["type"], "CreativeWork")
        self.assertEqual(entities["./"]["type"], "Dataset")
        self.assertTrue(entities["./"]["name"].startswith("Default Crate:"))


class TestReadingAndInspection(unittest.TestCase):
    def test_reads_equivalent_crates_from_file_object_and_zip(self):
        crates = {
            "file": read(str(MINIMAL_CRATE), 0),
            "object": read_object(MINIMAL_CRATE.read_text(encoding="utf-8"), 0),
            "zip": read_zip(str(ZIPPED_CRATE), 0),
        }

        for source, crate in crates.items():
            with self.subTest(source=source):
                self.assertEqual(crate.get_entity("./"), EXPECTED_ROOT)
                self.assertEqual(
                    {entity["id"] for entity in crate.to_list()},
                    {
                        "ro-crate-metadata.json",
                        "./",
                        "https://creativecommons.org/licenses/by-nc-sa/3.0/au/",
                    },
                )

    def test_read_reports_missing_and_malformed_inputs(self):
        with self.assertRaises(OSError):
            read(str(FIXTURES / "missing-ro-crate.json"), 0)

        with self.assertRaises(OSError):
            read_object("Invalid JSON", 0)

    def test_get_entity_rejects_unknown_id(self):
        crate = read(str(MINIMAL_CRATE), 0)

        with self.assertRaises(ValueError):
            crate.get_entity("#missing")


class TestEntityLifecycle(unittest.TestCase):
    @staticmethod
    def new_populated_crate():
        crate = PyRoCrate(PyRoCrateContext.from_string(RO_CRATE_CONTEXT))
        crate.update_descriptor(
            {
                "id": "ro-crate-metadata.json",
                "type": "CreativeWork",
                "conformsTo": {"id": "https://w3id.org/ro/crate/1.1"},
                "about": {"id": "./"},
            }
        )
        crate.update_root(
            {
                "id": "./",
                "type": "Dataset",
                "name": "Test crate",
                "description": "A crate used by the Python contract tests",
                "datePublished": "2026-01-01",
                "license": {"id": "https://creativecommons.org/licenses/by/4.0/"},
                "hasPart": {"id": "data.txt"},
            }
        )
        return crate

    def test_add_overwrite_replace_and_delete_entities(self):
        crate = self.new_populated_crate()
        crate.update_data({"id": "data.txt", "type": "File", "name": "Old name"})
        crate.update_contextual({"id": "#person", "type": "Person", "name": "Ada"})

        crate.update_data({"id": "data.txt", "type": "File", "name": "New name"})
        self.assertEqual(len(crate.to_list()), 4)
        self.assertEqual(crate.get_entity("data.txt")["name"], "New name")

        crate.replace_id("data.txt", "renamed.txt")
        self.assertEqual(crate.get_entity("renamed.txt")["name"], "New name")
        self.assertEqual(crate.get_entity("./")["hasPart"], {"id": "renamed.txt"})
        with self.assertRaises(ValueError):
            crate.get_entity("data.txt")

        crate.delete_entity("#person", False)
        with self.assertRaises(ValueError):
            crate.get_entity("#person")

    def test_prefix_object_id_changes_only_relative_entity_ids(self):
        crate = read(str(MINIMAL_CRATE), 0)
        object_root = "s3://example-bucket/crates/123/"

        prefix_object_id(crate, object_root)
        ids = {entity["id"] for entity in crate.to_list()}

        self.assertIn(f"{object_root}ro-crate-metadata.json", ids)
        self.assertIn(f"{object_root}./", ids)
        self.assertIn(
            "https://creativecommons.org/licenses/by-nc-sa/3.0/au/", ids
        )


class TestPersistence(unittest.TestCase):
    def test_write_round_trip_uses_temporary_output(self):
        crate = PyRoCrate.new_default()

        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "ro-crate-metadata.json"
            crate.write(str(output))

            self.assertTrue(output.is_file())
            self.assertEqual(
                {entity["id"] for entity in read(str(output), 0).to_list()},
                {"ro-crate-metadata.json", "./"},
            )

    def test_zip_round_trip_uses_temporary_crate(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            crate_dir = Path(temp_dir) / "crate"
            crate_dir.mkdir()
            metadata = crate_dir / "ro-crate-metadata.json"
            shutil.copyfile(MINIMAL_CRATE, metadata)

            zip_crate(str(metadata), False, 0, False, False)
            archive = crate_dir / "crate.zip"

            self.assertTrue(archive.is_file())
            with ZipFile(archive) as zip_file:
                self.assertIn("ro-crate-metadata.json", zip_file.namelist())
            self.assertEqual(read_zip(str(archive), 0).get_entity("./"), EXPECTED_ROOT)


class TestValidation(unittest.TestCase):
    def assert_report_shape(self, report):
        self.assertEqual(set(report), REPORT_KEYS)
        self.assertIsInstance(report["is_valid"], bool)
        self.assertIsInstance(report["invalid_keys"], list)
        self.assertIsInstance(report["invalid_ids"], list)
        self.assertIsInstance(report["invalid_types"], list)

    def test_valid_file_object_and_zip_reports(self):
        reports = {
            "file": validate(str(MINIMAL_CRATE)),
            "object": validate_object(MINIMAL_CRATE.read_text(encoding="utf-8")),
            "zip": validate_zip(str(ZIPPED_CRATE)),
        }

        for source, report in reports.items():
            with self.subTest(source=source):
                self.assert_report_shape(report)
                self.assertTrue(report["is_valid"])
                self.assertEqual(report["invalid_keys"], [])
                self.assertEqual(report["invalid_ids"], [])
                self.assertEqual(report["invalid_types"], [])
                self.assertIsNone(report["error_type"])
                self.assertIsNone(report["error_message"])

    def test_invalid_schema_key_is_reported(self):
        report = validate(str(BROKEN_SCHEMA_CRATE))

        self.assert_report_shape(report)
        self.assertFalse(report["is_valid"])
        self.assertIn("nonschemakey", report["invalid_keys"])
        self.assertEqual(report["invalid_ids"], [])
        self.assertIsNone(report["error_type"])

    def test_dangling_local_id_is_reported(self):
        crate_object = {
            "@context": RO_CRATE_CONTEXT,
            "@graph": [
                {
                    "@type": "CreativeWork",
                    "@id": "ro-crate-metadata.json",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
                    "about": {"@id": "./"},
                },
                {
                    "@id": "./",
                    "@type": "Dataset",
                    "name": "Example",
                    "description": "Example",
                    "datePublished": "2026-01-01",
                    "license": {"@id": "https://creativecommons.org/licenses/by/4.0/"},
                },
                {
                    "@id": "#run",
                    "@type": "CreateAction",
                    "instrument": {"@id": "#ghost"},
                },
            ],
        }

        report = validate_object(json.dumps(crate_object))

        self.assert_report_shape(report)
        self.assertFalse(report["is_valid"])
        self.assertEqual(report["invalid_keys"], [])
        self.assertEqual(report["invalid_ids"], ["#ghost"])
        self.assertEqual(report["invalid_types"], [])
        self.assertIsNone(report["error_type"])

    def test_validation_returns_structured_read_errors(self):
        reports = {
            "invalid file": (validate(str(INVALID_JSON)), "JsonError"),
            "missing file": (
                validate(str(FIXTURES / "missing-ro-crate.json")),
                "IoError",
            ),
            "invalid object": (validate_object("Invalid JSON"), "JsonError"),
        }

        for source, (report, error_type) in reports.items():
            with self.subTest(source=source):
                self.assert_report_shape(report)
                self.assertFalse(report["is_valid"])
                self.assertEqual(report["invalid_keys"], [])
                self.assertEqual(report["invalid_ids"], [])
                self.assertEqual(report["invalid_types"], [])
                self.assertEqual(report["error_type"], error_type)
                self.assertTrue(report["error_message"])


if __name__ == "__main__":
    unittest.main()
