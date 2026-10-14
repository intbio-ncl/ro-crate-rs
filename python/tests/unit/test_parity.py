"""Tests for the bindings that expose the rest of the ro-crate-rs core API."""

import contextlib
import functools
import http.server
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path

import rocraters
from rocraters import (
    PyRdfGraph,
    PyRoCrate,
    PyRoCrateContext,
    from_rdf,
    load_schema,
    read,
    read_object,
    read_remote,
    read_zip,
    validate_remote,
    zip,
)

FIXTURES = Path.cwd() / "tests" / "fixtures"
BASE = "https://example.org/crate/"
RESTORED_IDS = ["#alice", "./", "./data.csv", "ro-crate-metadata.json"]


def lines(rdf):
    """Triples are unordered, so compare serialisations line by line"""
    return sorted(rdf.splitlines())


def crate_json(context="https://w3id.org/ro/crate/1.3/context", extra=None):
    graph = [
        {
            "@id": "ro-crate-metadata.json",
            "@type": "CreativeWork",
            "conformsTo": {"@id": "https://w3id.org/ro/crate/1.3"},
            "about": {"@id": "./"},
        },
        {
            "@id": "./",
            "@type": "Dataset",
            "name": "Example",
            "description": "Example crate",
            "datePublished": "2024",
            "license": {"@id": "https://spdx.org/licenses/MIT"},
            "hasPart": [{"@id": "data.csv"}],
        },
        {"@id": "data.csv", "@type": "File", "name": "Data", "author": {"@id": "#alice"}},
        {"@id": "#alice", "@type": "Person", "name": "Alice"},
    ]
    return json.dumps({"@context": context, "@graph": graph + (extra or [])})


def example_crate(**kwargs):
    return read_object(crate_json(**kwargs), 0)


def subcrate_refs(*ids):
    """Data entities that reference other RO-Crates"""
    return [
        {"@id": id, "@type": "Dataset", "conformsTo": {"@id": "https://w3id.org/ro/crate"}}
        for id in ids
    ]


@contextlib.contextmanager
def serve_directory(files):
    """Serves `files` ({name: text}) over HTTP on localhost. `{base}` in a file
    is replaced with the server's URL."""
    with tempfile.TemporaryDirectory() as tmp:
        handler = functools.partial(QuietHandler, directory=tmp)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        url = f"http://127.0.0.1:{server.server_address[1]}"
        for name, text in files.items():
            Path(tmp, name).write_text(text.replace("{base}", url))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield url
        finally:
            server.shutdown()
            server.server_close()


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


class TestContext(unittest.TestCase):
    def test_from_embedded(self):
        context = PyRoCrateContext.from_embedded([{"name": "http://schema.org/name"}])
        crate = PyRoCrate(context)
        self.assertEqual(crate.get_context_items(), ["name"])

    def test_from_list_rejects_other_types(self):
        with self.assertRaises(ValueError):
            PyRoCrateContext.from_list([1])

    def test_from_list(self):
        context = PyRoCrateContext.from_list(
            ["https://w3id.org/ro/crate/1.3/context", {"@base": "https://example.org/"}]
        )
        crate = PyRoCrate(context)
        self.assertEqual(crate.get_rocrate_version(), "1.3")
        self.assertEqual(crate.get_specific_context("@base"), "https://example.org/")

    def test_urn_with_non_urn_base(self):
        crate = example_crate()
        crate.add_context({"@base": "https://example.org/"})
        self.assertIsNone(crate.get_urn_uuid())
        # add_urn_uuid replaces the existing @base
        urn = crate.add_urn_uuid()
        self.assertEqual(crate.get_specific_context("@base"), f"urn:uuid:{urn}")

    def test_add_urn_without_embedded_terms(self):
        crate = example_crate(context=["https://w3id.org/ro/crate/1.3/context"])
        urn = crate.add_urn_uuid()
        self.assertEqual(crate.get_urn_uuid(), urn)

    def test_add_urn_needs_room_for_a_base(self):
        crate = PyRoCrate(PyRoCrateContext.from_embedded([{"name": "http://schema.org/name"}]))
        with self.assertRaises(ValueError):
            crate.add_urn_uuid()

    def test_add_replace_remove_context(self):
        crate = example_crate()
        crate.add_context({"@base": "https://example.org/a/"})
        self.assertEqual(crate.get_specific_context("@base"), "https://example.org/a/")

        crate.replace_context({"@base": "https://example.org/b/"}, {"@base": "https://example.org/a/"})
        self.assertEqual(crate.get_specific_context("@base"), "https://example.org/b/")

        crate.remove_context({"@base": "https://example.org/b/"})
        self.assertIsNone(crate.get_specific_context("@base"))

        crate.add_context("https://example.org/extra-context")
        contexts = [item["@context"] for item in crate.get_all_context()]
        self.assertIn("https://example.org/extra-context", contexts)

    def test_remove_missing_context_raises(self):
        crate = example_crate()
        with self.assertRaises(ValueError):
            crate.remove_context("https://example.org/not-there")

    def test_rocrate_version(self):
        self.assertEqual(example_crate().get_rocrate_version(), "1.3")
        crate = read(str(FIXTURES / "_ro-crate-metadata-minimal.json"), 0)
        self.assertEqual(crate.get_rocrate_version(), "1.1")
        self.assertIsNone(example_crate(context="https://example.org/ctx").get_rocrate_version())

    def test_urn_uuid(self):
        crate = example_crate()
        self.assertIsNone(crate.get_urn_uuid())
        urn = crate.add_urn_uuid()
        self.assertEqual(crate.get_urn_uuid(), urn)
        self.assertEqual(crate.get_specific_context("@base"), f"urn:uuid:{urn}")
        # Adding again keeps the existing identifier
        self.assertEqual(crate.add_urn_uuid(), urn)

    def test_missing_specific_context_is_none(self):
        self.assertIsNone(example_crate().get_specific_context("@base"))


class TestQueries(unittest.TestCase):
    def setUp(self):
        self.crate = example_crate()

    def test_get_all_ids(self):
        self.assertEqual(
            self.crate.get_all_ids(), ["ro-crate-metadata.json", "./", "data.csv", "#alice"]
        )

    def test_get_all_properties(self):
        self.assertEqual(self.crate.get_all_properties(), ["author", "hasPart", "name"])

    def test_get_all_property_values(self):
        values = dict(self.crate.get_all_property_values("name"))
        self.assertEqual(values["./"], "Example")
        self.assertEqual(values["data.csv"], "Data")
        self.assertEqual(values["#alice"], "Alice")

    def test_get_specific_value(self):
        self.assertEqual(self.crate.get_specific_value("Alice"), [("#alice", "name")])

    def test_get_linked_ids(self):
        self.assertEqual(self.crate.get_linked_ids("data.csv"), ["#alice"])
        with self.assertRaises(ValueError):
            self.crate.get_linked_ids("missing")

    def test_validate_in_memory(self):
        self.assertTrue(self.crate.validate()["is_valid"])
        self.crate.add_dynamic_entity_property("data.csv", {"notAKey": "x"})
        report = self.crate.validate()
        self.assertFalse(report["is_valid"])
        self.assertEqual(report["invalid_keys"], ["notAKey"])

    def test_to_json_roundtrip(self):
        text = self.crate.to_json()
        self.assertIn("\n", text)
        self.assertNotIn("\n", self.crate.to_json(pretty=False))
        self.assertEqual(read_object(text, 0).get_all_ids(), self.crate.get_all_ids())


class TestMutation(unittest.TestCase):
    def setUp(self):
        self.crate = example_crate()

    def test_add_dynamic_entity_property_types(self):
        updated = self.crate.add_dynamic_entity_property(
            "data.csv",
            {
                "contentSize": 42,
                "ratio": 0.5,
                "flag": True,
                "nothing": None,
                "nested": {"a": 1},
                "creator": {"id": "#alice"},
                "keywords": ["a", "b"],
            },
        )
        self.assertTrue(updated)
        entity = self.crate.get_entity("data.csv")
        self.assertEqual(entity["contentSize"], 42)
        self.assertEqual(entity["ratio"], 0.5)
        self.assertIs(entity["flag"], True)
        self.assertIsNone(entity["nothing"])
        self.assertEqual(entity["nested"], {"a": 1})
        self.assertEqual(entity["creator"], {"id": "#alice"})
        self.assertEqual(entity["keywords"], ["a", "b"])

        written = json.loads(self.crate.to_json())["@graph"][2]
        self.assertIsNone(written["nothing"])
        self.assertEqual(written["creator"], {"@id": "#alice"})

    def test_json_scalars_read_back(self):
        crate = example_crate(
            extra=[{"@id": "#flags", "@type": "Thing", "on": True, "off": False, "none": None}]
        )
        entity = crate.get_entity("#flags")
        self.assertIs(entity["on"], True)
        self.assertIs(entity["off"], False)
        self.assertIsNone(entity["none"])

    def test_value_shapes_read_and_write_back(self):
        values = {
            "strings": ["a", "b"],
            "ints": [1, 2],
            "floats": [1.5, 2.5],
            "bools": [True, False],
            "mixed": [1, "a", True],
            "objects": [{"name": "x"}, {"name": "y"}],
            "links": [{"@id": "#a"}, {"@id": "#b"}],
            "nested": {"inner": {"deep": 1}},
            "nested_list": [[1, 2], [3]],
            "empty": [],
            "nulls": [None],
            # An object with more than an @id is not a link, so keeps "@id"
            "described": {"@id": "#a", "name": "x"},
        }
        crate = example_crate(extra=[{"@id": "#values", "@type": "Thing", **values}])
        entity = crate.get_entity("#values")
        self.assertEqual(entity["links"], [{"id": "#a"}, {"id": "#b"}])
        for key in values.keys() - {"links"}:
            self.assertEqual(entity[key], values[key], key)

        crate.update_contextual(entity)
        written = next(e for e in json.loads(crate.to_json())["@graph"] if e["@id"] == "#values")
        for key, value in values.items():
            self.assertEqual(written[key], value, key)

    def test_add_dynamic_entity_property_missing_entity(self):
        self.assertFalse(self.crate.add_dynamic_entity_property("missing", {"a": "b"}))

    def test_remove_dynamic_entity_property(self):
        self.assertTrue(self.crate.remove_dynamic_entity_property("data.csv", "author"))
        self.assertNotIn("author", self.crate.get_entity("data.csv"))
        self.assertFalse(self.crate.remove_dynamic_entity_property("missing", "author"))

    def test_add_and_remove_type(self):
        self.crate.add_type("data.csv", "SoftwareSourceCode")
        self.assertEqual(self.crate.get_entity("data.csv")["type"], ["File", "SoftwareSourceCode"])
        self.crate.remove_type("data.csv", "File")
        self.assertEqual(self.crate.get_entity("data.csv")["type"], "SoftwareSourceCode")

        with self.assertRaises(ValueError):
            self.crate.remove_type("data.csv", "SoftwareSourceCode")
        with self.assertRaises(ValueError):
            self.crate.remove_type("data.csv", "NotAType")
        with self.assertRaises(ValueError):
            self.crate.add_type("missing", "File")
        with self.assertRaises(ValueError):
            self.crate.remove_type("missing", "File")

    def test_add_data_to_partof_root(self):
        self.crate.add_data_to_partof_root("other.csv")
        self.crate.add_data_to_partof_root("data.csv")
        self.assertEqual(
            self.crate.get_entity("./")["hasPart"], [{"id": "data.csv"}, {"id": "other.csv"}]
        )

    def test_add_data_to_partof_root_keeps_single_part(self):
        self.crate.add_dynamic_entity_property("./", {"hasPart": {"id": "data.csv"}})
        self.crate.add_data_to_partof_root("other.csv")
        self.assertEqual(
            self.crate.get_entity("./")["hasPart"], [{"id": "data.csv"}, {"id": "other.csv"}]
        )

    def test_prefix_object_id(self):
        rocraters.prefix_object_id(self.crate, "s3://bucket/")
        self.assertIn("s3://bucket/data.csv", self.crate.get_all_ids())


class TestSubcrates(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        # The crate references sub/, which references sub/nested/, and missing/
        root = Path(self.tmp.name)
        nested = root / "sub" / "nested"
        nested.mkdir(parents=True)
        (nested / "ro-crate-metadata.json").write_text(crate_json())
        (root / "sub" / "ro-crate-metadata.json").write_text(
            crate_json(extra=subcrate_refs(f"{nested}/"))
        )
        self.subcrate_id = f"{root / 'sub'}/"
        self.crate = example_crate(extra=subcrate_refs(self.subcrate_id, "missing/"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_get_subcrates(self):
        ids = [entity["id"] for entity in self.crate.get_subcrates()]
        self.assertEqual(ids, [self.subcrate_id, "missing/"])

    def test_fetch_subcrates_strict(self):
        with self.assertRaises(OSError):
            self.crate.fetch_subcrates()

    def test_fetch_subcrates_lenient(self):
        subcrates = self.crate.fetch_subcrates(strict=False)
        self.assertEqual(len(subcrates), 1)
        self.assertIn("data.csv", subcrates[0].get_all_ids())
        self.assertEqual(len(self.crate.fetch_subcrates(recursive=True, strict=False)), 2)

    def test_fetch_remote_subcrates(self):
        files = {
            "sub.json": crate_json(extra=subcrate_refs("{base}/nested.json")),
            "nested.json": crate_json(),
        }
        with serve_directory(files) as url:
            crate = example_crate(extra=subcrate_refs(f"{url}/sub.json", f"{url}/missing.json"))
            with self.assertRaises(OSError):
                crate.fetch_subcrates()
            self.assertEqual(len(crate.fetch_subcrates(strict=False)), 0)
            self.assertEqual(len(crate.fetch_subcrates(strict=False, allow_remote=True)), 1)
            self.assertEqual(len(crate.fetch_subcrates(recursive=True, strict=False, allow_remote=True)), 2)


class TestRdf(unittest.TestCase):
    def setUp(self):
        self.crate = example_crate()

    def test_to_rdf_formats(self):
        turtle = self.crate.to_rdf(base=BASE)
        self.assertIn("<https://example.org/crate/data.csv>", turtle)
        ntriples = self.crate.to_rdf("ntriples", base=BASE)
        self.assertEqual(lines(ntriples), lines(self.crate.to_rdf("nt", base=BASE)))
        self.assertTrue(all(line.endswith(" .") for line in ntriples.splitlines()))
        self.assertIn("rdf:RDF", self.crate.to_rdf("rdfxml", base=BASE))
        self.assertTrue(self.crate.to_rdf("nquads", base=BASE))

    def test_to_rdf_errors(self):
        with self.assertRaises(ValueError):
            self.crate.to_rdf()  # strict: no @base for relative IDs
        with self.assertRaises(ValueError):
            self.crate.to_rdf(base=BASE, allow_relative=True)
        with self.assertRaises(ValueError):
            self.crate.to_rdf("json", base=BASE)
        with self.assertRaises(ValueError):
            self.crate.to_rdf(base="urn:uuid:1234")
        self.crate.add_urn_uuid()
        with self.assertRaises(ValueError):
            self.crate.to_rdf()  # urn:uuid bases cannot resolve relative IRIs
        with self.assertRaises(ValueError):
            self.crate.to_rdf_graph()

    def test_expand_with_urn_base(self):
        self.crate.add_urn_uuid()
        self.assertEqual(self.crate.expand_term("name"), "http://schema.org/name")
        self.assertEqual(self.crate.expand_term("data.csv"), "data.csv")
        with self.assertRaises(ValueError):
            self.crate.expand_term("data.csv", strict=True)

    def test_remote_contexts(self):
        terms = json.dumps({"@context": {"foo": "https://example.org/terms#foo"}})
        with serve_directory({"terms.json": terms}) as url:
            crate = example_crate(
                context=["https://w3id.org/ro/crate/1.3/context", f"{url}/terms.json"]
            )
            crate.add_dynamic_entity_property("data.csv", {"foo": "bar"})
            with self.assertRaises(ValueError):
                crate.expand_term("foo")
            self.assertEqual(
                crate.expand_term("foo", allow_remote=True), "https://example.org/terms#foo"
            )
            self.assertEqual(
                crate.compact_iri("https://example.org/terms#foo", allow_remote=True), "foo"
            )
            ntriples = crate.to_rdf("nt", base=BASE, allow_remote=True)
            self.assertIn('<https://example.org/terms#foo> "bar"', ntriples)

    def test_invalid_preloaded_context(self):
        with self.assertRaises(ValueError):
            self.crate.to_rdf(base=BASE, contexts={"https://example.org/terms": "not json"})

    def test_allow_relative(self):
        self.assertIn("data.csv", self.crate.to_rdf("nt", allow_relative=True))

    def test_from_rdf_roundtrip(self):
        # Relative data entity IDs come back with an explicit "./" prefix
        restored = from_rdf(self.crate.to_rdf(base=BASE), "turtle")
        self.assertEqual(sorted(restored.get_all_ids()), RESTORED_IDS)
        self.assertEqual(restored.get_entity("#alice")["name"], "Alice")

        with self.assertRaises(ValueError):
            from_rdf("not turtle", "turtle")

    def test_rdf_graph(self):
        graph = self.crate.to_rdf_graph(base=BASE)
        self.assertIsInstance(graph, PyRdfGraph)
        self.assertGreater(len(graph), 0)
        self.assertEqual(repr(graph), f"PyRdfGraph(triples: {len(graph)})")
        with self.assertRaises(ValueError):
            graph.serialize("json")
        self.assertEqual(lines(graph.serialize("nt")), lines(self.crate.to_rdf("nt", base=BASE)))
        self.assertEqual(sorted(graph.to_rocrate().get_all_ids()), RESTORED_IDS)
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "crate.ttl")
            graph.write(path)
            self.assertEqual(sorted(from_rdf(Path(path).read_text()).get_all_ids()), RESTORED_IDS)

    def test_expand_and_compact(self):
        self.assertEqual(self.crate.expand_term("name"), "http://schema.org/name")
        self.assertEqual(self.crate.compact_iri("http://schema.org/name"), "name")
        self.assertEqual(self.crate.expand_term("data.csv"), "data.csv")
        with self.assertRaises(ValueError):
            self.crate.expand_term("data.csv", strict=True)

    def test_preloaded_contexts(self):
        crate = example_crate(
            context=["https://w3id.org/ro/crate/1.3/context", "https://example.org/terms"]
        )
        terms = {"https://example.org/terms": json.dumps({"@context": {"foo": "https://example.org/terms#foo"}})}
        with self.assertRaises(ValueError):
            crate.expand_term("foo")
        self.assertEqual(crate.expand_term("foo", contexts=terms), "https://example.org/terms#foo")


class TestIo(unittest.TestCase):
    def test_load_schema(self):
        schema = load_schema()
        self.assertEqual(schema["@id"], "https://w3id.org/ro/crate/1.3/context")
        self.assertEqual(schema["@context"]["name"], "http://schema.org/name")
        self.assertEqual(load_schema("1.1")["version"].split(".")[:2], ["1", "1"])
        with self.assertRaises(ValueError):
            load_schema("9.9")

    def test_read_remote_invalid_url(self):
        with self.assertRaises(ValueError):
            read_remote("not a url", 0)
        with self.assertRaises(ValueError):
            validate_remote("not a url")

    def test_read_and_validate_remote(self):
        with serve_directory({"ro-crate-metadata.json": crate_json()}) as url:
            crate = read_remote(f"{url}/ro-crate-metadata.json", 2)
            self.assertIn("data.csv", crate.get_all_ids())
            self.assertTrue(validate_remote(f"{url}/ro-crate-metadata.json")["is_valid"])

            with self.assertRaises(OSError):
                read_remote(f"{url}/missing.json", 0)
            report = validate_remote(f"{url}/missing.json")
            self.assertFalse(report["is_valid"])
            self.assertEqual(report["error_type"], "JsonError")

    def test_read_zip_errors(self):
        with self.assertRaises(OSError):
            read_zip(str(FIXTURES / "missing.zip"), 0)
        with self.assertRaises(OSError):
            read_zip(str(FIXTURES / "_ro-crate-metadata-minimal.json"), 0)
        report = rocraters.validate_zip(str(FIXTURES / "missing.zip"))
        self.assertFalse(report["is_valid"])
        self.assertEqual(report["error_type"], "IoError")

    def test_write_errors(self):
        with self.assertRaises(OSError):
            example_crate().write("/nonexistent-dir/ro-crate-metadata.json")

    def test_write_default_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                example_crate().write()
                self.assertTrue(Path("ro-crate-metadata.json").exists())
            finally:
                os.chdir(cwd)

    def test_zip_errors(self):
        with self.assertRaises(OSError):
            zip(str(FIXTURES / "missing" / "ro-crate-metadata.json"), False, 0, False, False)

    @unittest.skipUnless(hasattr(rocraters, "write_csv"), "built without the parquet feature")
    def test_tabular_export(self):
        crate = example_crate()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                rocraters.write_csv(crate, os.path.join(tmp, "crate.csv"))
            with self.assertRaises(ValueError):
                rocraters.write_parquet(crate, os.path.join(tmp, "crate.parquet"))

            crate.add_urn_uuid()
            rocraters.write_csv(crate, os.path.join(tmp, "crate.csv"))
            rocraters.write_parquet(crate, os.path.join(tmp, "crate.parquet"))
            header = Path(tmp, "crate.csv").read_text().splitlines()[0]
            self.assertEqual(header, "uuid,id,key,value")
            self.assertGreater(Path(tmp, "crate.parquet").stat().st_size, 0)


class TestEntityUpdates(unittest.TestCase):
    """The update, replace, delete and write methods that predate this test file"""

    def setUp(self):
        self.crate = example_crate()

    def test_new_default(self):
        crate = PyRoCrate.new_default()
        self.assertEqual(crate.get_all_ids(), ["ro-crate-metadata.json", "./"])
        self.assertEqual(crate.get_rocrate_version(), "1.3")
        self.assertTrue(crate.validate()["is_valid"])

    def test_update_data_inserts_and_overwrites(self):
        self.crate.update_data({"id": "new.csv", "type": "File", "name": "New"})
        self.assertEqual(self.crate.get_entity("new.csv")["name"], "New")

        data = self.crate.get_entity("data.csv")
        data["name"] = "Renamed"
        data.pop("author")
        self.crate.update_data(data)
        self.assertEqual(
            self.crate.get_entity("data.csv"), {"id": "data.csv", "type": "File", "name": "Renamed"}
        )
        self.assertEqual(len(self.crate.get_all_ids()), 5)

    def test_update_contextual(self):
        self.crate.update_contextual({"id": "#org", "type": "Organization", "name": "Org"})
        self.assertEqual(self.crate.get_entity("#org")["name"], "Org")
        self.crate.update_contextual(
            {"id": "#alice", "type": ["Person", "Researcher"], "name": "Alice"}
        )
        self.assertEqual(self.crate.get_entity("#alice")["type"], ["Person", "Researcher"])

    def test_update_root(self):
        root = self.crate.get_entity("./")
        root["name"] = "Renamed crate"
        root["keywords"] = ["a", "b"]
        self.crate.update_root(root)
        updated = self.crate.get_entity("./")
        self.assertEqual(updated["name"], "Renamed crate")
        self.assertEqual(updated["keywords"], ["a", "b"])
        self.assertEqual(updated["license"], {"id": "https://spdx.org/licenses/MIT"})

    def test_update_descriptor(self):
        descriptor = self.crate.get_entity("ro-crate-metadata.json")
        descriptor["conformsTo"] = {"id": "https://w3id.org/ro/crate/1.2"}
        self.crate.update_descriptor(descriptor)
        self.assertEqual(
            self.crate.get_entity("ro-crate-metadata.json")["conformsTo"],
            {"id": "https://w3id.org/ro/crate/1.2"},
        )

    def test_update_requires_keys(self):
        with self.assertRaises(TypeError):
            self.crate.update_data({"id": "no-type.csv"})
        with self.assertRaises(TypeError):
            self.crate.update_contextual({"type": "Person"})
        with self.assertRaises(TypeError):
            self.crate.update_data("not a dict")

        root = self.crate.get_entity("./")
        for key in ["name", "description", "datePublished", "license"]:
            with self.assertRaises(TypeError, msg=key):
                self.crate.update_root({k: v for k, v in root.items() if k != key})

        descriptor = self.crate.get_entity("ro-crate-metadata.json")
        for key in ["conformsTo", "about"]:
            with self.assertRaises(TypeError, msg=key):
                self.crate.update_descriptor({k: v for k, v in descriptor.items() if k != key})

    def test_replace_id(self):
        self.crate.replace_id("#alice", "#bob")
        self.assertEqual(self.crate.get_entity("#bob")["name"], "Alice")
        self.assertEqual(self.crate.get_entity("data.csv")["author"], {"id": "#bob"})

    def test_delete_entity(self):
        self.crate.delete_entity("#alice", False)
        self.assertNotIn("#alice", self.crate.get_all_ids())
        self.assertEqual(self.crate.get_entity("data.csv")["author"], {"id": "#alice"})

    def test_delete_entity_recursive(self):
        self.crate.delete_entity("#alice", True)
        self.assertNotIn("#alice", self.crate.get_all_ids())
        self.assertEqual(self.crate.get_linked_ids("data.csv"), [])

    def test_get_entity_missing(self):
        with self.assertRaises(ValueError):
            self.crate.get_entity("missing")

    def test_write_and_read_back(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "ro-crate-metadata.json")
            self.crate.add_dynamic_entity_property("data.csv", {"contentSize": 42})
            self.crate.write(path)
            restored = read(path, 2)
            self.assertEqual(restored.get_all_ids(), self.crate.get_all_ids())
            self.assertEqual(restored.get_entity("data.csv")["contentSize"], 42)

    def test_repr(self):
        self.assertTrue(repr(self.crate).startswith("PyRoCrate(data: "))


if __name__ == "__main__":
    unittest.main()
