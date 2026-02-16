from rocraters import PyRoCrate, PyRoCrateContext, read, read_object, read_zip, zip
import unittest
from pathlib import Path



# Test cases
class TestApi(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Run once before all tests."""
        cls.path = Path.cwd()
        print("Setting up class resources...")

        cls.metadata_fixture = {
            "type": "CreativeWork",
            "id": "ro-crate-metadata.json",
            "conformsTo": {"id": "https://w3id.org/ro/crate/1.1"},
            "about": {"id": "./"}
        }  
        
        cls.root_fixture = {
            "id": "./",
            "identifier": "https://doi.org/10.4225/59/59672c09f4a4b",
            "type": "Dataset",
            "datePublished": "2017",
            "name": "Data files associated with the manuscript:Effects of facilitated family case conferencing for ...",
            "description": "Palliative care planning for nursing home residents with advanced dementia ...",
            "license": {"id": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/"}
        }

        cls.contextual_fixture = {
            "id": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/",
            "type": "CreativeWork",
            "description": "This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 3.0 Australia License. To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/au/ or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.",
            "identifier": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/",
            "name": "Attribution-NonCommercial-ShareAlike 3.0 Australia (CC BY-NC-SA 3.0 AU)",
            "value": None
        }



    @classmethod
    def tearDownClass(cls):
        """Run once after all tests."""
        pass

    def setUp(self):
        """Run before every test."""
        pass

    def tearDown(self):
        """Run after every test."""
        pass

    def test_add(self):
        """Test the add function."""
        crate_path = self.path / Path("tests/fixtures/_ro-crate-metadata-minimal.json")
        crate = read(str(crate_path),1)
        self.assertTrue(bool(crate), "The result should not be empty.")

    def test_context_string(self):
        context = PyRoCrateContext.from_string("https://w3id.org/ro/crate/1.1/context")
        # Define context

    def test_empty_crate(self):

        # Initialise empty crate
        context = PyRoCrateContext.from_string("https://w3id.org/ro/crate/1.1/context")
        crate = PyRoCrate(context)

    def test_default_crate(self):

        # For an easy start, you can make a default crate!
        default_crate = PyRoCrate.new_default()



    def test_read_crate(self):
        crate_path = self.path / Path("tests/fixtures/_ro-crate-metadata-minimal.json")
        crate = read(str(crate_path), 0)
        self.assertEqual(crate.get_entity("./"), self.root_fixture) 

    def test_read_obj(self):
        crate_path = self.path / Path("tests/fixtures/_ro-crate-metadata-minimal.json")
        crate_object = '''{ 
            "@context": "https://w3id.org/ro/crate/1.1/context", 
            "@graph": [
                {
                    "@type": "CreativeWork",
                    "@id": "ro-crate-metadata.json",
                    "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
                    "about": {"@id": "./"}
                },  
                {
                    "@id": "./",
                    "identifier": "https://doi.org/10.4225/59/59672c09f4a4b",
                    "@type": "Dataset",
                    "datePublished": "2017",
                    "name": "Data files associated with the manuscript:Effects of facilitated family case conferencing for ...",
                    "description": "Palliative care planning for nursing home residents with advanced dementia ...",
                    "license": {"@id": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/"}
                },
                {
                    "@id": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/",
                    "@type": "CreativeWork",
                    "description": "This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 3.0 Australia License. To view a copy of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/au/ or send a letter to Creative Commons, PO Box 1866, Mountain View, CA 94042, USA.",
                    "identifier": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/",
                    "name": "Attribution-NonCommercial-ShareAlike 3.0 Australia (CC BY-NC-SA 3.0 AU)",
                    "value": None
                }
            ]
        }'''
        crate = read_object(crate_object, 0)
        entity = crate.get_entity("https://creativecommons.org/licenses/by-nc-sa/3.0/au/")

        self.assertEqual(entity, self.contextual_fixture)
        
    def test_read_zip(self):
        crate_path = self.path / Path("tests/fixtures/zip_test/fixtures.zip")
        crate = read_zip(str(crate_path), 1)
        root = crate.get_entity("./")

        self.assertEqual(root, self.root_fixture)

    def test_zip_crate(self):

        # TODO: FIX
        crate_path = self.path / Path("tests/fixtures/test_experiment/_ro-crate-metadata-minimal.json")
        zip(str(crate_path), True, 1, False, False)

        self.assertTrue(Path.exists(self.path / Path("tests/fixtures/test_experiment/test_experiment.zip")))

    def test_get_context(self):
        crate_path = self.path / Path("tests/fixtures/_ro-crate-metadata-minimal.json")
        crate = read(str(crate_path), 0)

        context = crate.get_all_context()

        self.assertIsInstance(context, list)
        self.assertEqual(len(context), 1)
        self.assertTrue(all(isinstance(item, dict) for item in context))
        self.assertTrue(all("@context" in item for item in context))

        self.assertEqual(
            context[0],
            {"@context": "https://w3id.org/ro/crate/1.1/context"},
        )

    def test_get_context_extended(self):
        crate_path = self.path / Path("python/tests/fixtures/_ro-crate-metadata-minimal.json")
        crate = read(str(crate_path), 0)

        context = crate.get_all_context()

        self.assertIsInstance(context, list)
        self.assertEqual(len(context), 2)
        self.assertTrue(all(isinstance(item, dict) for item in context))
        self.assertTrue(all("@context" in item for item in context))

        context_values = [item["@context"] for item in context]
        self.assertIn("https://w3id.org/ro/crate/1.1/context", context_values)
        self.assertIn(
            {"@base": "urn:uuid:01234567-89ab-cdef-0123-456789abcdef"},
            context_values,
        )

    def test_to_list(self):
        crate_path = self.path / Path("tests/fixtures/_ro-crate-metadata-minimal.json")
        crate = read(str(crate_path), 0)

        entities = crate.to_list()

        self.assertIsInstance(entities, list)
        self.assertEqual(len(entities), 3)

        ids = {entity["id"] for entity in entities}
        expected_ids = {
            "ro-crate-metadata.json",
            "./",
            "https://creativecommons.org/licenses/by-nc-sa/3.0/au/",
        }

        self.assertEqual(ids, expected_ids)

        entities_by_id = {entity["id"]: entity for entity in entities}

        metadata = entities_by_id["ro-crate-metadata.json"]
        self.assertEqual(metadata["type"], self.metadata_fixture["type"])
        self.assertEqual(metadata["conformsTo"], self.metadata_fixture["conformsTo"])
        self.assertEqual(metadata["about"], self.metadata_fixture["about"])

        root = entities_by_id["./"]
        self.assertEqual(root["type"], self.root_fixture["type"])
        self.assertEqual(root["name"], self.root_fixture["name"])
        self.assertEqual(root["description"], self.root_fixture["description"])
        self.assertEqual(root["datePublished"], self.root_fixture["datePublished"])
        self.assertEqual(root["license"], self.root_fixture["license"])
        self.assertEqual(root["identifier"], self.root_fixture["identifier"])

        contextual = entities_by_id["https://creativecommons.org/licenses/by-nc-sa/3.0/au/"]
        self.assertEqual(contextual["type"], self.contextual_fixture["type"])
        self.assertEqual(contextual["description"], self.contextual_fixture["description"])
        self.assertEqual(contextual["identifier"], self.contextual_fixture["identifier"])
        self.assertEqual(contextual["name"], self.contextual_fixture["name"])


if __name__ == '__main__':
    unittest.main()
