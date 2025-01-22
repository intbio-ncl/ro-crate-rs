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

if __name__ == '__main__':
    unittest.main()


