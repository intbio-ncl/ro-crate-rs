# Overview 

rocraters is a python library that is built upon a rust backend for interfacing with [RO-Crates](https://www.researchobject.org/ro-crate/1.1/). 
This implementation was specifically created to address the challenges of data 
handling on automated robotic systems within an automated synthetic biology lab,
however it's noted that this could have more general applicability to other environments.

It's aim is to provide a robust, portable and scalable solution to dealing with 
RO-Crates within the varying software environments of a synthetic biology lab stack.
It's designed to be maximally flexible with minimal onboarding, allowing you to 
incorprate it into scrpits/ data pipelines as easily as possible. 
This also relies on you to have an understanding of the structure of an RO-Crate, but focuses more on the fact that some metadata is better than no metadata. 


*This is not the go-to python libary for RO-Crate interfacing, 
please see [ro-crate-py](https://github.com/ResearchObject/ro-crate-py) for a 
full python implementation.*

# Build 

Built using PyO3 and maturin. Recommended to setup python venv, then install maturin (and remember maturin[patchelf])

# Installation 

```bash
pip install rocraters
```

If pip not available for your particular platform, you can build from source by 
cloning this repository and building via maturin. 

# Basic usage 

The RO-Crate specification defines an RO-Crate as a JSON-LD file, consisting of a context and a graph. As such, in python it is a dictionary containing a "context" key, with some form of vocab context (default is the RO-Crate context) and a "graph" key, which contains a list of json objects (dictionaries).

To create an empty RO-Crate, you need to do the following: 
```python
from rocraters import PyRoCrateContext, PyRoCrate, read, zip

# Define context
context = PyRoCrateContext.from_string("https://w3id.org/ro/crate/1.1/context")

# Initialise empty crate
crate = PyRoCrate(context)

# For an easy start, you can make a default crate!
default_crate = PyRoCrate.new_default()

print(f"Example of a default crate \n {default_crate}")
```

Now, there are 4 primary objects (dictionaries) that can be added to the crate:
1. Metadata descriptor (only 1 per crate)
2. Root data entity (only 1 per crate)
3. Data entity (zero - many)
4. Contextual entity (zero - many)

These are all based upon the specification. 

To populate the basic crate, with the essential keys to conform to specification: 

```python 
# Continuation of the above examples 
# Metadata descriptor
descriptor = {
    "type": "CreativeWork",
    "id": "ro-crate-metadata.json",
    "conformsTo": {"id": "https://w3id.org/ro/crate/1.1"},
    "about": {"id": "./"},
}

# Root data entity
root = {
    "id": "./",
    "type": "Dataset",
    "name": "Example Crate",
    "description": "Description of the Example Crate",
    "datePublished": "2017",
    "license": {"id": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/"},
    "author": {"id": "#johndoe"},
}
# Data entity
data = {"id": "output/data_file.txt", "type": "Dataset", "name": "Data file name"}
# Contextual entity
contextual = {
    "id": "#JohnDoe",
    "type": "Person",
}

failed_data = {"id": "this_is_not_a_file.txt", "type": "Dataset"}

# Update the RO-Crate object
crate.update_descriptor(descriptor)
crate.update_root(root)
crate.update_data(data)
crate.update_contextual(contextual)

# This acts as an example of how if the file/ URI isn't valid as a potentially
# accessible data entity, it loads as a contextual entity
crate.update_data(failed_data)
```

To then write the crate to a `ro-crate-metadata.json` file in the current working directory:
```python
# Continuation of the above examples
# Write crate 
crate.write()
```

To then read a `ro-crate-metadata.json` file and load it in as a structured object:
```python 
# New example

# Read RO-Crate at specified path with validation level 1
crate = read("ro-crate-metadata.json", 1)
```
If required, you can also use `read_object` to read in a string of a ro-crate if querying as json response.

Additionally, you can read the ro-crate directly from a zip file using `read_zip`

To zip the folder and all contained directories within the `ro-crate-metadata.json` directory:
```python
# new example 
from rocraters import zip

# This zips the target ro-crate up into a zip file, pulling in externally 
# definined files from their paths, with validation level 1, without flattening 
# and naming the zip file as the UUID as defined in the URN. If you flatten
# the zip file will not preserve directory structure
zip("ro-crate-metadata.json", True, 1, False, True)
```

# Modifying a RO-Crate 

As per the libraries purpose, modification, ie the deletion, update and addition of entites is intended to be as simple as possible, whilst ensuring minimal conformance:

```python
# Example based upon previously made crate
from rocraters import read

crate = read("ro-crate-metadata.json", True) 

# Update the data entity and make modification 
data_target = crate.get_entity("data_file.txt")
data_target["description"] = "A text file dataset containing information"

# Update the contextual entity and make modification
contextual_target = crate.get_entity("#JohnDoe")
contextual_target.update({"id" : "#JaneDoe"})
crate.update_contextual(contextual_target)

# To delete a key:value 
data_target.pop("description")

# To delete an entity - this immediately updates the crate object
contextual_target.delete_entity("#JaneDoe")

# We then update the crate the same way we make it
# The ID will be used to serach the crate and overwrites the object with an indentical "id" key
crate.update_data(data_target)
crate.write()
```

# Custom compilation 

PyO3 is used to handle python bindings. Maturin is used as the build tool.

