from rocraters import PyRoCrateContext, PyRoCrate, read, zip

# Define context
context = PyRoCrateContext.from_string("https://w3id.org/ro/crate/1.1/context")

# Initialise empty crate
crate = PyRoCrate(context)

# For an easy start, you can make a default crate!
default_crate = PyRoCrate.new_default()

print(f"Example of a default crate \n {default_crate}")

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
    "datePublished": "2017",
    "name": "Example",
    "description": "Example Ro-Crate for using python library",
    "license": {"id": "https://creativecommons.org/licenses/by-nc-sa/3.0/au/"},
    "author": {"id": "#JohnDoe"},
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

# Write crate
crate.write()


# Now that a new crate is written, we can open it again!
crate = read("ro-crate-metadata.json", 0)
# print(f"Example of crate created and read in \n {crate}")

# Update the data entity and make modification
data_target = crate.get_entity("output/data_file.txt")
# print(data_target)
data_target["description"] = "A text file dataset containing information"

# print(f"This is the loaded and modified data_file entity \n {data_target}")

crate.update_data(data_target)

# print(f"This is now the updated, in memory, crate: \n {crate}")

# Update the contextual entity and make modification
contextual_target = crate.get_entity("#JohnDoe")
contextual_target.update({"id": "#JaneDoe"})

crate.update_contextual(contextual_target)
# print(f"Example of a modified entity id that will save as a new entity: \n {crate}")

# To delete a key:value
data_target.pop("description")

# We then update the crate the same way we make it
# The ID will be used to serach the crate and overwrites the object with an indentical "id" key
crate.update_data(data_target)

# To delete an entity - this immediately updates the crate object
crate.delete_entity("#JaneDoe", True)
crate.replace_id("#JohnDoe", "#JaneDoe")

# print(crate)
crate.write()

# Final example of modified crate
crate = read("ro-crate-metadata.json", 1)

# Zip the crate to get all data
# zip("ro-crate-metadata.json", True, 2)
