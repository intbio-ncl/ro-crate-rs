class PyRoCrateContext:
    """A class representing the context of the RoCrate. It is initialised as empty"""
    def __init__(self) -> None: ...
    def from_string(self, context: str) -> "PyRoCrateContext":
        """
        Creates a RoCrate context using a single string. This is useful for 
        default RoCrate context creation
        
        :param context: String of context to be created e.g.\
                        https://w3id.org/ro/crate/1.2/context
        """

    def from_list(self, context: list) -> "PyRoCrateContext":
        """
        Crates a complex RoCrate context using a list of contexts. This can be
        used for creating both extended and embedded contexts.

        :param context: List of contexts to be crated
        """

class PyRoCrate:
    """
    A class representing the RoCrate Graph. It is initialised as empty with
    a known context
    """
    def __init__(self, context: PyRoCrateContext) -> None: ...
    def new_default(self) -> "PyRoCrate":
        """
        Creates a default RoCrate object with MUST/SHOULD defaults.
        """

    def get_entity(self, id: str) -> dict:
        """
        Gets a crate entity based upon ID and returns as a dict

        :param id: ID to search
        """

    def update_data(self, py_obj: dict) -> None:
        """
        Update a data entity with new data

        Lazy update of data entity, finds id and overwrites the index.
        Strongly recommended to extract index data, modify, then rewrite the
        modified index data as the update.

        :param py_obj: dictionary to overwrite/add
        """
    def update_contextual(self, py_obj: dict) -> None:
        """
        Update a contextual entity with new data

        Lazy update of contextual entity, finds id and overwrites the index.
        Strongly recommended to extract index data, modify, then rewrite the
        modified index data as the update.

        :param py_obj: dictionary to overwrite/add
        """
    def update_root(self, py_obj: dict) -> None:
        """
        Update a root entity with new data

        Lazy update of root entity, finds id and overwrites the index.
        Strongly recommended to extract index data, modify, then rewrite the
        modified index data as the update.

        :param py_obj: dictionary to overwrite/add
        """
    def update_descriptor(self, py_obj: dict) -> None:
        """
        Update the metadata decriptor with new data

        Lazy update of the metadata descriptor, finds id and overwrites the index.
        Strongly recommended to extract index data, modify, then rewrite the
        modified index data as the update.

        :param py_obj: dictionary to overwrite/add
        """

    def replace_id(self, id_old: str, id_new: str) -> None:
        """
        Overwrites an ID with a new ID

        Overwrites an ID with a new ID, and recursively changes every instance
        of the old ID within the RO-Crate

        :param id_old: Old ID to search
        :param new_id: New ID to replace old
        """

    def delete_entity(self, id: str, recursive: bool) -> None:
        """
        Deletes target id entity. Can select recursive deletion

        :param id: ID of entity to delete
        :param recursive: Boolean True if you want to delete all mentions of entity
        """

    def write(self, file_path: str) -> None:
        """
        Writes the RO-Crate back to a ro-crate-metdata.json file

        :param file_path: File path to RO-Crate, if None default is current dir
        """

def read(relative_path: str, validity: bool) -> "PyRoCrate":
    """
    Reads in a Ro-Crate to memory allowing manipulation

    :param relative_path: Path to RO-Crate
    :param validity: True if crate validation needed
    """

def read_ob(obj: str, validity: bool) -> "PyRoCrate":
    """
    Reads in a json object of a crate into memory allowing manipulation

    Useful for browsers/ applications

    :param obj: String of full RO-Crate
    :param validity: True if crate validation needed
    """

def zip(crate_path: str, external: bool) -> None:
    """
    Targets an RO-Crate and zip directory contents. If external is True, pulls
    all externally referenced (none relative) data entities into zip

    :param crate_path: Path to target crate
    :param external: Boolean, if True pulls in external data to zip
    """

def validate(relative_path: str) -> dict:
    """
    Validates a crate and returns a structured report dictionary.

    Report keys:
    - is_valid: bool
    - invalid_keys: list[str]
    - invalid_ids: list[str]
    - invalid_types: list[str]
    - error_type: str | None
    - error_message: str | None
    """

def validate_object(obj: str) -> dict:
    """
    Validates a crate represented as a JSON object string.

    Returns the same report dictionary shape as `validate`.
    """

def validate_zip(path: str) -> dict:
    """
    Validates a crate stored in a zip archive.

    Returns the same report dictionary shape as `validate`.
    """
