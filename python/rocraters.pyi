"""
Type stubs for rocraters, the Python bindings for ro-crate-rs.

Entities are plain dictionaries using "id" and "type" in place of "@id" and
"@type". A link to another entity is written as {"id": "..."}.

Some functions depend on cargo features the wheel was built with: RDF
(`rdf`) and subcrate fetching (`subcrate-resolution`) are on by default,
tabular export (`parquet`) is opt-in.
"""

from typing import Any, Literal, Optional, TypedDict, Union

ContextItem = Union[str, dict[str, str]]
RdfFormat = Literal["turtle", "ttl", "ntriples", "nt", "nquads", "nq", "rdfxml", "xml", "rdf"]
SpecVersion = Literal["1.1", "1.2", "1.3"]

class ValidationReport(TypedDict):
    """Returned by the validate functions"""

    is_valid: bool
    invalid_keys: list[str]
    invalid_ids: list[str]
    invalid_types: list[str]
    # IoError, JsonError, VocabNotValid, SchemaError or ReqwestError
    error_type: Optional[str]
    error_message: Optional[str]

class PyRoCrateContext:
    """The JSON-LD @context of an RO-Crate"""

    @staticmethod
    def from_string(context: str) -> "PyRoCrateContext":
        """
        Creates a reference context from a single URL, e.g.
        https://w3id.org/ro/crate/1.3/context
        """

    @staticmethod
    def from_list(context: list[ContextItem]) -> "PyRoCrateContext":
        """
        Creates an extended context from context URLs and dictionaries of
        term definitions.

        :raises ValueError: if an item is not a string or a dict of strings
        """

    @staticmethod
    def from_embedded(context: list[dict[str, str]]) -> "PyRoCrateContext":
        """Creates a fully embedded context from dictionaries of term definitions"""

class PyRoCrate:
    """An RO-Crate: a context and a graph of entities"""

    def __init__(self, context: PyRoCrateContext) -> None:
        """Creates an empty crate with the given context"""

    @staticmethod
    def new_default() -> "PyRoCrate":
        """Creates a crate with a default metadata descriptor and root data entity"""

    # Context

    def get_all_context(self) -> list[dict[str, Union[str, dict[str, str]]]]:
        """Returns each context item as {"@context": item}"""

    def get_context_items(self) -> list[str]:
        """Returns embedded context keys, or the URL of a reference context"""

    def get_specific_context(self, context: str) -> Optional[str]:
        """Returns the value of an embedded context key such as "@base", or None"""

    def add_context(self, item: ContextItem) -> None:
        """Adds a context URL or a dictionary of term definitions"""

    def remove_context(self, item: ContextItem) -> None:
        """
        Removes a context item.

        :raises ValueError: if the item is not present or cannot be removed
        """

    def replace_context(self, new: ContextItem, old: ContextItem) -> None:
        """
        Replaces the context item `old` with `new`.

        :raises ValueError: if `old` is not present
        """

    def get_rocrate_version(self) -> Optional[SpecVersion]:
        """Returns the RO-Crate version declared by the context, or None"""

    def get_urn_uuid(self) -> Optional[str]:
        """Returns the UUID of a urn:uuid @base, or None"""

    def add_urn_uuid(self) -> str:
        """
        Adds a urn:uuid @base if the crate has none, and returns the UUID.

        :raises ValueError: if the context cannot hold an @base
        """

    # Reading entities

    def get_entity(self, id: str) -> dict[str, Any]:
        """
        Returns the entity with the given ID.

        :raises ValueError: if there is no such entity
        """

    def to_list(self) -> list[dict[str, Any]]:
        """Returns every entity in the graph"""

    def get_all_ids(self) -> list[str]:
        """Returns the ID of every entity in the graph"""

    def get_all_properties(self) -> list[str]:
        """Returns every property key used in the crate"""

    def get_all_property_values(self, property: str) -> list[tuple[str, Any]]:
        """Returns (entity id, value) for each entity with the property"""

    def get_specific_value(self, value: str) -> list[tuple[str, str]]:
        """Returns (entity id, property) for each entity holding the value"""

    def get_linked_ids(self, id: str) -> list[str]:
        """
        Returns every ID the entity links to.

        :raises ValueError: if there is no such entity
        """

    def get_subcrates(self) -> list[dict[str, Any]]:
        """Returns the data entities that reference other RO-Crates"""

    def fetch_subcrates(self, recursive: bool = False, strict: bool = True) -> list["PyRoCrate"]:
        """
        Fetches the referenced subcrates. Local paths are resolved relative to
        the working directory, remote IDs over HTTP. Requires the
        `subcrate-resolution` feature.

        :param recursive: also fetch the subcrates of each subcrate
        :param strict: raise if a subcrate cannot be resolved, rather than skip it
        :raises OSError: if strict and a subcrate cannot be resolved
        """

    # Changing entities

    def update_data(self, py_obj: dict[str, Any]) -> None:
        """Adds a data entity, or overwrites the entity with the same ID"""

    def update_contextual(self, py_obj: dict[str, Any]) -> None:
        """Adds a contextual entity, or overwrites the entity with the same ID"""

    def update_root(self, py_obj: dict[str, Any]) -> None:
        """
        Adds the root data entity, or overwrites it. Requires id, type, name,
        description, datePublished and license.
        """

    def update_descriptor(self, py_obj: dict[str, Any]) -> None:
        """
        Adds the metadata descriptor, or overwrites it. Requires id, type,
        conformsTo and about.
        """

    def replace_id(self, id_old: str, id_new: str) -> None:
        """Changes an entity's ID and updates every reference to it"""

    def delete_entity(self, id: str, recursive: bool) -> None:
        """Deletes an entity; recursive also removes every reference to it"""

    def add_type(self, id: str, data_type: str) -> None:
        """
        Adds a type to an entity.

        :raises ValueError: if there is no such entity
        """

    def remove_type(self, id: str, data_type: str) -> None:
        """
        Removes a type from an entity.

        :raises ValueError: if there is no such entity, the type is not
            present, or it is the only type
        """

    def add_dynamic_entity_property(self, id: str, properties: dict[str, Any]) -> bool:
        """
        Adds or overwrites properties on an entity. Returns False if there is
        no such entity.
        """

    def remove_dynamic_entity_property(self, id: str, property: str) -> bool:
        """Removes a property from an entity. Returns False if there is no such entity."""

    def add_data_to_partof_root(self, id: str) -> None:
        """Adds an entity ID to the root data entity's hasPart"""

    # Validation and serialisation

    def validate(self) -> ValidationReport:
        """Validates property keys and local IDs against the crate's RO-Crate version"""

    def to_json(self, pretty: bool = True) -> str:
        """Serialises the crate as RO-Crate JSON-LD"""

    def write(self, file_path: Optional[str] = None) -> None:
        """
        Writes the crate to file_path, or ./ro-crate-metadata.json.

        :raises OSError: if the file cannot be written
        """

    # RDF (requires the `rdf` feature)

    def to_rdf(
        self,
        format: RdfFormat = "turtle",
        base: Optional[str] = None,
        allow_relative: bool = False,
        allow_remote: bool = False,
        contexts: Optional[dict[str, str]] = None,
    ) -> str:
        """
        Converts the crate to RDF.

        :param base: base IRI for relative IDs when the context has no @base
        :param allow_relative: leave unresolvable relative IRIs as they are
        :param allow_remote: fetch unknown context documents over HTTP
        :param contexts: context documents to preload, as {url: json_text}
        :raises ValueError: if the crate cannot be converted, or both base
            and allow_relative are given
        """

    def to_rdf_graph(
        self,
        base: Optional[str] = None,
        allow_relative: bool = False,
        allow_remote: bool = False,
        contexts: Optional[dict[str, str]] = None,
    ) -> "PyRdfGraph":
        """Converts the crate to an RDF graph; takes the same options as to_rdf"""

    def expand_term(
        self,
        term: str,
        strict: bool = False,
        allow_remote: bool = False,
        contexts: Optional[dict[str, str]] = None,
    ) -> str:
        """
        Expands a term or relative ID to a full IRI using the crate's context.

        :param strict: raise if the term cannot be made absolute
        """

    def compact_iri(
        self,
        iri: str,
        allow_remote: bool = False,
        contexts: Optional[dict[str, str]] = None,
    ) -> str:
        """Compacts a full IRI to a term using the crate's context"""

class PyRdfGraph:
    """RDF triples converted from a crate, with the context used to convert them"""

    def serialize(self, format: RdfFormat = "turtle") -> str: ...
    def write(self, path: str, format: RdfFormat = "turtle") -> None: ...
    def to_rocrate(self) -> PyRoCrate:
        """Converts the graph back to a crate using its original context"""
    def __len__(self) -> int: ...

def read(relative_path: str, validation_level: int) -> PyRoCrate:
    """
    Reads a ro-crate-metadata.json file.

    :param validation_level: 0 none, 1 warn, 2 strict
    :raises OSError: if the crate cannot be read or fails validation
    """

def read_object(obj: str, validation_level: int) -> PyRoCrate:
    """Reads a crate from a JSON string"""

def read_zip(path: str, validation_level: int) -> PyRoCrate:
    """Reads ro-crate-metadata.json from a zipped crate"""

def read_remote(url: str, validation_level: int) -> PyRoCrate:
    """
    Fetches and reads a crate from a URL.

    :raises ValueError: for an invalid URL
    :raises OSError: if the crate cannot be fetched or read
    """

def from_rdf(data: str, format: RdfFormat = "turtle", base: Optional[str] = None) -> PyRoCrate:
    """
    Parses RDF into a crate. Requires the `rdf` feature.

    :raises ValueError: if the RDF cannot be parsed or has no root entities
    """

def validate(relative_path: str) -> ValidationReport:
    """Validates a ro-crate-metadata.json file"""

def validate_object(obj: str) -> ValidationReport:
    """Validates a crate given as a JSON string"""

def validate_zip(path: str) -> ValidationReport:
    """Validates a zipped crate"""

def validate_remote(url: str) -> ValidationReport:
    """
    Validates a crate fetched from a URL.

    :raises ValueError: for an invalid URL
    """

def load_schema(version: SpecVersion = "1.3") -> dict[str, Any]:
    """Returns the bundled RO-Crate JSON-LD context document for a version"""

def zip(
    crate_path: str, external: bool, validation_level: int, flatten: bool, unique: bool
) -> None:
    """
    Zips a crate directory next to its ro-crate-metadata.json.

    :param crate_path: path to the ro-crate-metadata.json file
    :param external: copy externally referenced files into an external/ folder
    :param flatten: drop the directory structure inside the zip
    :param unique: name the zip after the crate's urn:uuid
    :raises OSError: if the crate cannot be read or the zip written
    """

def prefix_object_id(rocrate: PyRoCrate, object_root: str) -> None:
    """Prefixes every local entity ID with an object storage root, in place"""

def write_parquet(rocrate: PyRoCrate, path: str) -> None:
    """
    Writes the crate as a uuid/id/key/value Parquet table. Requires the
    `parquet` feature.

    :raises ValueError: if the crate has no urn:uuid @base (see add_urn_uuid)
    :raises OSError: if the file cannot be written
    """

def write_csv(rocrate: PyRoCrate, path: str) -> None:
    """Writes the crate as a uuid/id/key/value CSV table; see write_parquet"""
