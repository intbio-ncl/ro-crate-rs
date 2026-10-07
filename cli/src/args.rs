//! Contains all the arguments for cli tool
//!
//! Built with clap

#[cfg(feature = "rdf")]
use clap::ValueEnum;
use clap::{ArgAction, Args, Parser, Subcommand};
use std::str::FromStr;

#[derive(Debug, Parser)]
#[clap(author, version, about)]
pub struct RoCrateArgs {
    #[clap(subcommand)]
    pub crate_action: CrateAction,
}

#[derive(Debug, Subcommand)]
pub enum CrateAction {
    /// Initialise a new empty Ro-Crate
    Init(InitCommand),
    /// Add an entity to an Ro-Crate
    Add(AddCommand),
    /// Delete an entity in an Ro-Crate
    Delete(DeleteCommand),
    /// Modify a particular entity within an Ro-Crate (includes Root and Descriptor)
    #[clap(subcommand)]
    Modify(ModifyCommand),
    /// Read the crate and display
    #[clap(subcommand)]
    Read(ReadCommand),
    /// Allows you to package crate into different formats
    #[clap(subcommand)]
    Package(PackageCommand),
    /// Run basic validation over a crate
    #[clap(subcommand)]
    Validate(ValidateCommand),
    /// Inspect or change the crate's JSON-LD @context
    #[clap(subcommand)]
    Context(ContextCommand),
    /// List or fetch the RO-Crates referenced by a crate (subcrates)
    #[clap(subcommand)]
    Subcrates(SubcratesCommand),
    /// Show the bundled RO-Crate JSON-LD context for a specification version
    Schema(SchemaCommand),
    /// Convert a crate to and from RDF
    #[cfg(feature = "rdf")]
    #[clap(subcommand)]
    Rdf(RdfCommand),
    /// Convert a crate to a uuid/id/key/value table
    #[cfg(feature = "parquet")]
    #[clap(subcommand)]
    Convert(ConvertCommand),
}

/// Crate for a command to operate on
#[derive(Debug, Args, Clone)]
pub struct CrateTarget {
    /// Target crate: a metadata file, or for read-only commands also a .zip
    /// archive, an http(s) URL or `-` for stdin
    #[clap(short, long, default_value = "ro-crate-metadata.json")]
    pub target_crate: String,
}

#[derive(Debug, Args)]
pub struct InitCommand {
    /// Default ro-crate initialisation using latest spec
    #[clap(short, long)]
    pub default: bool,
    /// Type of context
    #[clap(required_unless_present_any = ["default", "minimal", "context_items"])]
    #[clap(short, long, name="context", help=CONTEXT_HELP)]
    pub context_type: Option<ContextType>,
    /// Initialise with default minimal entites or leave empty
    #[clap(short, long)]
    pub minimal: bool,
    /// Context item for a non-interactive init: a context URL or a JSON object of
    /// term definitions. Repeat for multiple items
    #[clap(long = "context-item", value_name = "ITEM", conflicts_with_all = ["context", "default"])]
    pub context_items: Vec<String>,
    /// Store the --context-item objects as a fully embedded context
    #[clap(long, requires = "context_items")]
    pub embedded: bool,
}

// Create a detailed help message
const CONTEXT_HELP: &str = "Type of context:\n\
                            1 - Reference Context: Basic context type with minimal fields.\n\
                            2 - Extended Context: Includes additional metadata fields.\n\
                            3 - Embedded Context: C ontains embedded data for richer context.";

#[derive(Debug, Clone)]
pub enum ContextType {
    /// 1 - Reference Context
    Reference,
    /// 2 - Extended Context
    Extended,
    /// 3 - Embedded Context
    Embedded,
}

impl FromStr for ContextType {
    type Err = &'static str;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "1" => Ok(ContextType::Reference),
            "2" => Ok(ContextType::Extended),
            "3" => Ok(ContextType::Embedded),
            _ => Err("invalid context type"),
        }
    }
}

// Optional: Implement Display for nicer command line output
impl std::fmt::Display for ContextType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ContextType::Reference => "Reference Context",
                ContextType::Extended => "Extended Context",
                ContextType::Embedded => "Embedded Context",
            }
        )
    }
}

#[derive(Debug, Args, Clone)]
pub struct AddCommand {
    /// Target crate
    #[clap(
        short,
        long,
        required = false,
        default_value = "ro-crate-metadata.json"
    )]
    pub target_crate: String,
    /// Input ID
    pub id: String,
    /// Input Datatype. This can be a single datatype, or a list seperated by ',' (e.g type1,type2)
    #[clap(use_value_delimiter = true, value_delimiter = ',')]
    pub datatype: Vec<String>,
    /// Extra information. Allows you to input Custom fields and values.
    #[clap(short, long)]
    pub extra_information: bool,
}

#[derive(Debug, Args)]
pub struct DeleteCommand {
    /// Target crate
    #[clap(
        short,
        long,
        required = false,
        default_value = "ro-crate-metadata.json"
    )]
    pub target_crate: String,
    /// Input ID to delete
    pub id: String,
    /// Recursive deletion. Allows you to delete all occurances of the ID linked in other research objects.
    #[clap(short, long)]
    pub recursive: bool,
}

#[derive(Debug, Subcommand)]
pub enum ModifyCommand {
    /// Add a ID to an entity
    AddIdValue(AddIdValueCommand),
    /// Add a list of ID's to an entity
    AddIdVecValues(AddIdVecValuesCommand),
    /// Add multiple new fields to entity - Useful for large crates
    AddMultiple(AddMultipleCommand),
    /// Remove a specific field from an entity
    RemoveField(RemoveFieldCommand),
    /// Add a field to an entity, or overwrite it if present
    AddField(AddFieldCommand),
    /// Add a type to an entity's @type
    AddType(EntityTypeCommand),
    /// Remove a type from an entity's @type
    RemoveType(EntityTypeCommand),
    /// Change an entity's ID and update every reference to it
    ReplaceId(ReplaceIdCommand),
    /// Add an entity ID to the root data entity's hasPart
    AddHasPart(EntityIdCommand),
    /// Insert an entity from JSON-LD, overwriting any entity with the same @id
    Upsert(UpsertCommand),
    /// Prefix every local entity ID with an object storage root
    PrefixIds(PrefixIdsCommand),
}

#[derive(Debug, Args)]
pub struct AddFieldCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Input ID to target
    pub id: String,
    /// Key to add
    pub key: String,
    /// Value to add. Stored as a string unless --json is given
    pub value: String,
    /// Parse the value as JSON (e.g. 42, true, ["a","b"] or {"@id": "#ref"})
    #[clap(long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct EntityTypeCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Input ID to target
    pub id: String,
    /// Type to add or remove (e.g. File)
    pub data_type: String,
}

#[derive(Debug, Args)]
pub struct ReplaceIdCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Current ID
    pub old_id: String,
    /// New ID
    pub new_id: String,
}

#[derive(Debug, Args)]
pub struct EntityIdCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Input ID to target
    pub id: String,
}

#[derive(Debug, Args)]
pub struct UpsertCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// JSON-LD entity object with @id and @type, or `-` to read it from stdin
    pub entity: String,
}

#[derive(Debug, Args)]
pub struct PrefixIdsCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Object storage root to prepend (e.g. s3://bucket/crate/)
    pub object_root: String,
}

#[derive(Debug, Args)]
pub struct AddIdValueCommand {
    /// Target crate
    #[clap(
        short,
        long,
        required = false,
        default_value = "ro-crate-metadata.json"
    )]
    pub target_crate: String,
    /// Input ID to target
    pub id: String,
    /// Key to add
    pub key: String,
    /// ID value to add
    pub value: String,
}

#[derive(Debug, Args)]
pub struct AddIdVecValuesCommand {
    /// Target crate
    #[clap(
        short,
        long,
        required = false,
        default_value = "ro-crate-metadata.json"
    )]
    pub target_crate: String,
    /// Input ID to target
    pub id: String,
    /// Key to add
    pub key: String,
    /// ID values to add. This can be a single id, or a list seperated by ',' (e.g id1,id2)
    #[clap(use_value_delimiter = true, value_delimiter = ',')]
    pub values: Vec<String>,
}

#[derive(Debug, Args)]
pub struct AddMultipleCommand {
    /// Target crate
    #[clap(
        short,
        long,
        required = false,
        default_value = "ro-crate-metadata.json"
    )]
    pub target_crate: String,
    /// Input ID to target
    pub id: String,
}
#[derive(Debug, Args)]
pub struct RemoveFieldCommand {
    /// Target crate
    #[clap(
        short,
        long,
        required = false,
        default_value = "ro-crate-metadata.json"
    )]
    pub target_crate: String,
    /// Input ID to delete
    pub id: String,
    /// Field to delete
    pub field: String,
}

#[derive(Debug, Args)]
pub struct ZipCrateCommand {
    /// Target crate directory or metadata file
    #[clap(short,long,default_value_t=String::from("./"))]
    pub target_crate: String,
    /// Copy and include external reachable data files (`--external false` to disable)
    #[clap(short, long, default_value_t = true, num_args = 0..=1, default_missing_value = "true", action = ArgAction::Set)]
    pub external: bool,
    /// Flatten contents to remove folder stucture in zip
    #[clap(short, long, default_value_t = false)]
    pub flatten: bool,
    /// Renames zip with uuidv7
    #[clap(short, long, default_value_t = false)]
    pub unique: bool,
    /// Validation level for schema checks (0-2)
    #[clap(short = 'l', long, default_value_t = 1)]
    pub validation_level: i8,
}

#[derive(Debug, Subcommand)]
pub enum ReadCommand {
    /// Read full crate from zip archive
    Zip(ReadZipCommand),
    /// Read full crate
    Crate(ReadCrateCommand),
    /// Read entity of crate
    Entity(ReadEntityCommand),
    /// Read all of one field in crate
    Fields(ReadFieldsCommand),
    /// Read entity containing specific value
    Value(ReadValueCommand),
    /// List the IDs of every entity
    Ids(ReadListCommand),
    /// List every property key used in the crate
    Properties(ReadListCommand),
    /// List the IDs an entity links to
    Links(ReadLinksCommand),
}

#[derive(Debug, Args)]
pub struct ReadListCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Prints as json
    #[clap(short, long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ReadLinksCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Entity ID to inspect
    pub id: String,
    /// Prints as json
    #[clap(short, long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ReadCrateCommand {
    /// Target crate
    #[clap(
        short,
        long,
        required = false,
        default_value = "ro-crate-metadata.json"
    )]
    pub target_crate: String,
    /// Validation level for schema checks (0-2)
    #[clap(short = 'l', long, default_value_t = 1)]
    pub validation_level: i8,
    /// Raw struct data
    #[clap(short, long)]
    pub raw_struct: bool,
    /// Prints full view without trimming
    #[clap(short, long)]
    pub fit: bool,
    /// Prints as json
    #[clap(short, long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ReadZipCommand {
    /// Target zip archive
    #[clap(short, long, required = true)]
    pub target_zip: String,
    /// Validation level for schema checks (0-2)
    #[clap(short = 'l', long, default_value_t = 0)]
    pub validation_level: i8,
    /// Raw struct data
    #[clap(short, long)]
    pub raw_struct: bool,
    /// Prints full view without trimming
    #[clap(short, long)]
    pub fit: bool,
    /// Prints as json
    #[clap(short, long)]
    pub json: bool,
}

/// TODO: Add a field to recursively show all linked ids
#[derive(Debug, Args)]
pub struct ReadEntityCommand {
    /// Target crate
    #[clap(
        short,
        long,
        required = false,
        default_value = "ro-crate-metadata.json"
    )]
    pub target_crate: String,
    /// Entity ID to search
    pub id: String,
    /// Raw struct data
    #[clap(short, long)]
    pub raw_struct: bool,
    /// Prints full view without trimming
    #[clap(short, long)]
    pub fit: bool,
    /// Prints as json
    #[clap(short, long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ReadFieldsCommand {
    /// Target crate
    #[clap(
        short,
        long,
        required = false,
        default_value = "ro-crate-metadata.json"
    )]
    pub target_crate: String,
    /// Field to search for
    pub field: String,
    /// Prints as json
    #[clap(short, long)]
    pub json: bool,
}

#[derive(Debug, Args)]
pub struct ReadValueCommand {
    /// Target crate
    #[clap(
        short,
        long,
        required = false,
        default_value = "ro-crate-metadata.json"
    )]
    pub target_crate: String,
    /// Field to search for
    pub value: String,
    /// Show the object location
    #[clap(short, long)]
    pub location: bool,
    /// Prints as json
    #[clap(short, long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum PackageCommand {
    /// Zip full crate
    Zip(ZipCrateCommand),
}

#[derive(Debug, Subcommand)]
pub enum ValidateCommand {
    /// Runs basic validation on full ro-crate-metadata.json file
    Basic(ValidateCrateCommand),
}

#[derive(Debug, Args)]
pub struct ValidateCrateCommand {
    /// Target crate: a metadata file, a .zip archive, an http(s) URL or `-` for stdin
    #[clap(
        short,
        long,
        required = false,
        default_value = "ro-crate-metadata.json"
    )]
    pub target_crate: String,
    /// Prints the validation report as json
    #[clap(short, long)]
    pub json: bool,
}

#[derive(Debug, Subcommand)]
pub enum ContextCommand {
    /// Print the @context as JSON
    Show(TargetCommand),
    /// List context references and embedded term IRIs
    List(ReadListCommand),
    /// List embedded context keys, or the reference of a reference context
    Keys(ReadListCommand),
    /// Print the value of an embedded context key (e.g. @base)
    Get(ContextGetCommand),
    /// Add a context item: a context URL or a JSON object of term definitions
    Add(ContextItemCommand),
    /// Remove a context item: a context URL or a JSON object of term definitions
    Remove(ContextItemCommand),
    /// Replace one context item with another
    Replace(ContextReplaceCommand),
    /// Add a urn:uuid @base if the crate has none, and print the UUID
    AddUrn(TargetCommand),
    /// Print the UUID of the crate's urn:uuid @base
    Urn(TargetCommand),
    /// Print the RO-Crate specification version declared by the context
    Version(TargetCommand),
}

#[derive(Debug, Args)]
pub struct TargetCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
}

#[derive(Debug, Args)]
pub struct ContextGetCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Context key to look up
    pub key: String,
}

#[derive(Debug, Args)]
pub struct ContextItemCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Context URL, or a JSON object such as '{"@base": "https://example.org/"}'
    pub item: String,
}

#[derive(Debug, Args)]
pub struct ContextReplaceCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Context item to remove
    pub old: String,
    /// Context item to add
    pub new: String,
}

#[derive(Debug, Subcommand)]
pub enum SubcratesCommand {
    /// List the data entities that reference other RO-Crates
    List(ReadListCommand),
    /// Fetch the referenced subcrates and print them as a JSON array
    #[cfg(feature = "subcrate-resolution")]
    Fetch(SubcratesFetchCommand),
}

#[cfg(feature = "subcrate-resolution")]
#[derive(Debug, Args)]
pub struct SubcratesFetchCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Also fetch the subcrates of each subcrate
    #[clap(short, long)]
    pub recursive: bool,
    /// Skip subcrates that cannot be resolved instead of failing
    #[clap(long)]
    pub lenient: bool,
}

#[derive(Debug, Args)]
pub struct SchemaCommand {
    /// RO-Crate specification version (1.1, 1.2 or 1.3)
    #[clap(default_value = "1.3")]
    pub spec_version: String,
    /// List every term and the IRI it maps to
    #[clap(long)]
    pub terms: bool,
    /// Prints the full context document as json
    #[clap(short, long)]
    pub json: bool,
}

#[cfg(feature = "rdf")]
#[derive(Debug, Subcommand)]
pub enum RdfCommand {
    /// Convert a crate to RDF
    Export(RdfExportCommand),
    /// Convert RDF into an RO-Crate
    Import(RdfImportCommand),
    /// Convert a crate to RDF and back using its own context
    Roundtrip(RdfRoundtripCommand),
    /// Expand a term or relative ID to a full IRI using the crate's context
    Expand(RdfExpandCommand),
    /// Compact a full IRI to a term using the crate's context
    Compact(RdfCompactCommand),
}

#[cfg(feature = "rdf")]
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum RdfFormatArg {
    #[value(alias = "ttl")]
    Turtle,
    #[value(alias = "nt")]
    Ntriples,
    #[value(alias = "nq")]
    Nquads,
    #[value(alias = "xml", alias = "rdf")]
    Rdfxml,
}

#[cfg(feature = "rdf")]
#[derive(Debug, Args)]
pub struct RdfResolverArgs {
    /// Fetch unknown context documents over HTTP
    #[clap(long)]
    pub allow_remote: bool,
    /// Preload a context document from a file, as URL=PATH. Repeatable
    #[clap(long = "context", value_name = "URL=PATH")]
    pub contexts: Vec<String>,
}

#[cfg(feature = "rdf")]
#[derive(Debug, Args)]
pub struct RdfConversionArgs {
    /// Base IRI for relative IDs when the context defines no @base
    #[clap(long, conflicts_with = "allow_relative")]
    pub base: Option<String>,
    /// Leave relative IRIs unresolved instead of failing
    #[clap(long)]
    pub allow_relative: bool,
    #[clap(flatten)]
    pub resolver: RdfResolverArgs,
}

#[cfg(feature = "rdf")]
#[derive(Debug, Args)]
pub struct RdfExportCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Output format (default: from the output extension, else turtle)
    #[clap(short, long, value_enum)]
    pub format: Option<RdfFormatArg>,
    /// Output file (default: stdout)
    #[clap(short, long)]
    pub output: Option<String>,
    #[clap(flatten)]
    pub conversion: RdfConversionArgs,
}

#[cfg(feature = "rdf")]
#[derive(Debug, Args)]
pub struct RdfImportCommand {
    /// RDF file, or `-` to read stdin
    pub input: String,
    /// Input format (default: from the input extension, else turtle)
    #[clap(short, long, value_enum)]
    pub format: Option<RdfFormatArg>,
    /// Base IRI for resolving relative IRIs in the input
    #[clap(long)]
    pub base: Option<String>,
    /// Output metadata file, or `-` for stdout
    #[clap(short, long, default_value = "ro-crate-metadata.json")]
    pub output: String,
}

#[cfg(feature = "rdf")]
#[derive(Debug, Args)]
pub struct RdfRoundtripCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Output metadata file (default: stdout)
    #[clap(short, long)]
    pub output: Option<String>,
    #[clap(flatten)]
    pub conversion: RdfConversionArgs,
}

#[cfg(feature = "rdf")]
#[derive(Debug, Args)]
pub struct RdfExpandCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Term or relative ID to expand (e.g. name, schema:Person, data.csv)
    pub term: String,
    /// Fail if the term cannot be made absolute
    #[clap(long)]
    pub strict: bool,
    #[clap(flatten)]
    pub resolver: RdfResolverArgs,
}

#[cfg(feature = "rdf")]
#[derive(Debug, Args)]
pub struct RdfCompactCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Full IRI to compact
    pub iri: String,
    #[clap(flatten)]
    pub resolver: RdfResolverArgs,
}

#[cfg(feature = "parquet")]
#[derive(Debug, Subcommand)]
pub enum ConvertCommand {
    /// Write the crate as a CSV table
    Csv(ConvertOutputCommand),
    /// Write the crate as a Parquet table
    Parquet(ConvertOutputCommand),
}

#[cfg(feature = "parquet")]
#[derive(Debug, Args)]
pub struct ConvertOutputCommand {
    #[clap(flatten)]
    pub target: CrateTarget,
    /// Output file
    #[clap(short, long)]
    pub output: String,
}
