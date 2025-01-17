//! Contains all the arguments for cli tool
//!
//! Built with clap

use clap::{Args, Parser, Subcommand};
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
    // Allows you to run basic validation over a ro-crate-metadata.json file
    #[clap(subcommand)]
    Validate(ValidateCommand),
}

#[derive(Debug, Args)]
pub struct InitCommand {
    /// Default ro-crate initialisation using latest spec
    #[clap(short, long)]
    pub default: bool,
    /// Type of context
    #[clap(required_unless_present = "default")]
    #[clap(required_unless_present = "minimal")]
    #[clap(short, long, name="context", help=CONTEXT_HELP)]
    pub context_type: Option<ContextType>,
    /// Initialise with default minimal entites or leave empty
    #[clap(short, long)]
    pub minimal: bool,
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
    // Target crate
    #[clap(short,long,default_value_t=String::from("./"))]
    pub target_crate: String,
    // Copy and include external reachable data files
    #[clap(short, long, default_value_t = true)]
    pub external: bool,
    // Flatten contents to remove folder stucture in zip
    #[clap(short, long, default_value_t = false)]
    pub flatten: bool,
}

#[derive(Debug, Subcommand)]
pub enum ReadCommand {
    /// Read full crate
    Crate(ReadCrateCommand),
    /// Read entity of crate
    Entity(ReadEntityCommand),
    /// Read all of one field in crate
    Fields(ReadFieldsCommand),
    /// Read entity containing specific value
    Value(ReadValueCommand),
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
    /// Raw struct data
    #[clap(short, long)]
    pub raw_struct: bool,
    /// Prints full view without trimming
    #[clap(short, long)]
    pub fit: bool,
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
    // Target crate
    #[clap(
        short,
        long,
        required = false,
        default_value = "ro-crate-metadata.json"
    )]
    pub target_crate: String,
}
