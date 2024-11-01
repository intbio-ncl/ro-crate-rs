# Overview 

The ro-crate-rs cli tool can be used to immediately interface with RO-Crates within a somewhat constrained and structured environment, however an understanding of RO-Crate is a realistic requirement to conform with schema.org specifications. 

# Basics 

This cli allows basic interaction with an RO-Crate, such as reading, writing, updating and so on.

## Initial commands 

```bash
Usage: rocrate <COMMAND>

Commands:
  init     Initialise a new empty Ro-Crate
  add      Add an entity to an Ro-Crate
  delete   Delete an entity in an Ro-Crate
  modify   Modify a particular entity within an Ro-Crate (includes Root and Descriptor)
  read     Read the crate and display
  package  Allows you to package crate into different formats
  help     Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

## Init command

```bash
Initialise a new empty Ro-Crate

Usage: rocrate init [OPTIONS]

Options:
  -d, --default                 Default ro-crate initialisation using latest spec
  -c, --context-type <context>  Type of context:
                                1 - Reference Context: Basic context type with minimal fields.
                                2 - Extended Context: Includes additional metadata fields.
                                3 - Embedded Context: Contains embedded data for richer context.
  -m, --minimal                 Initialise with default minimal entites or leave empty
  -h, --help                    Print help 
```

## Add command 
```bash
Add an entity to an Ro-Crate

Usage: rocrate add [OPTIONS] <ID> [DATATYPE]...

Arguments:
  <ID>           Input ID
  [DATATYPE]...  Input Datatype. This can be a single datatype, or a list seperated by ',' (e.g type1,type2)

Options:
  -t, --target-crate <TARGET_CRATE>  Target crate [default: ro-crate-metadata.json]
  -e, --extra-information            Extra information. Allows you to input Custom fields and values
  -h, --help                         Print help
```
## Delete command 
```bash
Delete an entity in an Ro-Crate

Usage: rocrate delete [OPTIONS] <ID>

Arguments:
  <ID>  Input ID to delete

Options:
  -t, --target-crate <TARGET_CRATE>  Target crate [default: ro-crate-metadata.json]
  -r, --recursive                    Recursive deletion. Allows you to delete all occurances of the ID linked in other research objects
  -h, --help                         Print help
```

## Modify command 
```bash
Modify a particular entity within an Ro-Crate (includes Root and Descriptor)

Usage: rocrate modify <COMMAND>

Commands:
  add-string         Add a string value to an entity
  add-id-value       Add a ID to an entity
  add-id-vec-values  Add a list of ID's to an entity
  add-multiple       Add multiple new fields to entity - Useful for large crates
  remove-field       Remove a specific field from an entity
  help               Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```
### add-string 
```bash
Add a string value to an entity

Usage: rocrate modify add-string [OPTIONS] <ID> <KEY> <VALUE>

Arguments:
  <ID>     Input ID to add to
  <KEY>    Key to add
  <VALUE>  Value to add

Options:
  -t, --target-crate <TARGET_CRATE>  Target crate [default: ro-crate-metadata.json]
  -h, --help                         Print help
```
### add-id-value 
```bash
Add a ID to an entity

Usage: rocrate modify add-id-value [OPTIONS] <ID> <KEY> <VALUE>

Arguments:
  <ID>     Input ID to target
  <KEY>    Key to add
  <VALUE>  ID value to add

Options:
  -t, --target-crate <TARGET_CRATE>  Target crate [default: ro-crate-metadata.json]
  -h, --help                         Print help
```
### add-id-vec-values 
```bash
Add a list of ID's to an entity

Usage: rocrate modify add-id-vec-values [OPTIONS] <ID> <KEY> [VALUES]...

Arguments:
  <ID>         Input ID to target
  <KEY>        Key to add
  [VALUES]...  ID values to add. This can be a single id, or a list seperated by ',' (e.g id1,id2)

Options:
  -t, --target-crate <TARGET_CRATE>  Target crate [default: ro-crate-metadata.json]
  -h, --help                         Print help

```
### add-multiple
```bash
Add multiple new fields to entity - Useful for large crates

Usage: rocrate modify add-multiple [OPTIONS] <ID>

Arguments:
  <ID>  Input ID to target

Options:
  -t, --target-crate <TARGET_CRATE>  Target crate [default: ro-crate-metadata.json]
  -h, --help                         Print help
```
### remove-field 
```bash
Remove a specific field from an entity

Usage: rocrate modify remove-field [OPTIONS] <ID> <FIELD>

Arguments:
  <ID>     Input ID to delete
  <FIELD>  Field to delete

Options:
  -t, --target-crate <TARGET_CRATE>  Target crate [default: ro-crate-metadata.json]
  -h, --help                         Print help
```

## Read command 
```bash
Read the crate and display

Usage: rocrate read <COMMAND>

Commands:
  crate   Read full crate
  entity  Read entity of crate
  fields  Read all of one field in crate
  value   Read entity containing specific value
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

### crate 
```bash
Read full crate

Usage: rocrate read crate [OPTIONS]

Options:
  -t, --target-crate <TARGET_CRATE>  Target crate [default: ro-crate-metadata.json]
  -r, --raw-struct                   Raw struct data
  -h, --help                         Print help
```
### entity 
```bash
Read entity of crate

Usage: rocrate read entity [OPTIONS] <ID>

Arguments:
  <ID>  Entity ID to search

Options:
  -t, --target-crate <TARGET_CRATE>  Target crate [default: ro-crate-metadata.json]
  -r, --raw-struct                   Raw struct data
  -h, --help                         Print help
```
### fields
```bash
Read all of one field in crate

Usage: rocrate read fields [OPTIONS] <FIELD>

Arguments:
  <FIELD>  Field to search for

Options:
  -t, --target-crate <TARGET_CRATE>  Target crate [default: ro-crate-metadata.json]
  -h, --help                         Print help
```
### value 
```bash
Read entity containing specific value

Usage: rocrate read value [OPTIONS] <VALUE>

Arguments:
  <VALUE>  Field to search for

Options:
  -t, --target-crate <TARGET_CRATE>  Target crate [default: ro-crate-metadata.json]
  -l, --location                     Show the object location
  -h, --help                         Print help
```
## Package command 
```bash
Allows you to package crate into different formats

Usage: rocrate package <COMMAND>

Commands:
  zip   Read full crate
  help  Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```
### zip 
```bash
Read full crate

Usage: rocrate package zip [OPTIONS]

Options:
  -t, --target-folder <TARGET_FOLDER>  [default: ./]
  -h, --help                           Print help
```

