# Overview 

ro-crate-rs is a rust library that aims to provide a robust, portable 
and scalable solution to dealing [RO-Crates](https://www.researchobject.org/ro-crate/1.1/) 
within the varying software environments that are present across a 
Synthetic Biology Laboratory stack (it's current focus). 
This implementation was specifically created to address the challenges of data 
handling on automated robotic systems within an automated synthetic biology lab,
however it's noted that this could have more general applicability to other environments.
It enforces minimal 
RO-Crate structure adherence through it's defined data types, but equally allows 
the crate to be as expansive as needed.

# Basic structure 

RO-Crates are based upon the JSON-LD (Javascript Object Notation for Linked Data Model),
using keys that have been defined in schema.org vocabularies, or other custom ontologies. In this library, 
RO-Crates are deserialised directly into a `RoCrate` structure using `serde`.
The base Ro-Crate is formed out of a `struct RoCrate` containg a `@context` and a `@graph`. 

The `@context` object contains a value refering to either a:  
- Reference Context
- Embedded Context
- Extended Context

The `@graph` object is a `vector` containing other RO-Crate entity structures that are one of the following:
- Metadata Descriptor 
- Root Data Entity
- Data Entity 
- Contextual Entity

Within the `@graph`, there can only be one Metadata Descriptor and Root Data Entity, whilst 
there can be zero to many Data Entities or Contextual Entities 

Each entity structure consists of an `@id` (or `id` for struct field) and a `@type`
(or `type_` for struct field). For Metadata Descriptor and Root Data Entity, they 
consist of other fields that have been determined to be a requirement by the 
RO-Crate specification. Fields that have been determined as MUST by the RO-Crate
specification are required in their relevant data structure, whilst fields determined
as SHOULD are Options. Any field that is not described as MUST or SHOULD within 
the specification can be added as a `dynamic_entity`.

Each entity structure also has a field called `dynamic_entity`, which allows the 
population of any form of JSON-LD compatible value within the statically typed
requirements of rust. These, by default, are instantiated as `None`.

Fig 1 is an example of a base RO-Crate:
![](../docs/ro-crate-structure.png "Basic RO-Crate structure")

Fig 2 is a diagram describing how each file relates to one another with a brief
intro to the main structures involved.
![](../docs/ro-crate-schema.svg "Brief overview of ro-crate-rs core")

# Usage

To use, you will need to install rust. It's strongly recommended to use the `rustup` tool.
Follow the installation found on the [rust website](https://www.rust-lang.org/tools/install). Once 
this is installed, you can then use cargo.

To compile for testing, use `cargo build` or `cargo build --release` is performance is a concern.

# Docs

To compile docs, run `cargo doc --open` and the documentation will open in your
default browser.





