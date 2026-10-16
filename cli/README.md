# Overview 

The ro-crate-rs cli tool can be used to immediately interface with RO-Crates within a somewhat constrained and structured environment, however an understanding of RO-Crate is a realistic requirement to conform with schema.org specifications. 

# Basics 

This cli allows basic interaction with an RO-Crate, such as reading, writing, updating, validating and so on.

# Installion 

Via Cargo (need rust installed):
```
cargo install ro-crate-rs-cli
```

Or via github release page, choose your target platform and download the binary. Once download it can be used in whichever command line you choose.

# Usage

Commands act on `./ro-crate-metadata.json` unless given `-t <target>`. Commands
that only read a crate also accept a `.zip` archive, an `http(s)` URL or `-` for
stdin. Run `rocraters <command> --help` for every option.

```bash
# Create a crate with the required entities, without prompts
rocraters init -m --context-item https://w3id.org/ro/crate/1.3/context

# Add and describe entities
rocraters add data.csv File
rocraters modify add-field data.csv name "Results"
rocraters modify add-field data.csv contentSize 42 --json
rocraters modify add-field data.csv author '{"@id": "#alice"}' --json
rocraters modify upsert '{"@id": "#alice", "@type": "Person", "name": "Alice"}'
rocraters modify add-has-part data.csv
rocraters modify add-type data.csv Dataset
rocraters modify replace-id data.csv results/data.csv

# Query
rocraters read ids
rocraters read properties
rocraters read links results/data.csv
rocraters read entity results/data.csv --json
rocraters read crate -t https://example.org/ro-crate-metadata.json --json

# Context
rocraters context show
rocraters context add '{"@base": "https://example.org/crate/"}'
rocraters context get @base
rocraters context version

# Validate (exits non-zero if the crate is not valid)
rocraters validate basic --json
rocraters schema 1.3 --terms

# Subcrates
rocraters subcrates list
rocraters subcrates fetch --recursive --lenient
rocraters subcrates fetch --allow-remote  # only for trusted subcrate URLs

# RDF
rocraters rdf export --base https://example.org/crate/ -o crate.ttl
rocraters rdf import crate.ttl -o ro-crate-metadata.json
rocraters rdf expand name

# Package, naming the zip after a new urn:uuid @base
rocraters context add-urn
rocraters package zip --unique
```

RDF and subcrate fetching are built by default. Parquet and CSV export
(`rocraters convert parquet|csv`) is opt-in:

```bash
cargo install ro-crate-rs-cli --features parquet
```

See [docs/parity.md](../docs/parity.md) for how every core function maps to a
CLI command and the Python API.
