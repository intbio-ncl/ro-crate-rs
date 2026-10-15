# Core, CLI and Python parity

Every public operation in the `ro-crate-rs` core is available from the `rocraters`
CLI and the `rocraters` Python package. When adding to the core, add the CLI command
and Python binding in the same change and extend this table.

CLI commands that read a crate take `-t <target>`, which may be a metadata file,
a `.zip` archive, an `http(s)` URL or `-` for stdin. Commands that change a crate
need a metadata file. In Python, `crate` is a `PyRoCrate`.

## Reading and writing

| Core | CLI | Python |
| --- | --- | --- |
| `read::read_crate` | `read crate -t FILE [-l LEVEL]` | `read(path, level)` |
| `read::read_crate_obj` | `read crate -t -` | `read_object(json, level)` |
| `read::load_remote` | `read crate -t URL` | `read_remote(url, level)` |
| `write::write_crate` | written by every changing command | `crate.write(path)` |
| `Serialize for RoCrate` | `read crate --json` | `crate.to_json(pretty)` |
| `write::zip_crate` | `package zip [-e BOOL] [-f] [-u] [-l LEVEL] [-i PATH]...` | `zip(path, external, level, flatten, unique, include_hidden=None)` |
| `object_storage::relative_to_object_store` | `modify prefix-ids ROOT` | `prefix_object_id(crate, root)` |

## Validation and schema

| Core | CLI | Python |
| --- | --- | --- |
| `read::validate_crate_keys` | `validate basic -t TARGET [--json]` | `validate(path)`, `validate_object(json)`, `validate_zip(path)`, `validate_remote(url)`, `crate.validate()` |
| `schema::load_rocrate_schema` | `schema [VERSION] [--terms] [--json]` | `load_schema(version)` |

The CLI `--json` report and the Python report dictionary have the same shape. The
CLI exits non-zero when a crate is not valid.

## Creating crates and contexts

| Core | CLI | Python |
| --- | --- | --- |
| `RoCrate::new` + `RoCrateContext::ReferenceContext` | `init --context-item URL` | `PyRoCrate(PyRoCrateContext.from_string(url))` |
| `RoCrateContext::ExtendedContext` | `init --context-item ITEM --context-item ITEM` | `PyRoCrateContext.from_list(items)` |
| `RoCrateContext::EmbeddedContext` | `init --embedded --context-item JSON` | `PyRoCrateContext.from_embedded(maps)` |
| `RoCrate::default` + required entities | `init -m` | `PyRoCrate.new_default()` |

## Context

| Core | CLI | Python |
| --- | --- | --- |
| `RoCrate::add_context` | `context add ITEM` | `crate.add_context(item)` |
| `RoCrateContext::remove_context` | `context remove ITEM` | `crate.remove_context(item)` |
| `RoCrateContext::replace_context` | `context replace OLD NEW` | `crate.replace_context(new, old)` |
| `RoCrateContext::get_all_context` | `context list`, `context show` | `crate.get_all_context()` |
| `RoCrate::get_context_items` | `context keys` | `crate.get_context_items()` |
| `RoCrateContext::get_specific_context` | `context get KEY` | `crate.get_specific_context(key)` |
| `RoCrateContext::add_urn_uuid` | `context add-urn` | `crate.add_urn_uuid()` |
| `RoCrate::get_rocrate_version` | `context version` | `crate.get_rocrate_version()` |

A context item is a URL, or a JSON object (CLI) or dict (Python) of term definitions.

## Querying entities

| Core | CLI | Python |
| --- | --- | --- |
| `RoCrate::get_entity` | `read entity ID` | `crate.get_entity(id)` |
| `RoCrate::graph` | `read crate` | `crate.to_list()` |
| `RoCrate::get_all_ids` | `read ids` | `crate.get_all_ids()` |
| `RoCrate::get_all_properties` | `read properties` | `crate.get_all_properties()` |
| `RoCrate::get_all_property_values` | `read fields KEY` | `crate.get_all_property_values(key)` |
| `RoCrate::get_specific_value` | `read value VALUE [-l]` | `crate.get_specific_value(value)` |
| `GraphVector::get_linked_ids` | `read links ID` | `crate.get_linked_ids(id)` |
| `RoCrate::get_subcrates` | `subcrates list` | `crate.get_subcrates()` |

`read fields` and `read value` search the full JSON tree and report counts. Use
`read value -l` to print the objects holding a value, which shows the property.

## Changing entities

| Core | CLI | Python |
| --- | --- | --- |
| `RoCrate::overwrite_by_id` / push | `modify upsert JSON`, `add ID TYPES` | `crate.update_data`, `update_contextual`, `update_root`, `update_descriptor` |
| `RoCrate::remove_by_id` | `delete ID [-r]` | `crate.delete_entity(id, recursive)` |
| `RoCrate::update_id_recursive` | `modify replace-id OLD NEW` | `crate.replace_id(old, new)` |
| `RoCrate::remove_dynamic_entity_property` | `modify remove-field ID KEY` | `crate.remove_dynamic_entity_property(id, key)` |
| `GraphVector::add_type` | `modify add-type ID TYPE` | `crate.add_type(id, type)` |
| `RoCrate::add_data_to_partof_root` | `modify add-has-part ID` | `crate.add_data_to_partof_root(id)` |

## Subcrates (`subcrate-resolution` feature, on by default)

| Core | CLI | Python |
| --- | --- | --- |
| `fetch_subcrates` | `subcrates fetch` | `crate.fetch_subcrates()` |
| `fetch_subcrates_recursive` | `subcrates fetch -r` | `crate.fetch_subcrates(recursive=True)` |
| `try_fetch_subcrates` | `subcrates fetch --lenient` | `crate.fetch_subcrates(strict=False)` |
| `try_fetch_subcrates_recursive` | `subcrates fetch -r --lenient` | `crate.fetch_subcrates(recursive=True, strict=False)` |

HTTP fetching is disabled by default. Use `FetchOptions { allow_remote: true, ..Default::default() }` with the core
`*_with_options` functions, `subcrates fetch --allow-remote` in the CLI, or
`crate.fetch_subcrates(allow_remote=True)` in Python for trusted remote references.
Local paths are confined to the current working directory by default. The Rust
`FetchOptions.local_root` and Python `local_root` argument can select the crate directory;
the CLI derives it from the target metadata file.

## RDF (`rdf` feature, on by default)

| Core | CLI | Python |
| --- | --- | --- |
| `rocrate_to_rdf_with_options` + `RdfGraph::to_string` / `write` | `rdf export [-f FORMAT] [-o FILE]` | `crate.to_rdf(format)`, `crate.to_rdf_graph()`, `graph.serialize(format)`, `graph.write(path, format)` |
| `ConversionOptions::WithBase` / `AllowRelative` / `Strict` | `--base IRI`, `--allow-relative`, neither | `base=`, `allow_relative=`, neither |
| `ContextResolverBuilder::allow_remote` | `--allow-remote` | `allow_remote=True` |
| `ContextResolverBuilder::with_context` | `--context URL=PATH` | `contexts={url: json}` |
| `rdf_to_rocrate` | `rdf import FILE [-f FORMAT] [--base IRI]` | `from_rdf(data, format, base)` |
| `rdf_graph_to_rocrate` | `rdf roundtrip` | `graph.to_rocrate()` |
| `ResolvedContext::expand_term` / `expand_term_checked` | `rdf expand TERM [--strict]` | `crate.expand_term(term, strict)` |
| `ResolvedContext::compact_iri` | `rdf compact IRI` | `crate.compact_iri(iri)` |

Formats are `turtle`, `ntriples`, `nquads` and `rdfxml` (also `ttl`, `nt`, `nq`, `xml`).

## Tabular export (`parquet` feature, opt-in)

| Core | CLI | Python |
| --- | --- | --- |
| `convert::to_df` + `write_csv` | `convert csv -o FILE` | `write_csv(crate, path)` |
| `convert::to_df` + `write_parquet` | `convert parquet -o FILE` | `write_parquet(crate, path)` |

The table is keyed by the crate's `urn:uuid` `@base`, so add one first with
`context add-urn` or `crate.add_urn_uuid()`. Build with
`cargo build -p ro-crate-rs-cli --features parquet` or
`maturin build --features parquet`.

## Core calls worked around

Some core calls currently panic or have no effect, so the CLI and Python use the
replacements in `cli/src/core_compat.rs` and `python/src/core_compat.rs`. Each
replacement names the call it stands in for; switch back once the core is fixed.

| Core call | Problem | Replacement |
| --- | --- | --- |
| `RoCrate::add_data_to_partof_root` | looks the ID up as the root, so does nothing | `add_to_root_has_part` uses `RootDataEntity::add_entity_to_partof` |
| `RootDataEntity::add_entity_to_partof` | replaces a single existing `hasPart` ID | `add_to_root_has_part` makes it a list first |
| `RoCrateContext::add_urn_uuid` | does nothing for an extended context without embedded terms | `add_urn_uuid` adds an embedded `@base` |

Remaining core behaviour the tools pass through unchanged:

- `RoCrateContext::add_urn_uuid` gives every embedded term map its own `@base`
  when a crate has several maps and no `urn:uuid` yet.
- `validate_crate_keys` reports a plain-string `license` (such as the `"Private"`
  default from `init -m`) as a missing local ID.

## Not exposed

These public items are helpers or placeholders rather than operations on a crate:

- `read::crate_path`, `write::is_not_url`, `constraints::Id::contains_id`,
  `EntityValue::parse`: string and path helpers.
- `RoCrate::find_entity`, `find_entity_index`, `get_entity_mutable`,
  `GraphVector::get_entity*`, `overwrite`, `update_dynamic_entity_field`: lookups
  and in-place updates covered by the operations above.
- `RoCrate::to_parquet`, `DataType::modify_type`: empty placeholders.
- `convert::to_parquet`: writes to a discarded buffer and returns its size.
- `schema::load_rocrate_schema_from_str`: parses a schema document; validation
  only uses the bundled versions.
- `ResolvedContext`, `RdfGraph::insert`, `iter` and other triple-level access:
  internal RDF representation.
