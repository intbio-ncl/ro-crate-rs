//! Handlers for converting crates to and from RDF

use crate::args::{RdfCommand, RdfConversionArgs, RdfFormatArg, RdfImportCommand, RdfResolverArgs};
use crate::crate_io::{fail, load_crate, read_stdin, save_crate};
use rocraters::ro_crate::rdf::{
    rdf_graph_to_rocrate, rdf_to_rocrate, rocrate_to_rdf_with_options, ContextResolverBuilder,
    ConversionOptions, RdfFormat, RdfGraph,
};
use rocraters::ro_crate::rocrate::RoCrate;
use std::fs::{self, File};
use std::io;

impl From<RdfFormatArg> for RdfFormat {
    fn from(format: RdfFormatArg) -> Self {
        match format {
            RdfFormatArg::Turtle => RdfFormat::Turtle,
            RdfFormatArg::Ntriples => RdfFormat::NTriples,
            RdfFormatArg::Nquads => RdfFormat::NQuads,
            RdfFormatArg::Rdfxml => RdfFormat::RdfXml,
        }
    }
}

/// Uses the explicit format, else infers it from a file extension, else turtle
fn resolve_format(format: Option<RdfFormatArg>, path: Option<&str>) -> RdfFormat {
    if let Some(format) = format {
        return format.into();
    }
    let extension = path
        .and_then(|path| path.rsplit_once('.'))
        .map(|(_, extension)| extension.to_ascii_lowercase());
    match extension.as_deref() {
        Some("nt") => RdfFormat::NTriples,
        Some("nq") => RdfFormat::NQuads,
        Some("rdf" | "xml" | "owl") => RdfFormat::RdfXml,
        _ => RdfFormat::Turtle,
    }
}

fn build_resolver(args: &RdfResolverArgs) -> ContextResolverBuilder {
    let mut resolver = ContextResolverBuilder::new();
    for context in &args.contexts {
        let Some((url, path)) = context.split_once('=') else {
            fail(format!(
                "Invalid --context '{}': expected URL=PATH",
                context
            ));
        };
        let json = fs::read_to_string(path)
            .unwrap_or_else(|e| fail(format!("Failed to read context {}: {}", path, e)));
        resolver = resolver
            .with_context(url, &json)
            .unwrap_or_else(|e| fail(format!("Invalid context {}: {}", path, e)));
    }
    if args.allow_remote {
        resolver = resolver.allow_remote();
    }
    resolver
}

fn conversion_options(args: &RdfConversionArgs) -> ConversionOptions {
    match (&args.base, args.allow_relative) {
        (Some(base), _) => ConversionOptions::WithBase(base.clone()),
        (None, true) => ConversionOptions::AllowRelative,
        (None, false) => ConversionOptions::Strict,
    }
}

fn to_graph(rocrate: &RoCrate, args: &RdfConversionArgs) -> RdfGraph {
    rocrate_to_rdf_with_options(
        rocrate,
        build_resolver(&args.resolver),
        conversion_options(args),
    )
    .unwrap_or_else(|e| fail(format!("Failed to convert crate to RDF: {}", e)))
}

/// Writes a crate to a metadata file, or stdout when there is no path or it is `-`
fn output_crate(rocrate: &RoCrate, output: Option<&str>) {
    match output {
        Some(path) if path != "-" => save_crate(rocrate, path),
        _ => println!("{}", serde_json::to_string_pretty(rocrate).unwrap()),
    }
}

fn import(command: RdfImportCommand) {
    let input = if command.input == "-" {
        read_stdin()
    } else {
        fs::read_to_string(&command.input)
    }
    .unwrap_or_else(|e| fail(format!("Failed to read {}: {}", command.input, e)));

    let format = resolve_format(command.format, Some(&command.input));
    let rocrate = rdf_to_rocrate(&input, format, command.base.as_deref())
        .unwrap_or_else(|e| fail(format!("Failed to convert RDF to a crate: {}", e)));
    output_crate(&rocrate, Some(&command.output));
}

pub fn rdf(command: RdfCommand) {
    match command {
        RdfCommand::Export(command) => {
            let rocrate = load_crate(&command.target.target_crate, 1);
            let graph = to_graph(&rocrate, &command.conversion);
            let format = resolve_format(command.format, command.output.as_deref());

            let written = match &command.output {
                Some(path) => File::create(path)
                    .map_err(|e| e.to_string())
                    .and_then(|file| graph.write(file, format).map_err(|e| e.to_string())),
                None => graph.write(io::stdout(), format).map_err(|e| e.to_string()),
            };
            if let Err(e) = written {
                fail(format!("Failed to write RDF: {}", e));
            }
        }
        RdfCommand::Import(command) => import(command),
        RdfCommand::Roundtrip(command) => {
            let rocrate = load_crate(&command.target.target_crate, 1);
            let graph = to_graph(&rocrate, &command.conversion);
            let restored = rdf_graph_to_rocrate(graph)
                .unwrap_or_else(|e| fail(format!("Failed to convert RDF to a crate: {}", e)));
            output_crate(&restored, command.output.as_deref());
        }
        RdfCommand::Expand(command) => {
            let rocrate = load_crate(&command.target.target_crate, 1);
            let context = build_resolver(&command.resolver)
                .resolve(&rocrate.context)
                .unwrap_or_else(|e| fail(format!("Failed to resolve context: {}", e)));
            let expanded = if command.strict {
                context.expand_term_checked(&command.term, false)
            } else {
                context.expand_term(&command.term)
            };
            match expanded {
                Ok(iri) => println!("{}", iri),
                Err(e) => fail(e),
            }
        }
        RdfCommand::Compact(command) => {
            let rocrate = load_crate(&command.target.target_crate, 1);
            let context = build_resolver(&command.resolver)
                .resolve(&rocrate.context)
                .unwrap_or_else(|e| fail(format!("Failed to resolve context: {}", e)));
            println!("{}", context.compact_iri(&command.iri));
        }
    }
}
