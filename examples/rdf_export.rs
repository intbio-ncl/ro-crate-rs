//! Example: Exporting an RO-Crate to RDF formats
//!
//! This example demonstrates how to convert an RO-Crate to RDF triples
//! and serialize them to various formats (Turtle, N-Triples, RDF/XML).
//!
//! Run with:
//! ```bash
//! cargo run --example rdf_export --features rdf
//! ```

#[cfg(feature = "rdf")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use rocraters::ro_crate::constraints::{DataType, Id, License};
    use rocraters::ro_crate::metadata_descriptor::MetadataDescriptor;
    use rocraters::ro_crate::rdf::{
        rocrate_to_rdf, rocrate_to_rdf_with_options, ContextResolverBuilder, ConversionOptions,
        RdfFormat,
    };
    use rocraters::ro_crate::rocrate::{GraphVector, RoCrate, RoCrateContext};
    use rocraters::ro_crate::root::RootDataEntity;

    // Create a sample RO-Crate
    let rocrate = RoCrate {
        context: RoCrateContext::ReferenceContext(
            "https://w3id.org/ro/crate/1.2/context".to_string(),
        ),
        graph: vec![
            GraphVector::MetadataDescriptor(MetadataDescriptor {
                id: "ro-crate-metadata.json".to_string(),
                type_: DataType::Term("CreativeWork".to_string()),
                conforms_to: Id::Id("https://w3id.org/ro/crate/1.2".to_string()),
                about: Id::Id("./".to_string()),
                dynamic_entity: None,
            }),
            GraphVector::RootDataEntity(RootDataEntity {
                id: "./".to_string(),
                type_: DataType::Term("Dataset".to_string()),
                name: "Example Research Dataset".to_string(),
                description: "A dataset demonstrating RDF export capabilities".to_string(),
                date_published: "2025-01-15".to_string(),
                license: License::Id(Id::Id("https://spdx.org/licenses/MIT".to_string())),
                dynamic_entity: None,
            }),
        ],
    };

    // Convert RO-Crate to RDF using default options
    let resolver = ContextResolverBuilder::default();
    let rdf_graph = rocrate_to_rdf(&rocrate, resolver)?;

    println!("=== RO-Crate to RDF Conversion ===\n");
    println!("Generated {} triples\n", rdf_graph.len());

    // Export to Turtle format (human-readable)
    println!("--- Turtle Format ---");
    let turtle = rdf_graph.to_string(RdfFormat::Turtle)?;
    println!("{}\n", turtle);

    // Export to N-Triples format (simple, line-based)
    println!("--- N-Triples Format ---");
    let ntriples = rdf_graph.to_string(RdfFormat::NTriples)?;
    println!("{}\n", ntriples);

    // Convert with a custom base IRI
    println!("--- With Custom Base IRI ---");
    let resolver = ContextResolverBuilder::default();
    let rdf_with_base = rocrate_to_rdf_with_options(
        &rocrate,
        resolver,
        ConversionOptions::WithBase("https://example.org/my-research/".to_string()),
    )?;
    let turtle_with_base = rdf_with_base.to_string(RdfFormat::Turtle)?;
    println!("{}", turtle_with_base);

    Ok(())
}

#[cfg(not(feature = "rdf"))]
fn main() {
    eprintln!("This example requires the 'rdf' feature.");
    eprintln!("Run with: cargo run --example rdf_export --features rdf");
    std::process::exit(1);
}
