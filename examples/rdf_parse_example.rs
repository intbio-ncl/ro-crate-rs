/// Example demonstrating parsing RDF data into a RO-Crate structure.
///
/// This example shows how to use the rdf_to_rocrate function to parse
/// Turtle format RDF and convert it to a RO-Crate.
///
/// Run with:
/// cargo run --example rdf_parse_example --features rdf

#[cfg(feature = "rdf")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use rocraters::ro_crate::rdf::{rdf_to_rocrate, RdfFormat};

    // Example Turtle RDF data representing a minimal RO-Crate
    let turtle_data = r#"
        @prefix schema: <http://schema.org/> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        <http://example.org/my-crate/ro-crate-metadata.json> a schema:CreativeWork ;
            schema:conformsTo <https://w3id.org/ro/crate/1.2/context> ;
            schema:about <http://example.org/my-crate/> .

        <http://example.org/my-crate/> a schema:Dataset ;
            schema:name "Example Research Dataset" ;
            schema:description "A dataset demonstrating RDF parsing" ;
            schema:datePublished "2025-12-22" ;
            schema:license <https://spdx.org/licenses/MIT> .
    "#;

    println!("Parsing Turtle RDF data...");

    // Parse the RDF data with a base IRI
    let rocrate = rdf_to_rocrate(
        turtle_data,
        RdfFormat::Turtle,
        Some("http://example.org/my-crate/"),
    )?;

    println!("Successfully parsed RO-Crate!");
    println!("Number of entities in graph: {}", rocrate.graph.len());
    println!("\nContext: {:?}", rocrate.context);
    println!("\nGraph entities:");
    for entity in &rocrate.graph {
        println!("  - {:?}", entity);
    }

    Ok(())
}

#[cfg(not(feature = "rdf"))]
fn main() {
    eprintln!("This example requires the 'rdf' feature to be enabled.");
    eprintln!("Run with: cargo run --example rdf_parse_example --features rdf");
    std::process::exit(1);
}
