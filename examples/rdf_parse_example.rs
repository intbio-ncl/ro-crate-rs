//! Example: Parsing RDF data into an RO-Crate
//!
//! This example demonstrates how to parse RDF data (in Turtle format)
//! and convert it into an RO-Crate structure.
//!
//! Run with:
//! ```bash
//! cargo run --example rdf_parse_example --features rdf
//! ```

#[cfg(feature = "rdf")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use rocraters::ro_crate::rdf::{rdf_to_rocrate, RdfFormat};
    use rocraters::ro_crate::write::write_crate;

    // RDF data representing an RO-Crate in Turtle format
    let turtle_data = r#"
        @prefix schema: <http://schema.org/> .

        <ro-crate-metadata.json> a schema:CreativeWork ;
            schema:conformsTo <https://w3id.org/ro/crate/1.2> ;
            schema:about <./> .

        <./> a schema:Dataset ;
            schema:name "Parsed RDF Dataset" ;
            schema:description "An RO-Crate created from RDF triples" ;
            schema:datePublished "2025-01-15" ;
            schema:license <https://spdx.org/licenses/Apache-2.0> .

        <data/measurements.csv> a schema:MediaObject ;
            schema:name "Measurement Data" ;
            schema:encodingFormat "text/csv" .
    "#;

    println!("=== Parsing RDF to RO-Crate ===\n");

    // Parse Turtle RDF into an RO-Crate structure
    let rocrate = rdf_to_rocrate(
        turtle_data,
        RdfFormat::Turtle,
        Some("http://example.org/research/"),
    )?;

    println!("Parsed {} entities from RDF\n", rocrate.graph.len());

    // Display each entity in the graph
    for (i, entity) in rocrate.graph.iter().enumerate() {
        println!("Entity {}: {:?}", i + 1, entity);
    }

    // The parsed RO-Crate can be serialized back to JSON-LD
    println!("\n--- Serialized to JSON-LD ---");
    let json = serde_json::to_string_pretty(&rocrate)?;
    println!("{}", json);

    // Or written to a file
    write_crate(&rocrate, "parsed-ro-crate-metadata.json".to_string());
    println!("\nWritten to parsed-ro-crate-metadata.json");

    Ok(())
}

#[cfg(not(feature = "rdf"))]
fn main() {
    eprintln!("This example requires the 'rdf' feature.");
    eprintln!("Run with: cargo run --example rdf_parse_example --features rdf");
    std::process::exit(1);
}
