use std::collections::HashMap;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rocraters::ro_crate::constraints::{DataType, EntityValue, Id, License};
use rocraters::ro_crate::context::RoCrateContext;
use rocraters::ro_crate::contextual_entity::ContextualEntity;
use rocraters::ro_crate::data_entity::DataEntity;
use rocraters::ro_crate::graph_vector::GraphVector;
use rocraters::ro_crate::metadata_descriptor::MetadataDescriptor;
use rocraters::ro_crate::rdf::{
    ContextResolverBuilder, ConversionOptions, RdfFormat, RdfGraph, rdf_graph_to_rocrate,
    rdf_to_rocrate, rocrate_to_rdf_with_options,
};
use rocraters::ro_crate::rocrate::RoCrate;
use rocraters::ro_crate::root::RootDataEntity;

const BASE_IRI: &str = "https://example.org/bench/";
const ROCRATE_CONTEXT_IRI: &str = "https://w3id.org/ro/crate/1.2/context";

struct BenchmarkInput {
    entities: usize,
    triples: usize,
    rocrate: RoCrate,
    rdf_graph: RdfGraph,
    turtle: String,
}

fn benchmark_input(data_entities: usize) -> BenchmarkInput {
    let rocrate = synthetic_rocrate(data_entities);
    let rdf_graph = rocrate_to_rdf_with_options(
        &rocrate,
        ContextResolverBuilder::default(),
        ConversionOptions::with_base(BASE_IRI),
    )
    .expect("serialize benchmark fixture");
    let turtle = rdf_graph
        .to_string(RdfFormat::Turtle)
        .expect("serialize turtle benchmark fixture");

    BenchmarkInput {
        entities: rocrate.graph.len(),
        triples: rdf_graph.len(),
        rocrate,
        rdf_graph,
        turtle,
    }
}

fn synthetic_rocrate(data_entities: usize) -> RoCrate {
    let author_count = data_entities.div_ceil(16).clamp(4, 64);
    let authors_per_file = 3usize.min(author_count.max(1));

    let mut graph = Vec::with_capacity(data_entities + author_count + 2);
    graph.push(GraphVector::MetadataDescriptor(MetadataDescriptor {
        id: "ro-crate-metadata.json".to_string(),
        type_: DataType::Term("CreativeWork".to_string()),
        conforms_to: Id::Id(ROCRATE_CONTEXT_IRI.to_string()),
        about: Id::Id("./".to_string()),
        dynamic_entity: None,
    }));

    let data_ids: Vec<String> = (0..data_entities)
        .map(|index| format!("./data/file-{index:05}.csv"))
        .collect();

    let mut root_dynamic = HashMap::with_capacity(2);
    root_dynamic.insert(
        "hasPart".to_string(),
        EntityValue::EntityId(Id::IdArray(data_ids.clone())),
    );
    root_dynamic.insert(
        "creator".to_string(),
        EntityValue::EntityId(Id::IdArray(
            (0..author_count)
                .map(|index| format!("#person-{index:03}"))
                .collect(),
        )),
    );

    graph.push(GraphVector::RootDataEntity(RootDataEntity {
        id: "./".to_string(),
        type_: DataType::Term("Dataset".to_string()),
        name: format!("Synthetic RDF benchmark crate ({data_entities} files)"),
        description: "Synthetic benchmark for RO-Crate RDF serialization and deserialization"
            .to_string(),
        date_published: "2026-03-25".to_string(),
        license: License::Id(Id::Id("https://spdx.org/licenses/Apache-2.0".to_string())),
        dynamic_entity: Some(root_dynamic),
    }));

    for index in 0..author_count {
        let mut dynamic_entity = HashMap::with_capacity(3);
        dynamic_entity.insert(
            "name".to_string(),
            EntityValue::EntityString(format!("Researcher {index:03}")),
        );
        dynamic_entity.insert(
            "affiliation".to_string(),
            EntityValue::EntityString(format!("Benchmark Lab {}", index % 8)),
        );
        dynamic_entity.insert(
            "email".to_string(),
            EntityValue::EntityString(format!("researcher{index:03}@example.org")),
        );

        graph.push(GraphVector::ContextualEntity(ContextualEntity {
            id: format!("#person-{index:03}"),
            type_: DataType::Term("Person".to_string()),
            dynamic_entity: Some(dynamic_entity),
        }));
    }

    for (index, data_id) in data_ids.iter().enumerate() {
        let mut dynamic_entity = HashMap::with_capacity(5);
        dynamic_entity.insert(
            "name".to_string(),
            EntityValue::EntityString(format!("Measurement file {index:05}")),
        );
        dynamic_entity.insert(
            "encodingFormat".to_string(),
            EntityValue::EntityString("text/csv".to_string()),
        );
        dynamic_entity.insert(
            "author".to_string(),
            EntityValue::EntityId(Id::IdArray(
                (0..authors_per_file)
                    .map(|offset| format!("#person-{:03}", (index + offset) % author_count))
                    .collect(),
            )),
        );
        dynamic_entity.insert(
            "isPartOf".to_string(),
            EntityValue::EntityId(Id::Id("./".to_string())),
        );
        dynamic_entity.insert(
            "contentSize".to_string(),
            EntityValue::Entityi64(2_048 + ((index % 97) as i64 * 13)),
        );

        graph.push(GraphVector::DataEntity(DataEntity {
            id: data_id.clone(),
            type_: DataType::TermArray(vec!["File".to_string(), "MediaObject".to_string()]),
            dynamic_entity: Some(dynamic_entity),
        }));
    }

    RoCrate {
        context: RoCrateContext::ReferenceContext(ROCRATE_CONTEXT_IRI.to_string()),
        graph,
    }
}

fn bench_rocrate_to_rdf(c: &mut Criterion) {
    let mut group = c.benchmark_group("rdf_export");

    for size in [100usize, 1_000, 4_000] {
        let input = benchmark_input(size);
        group.throughput(Throughput::Elements(input.entities as u64));
        group.bench_with_input(
            BenchmarkId::new("rocrate_to_rdf", size),
            &input,
            |b, input| {
                b.iter(|| {
                    rocrate_to_rdf_with_options(
                        black_box(&input.rocrate),
                        ContextResolverBuilder::default(),
                        ConversionOptions::with_base(BASE_IRI),
                    )
                    .expect("serialize benchmark iteration")
                });
            },
        );

        group.throughput(Throughput::Elements(input.triples as u64));
        group.bench_with_input(
            BenchmarkId::new("rdf_graph_to_turtle", size),
            &input,
            |b, input| {
                b.iter(|| {
                    black_box(&input.rdf_graph)
                        .to_string(RdfFormat::Turtle)
                        .expect("turtle serialization benchmark iteration")
                });
            },
        );
    }

    group.finish();
}

fn bench_rdf_to_rocrate(c: &mut Criterion) {
    let mut group = c.benchmark_group("rdf_import");

    for size in [100usize, 1_000, 4_000] {
        let input = benchmark_input(size);
        group.throughput(Throughput::Elements(input.triples as u64));
        group.bench_with_input(
            BenchmarkId::new("turtle_to_rocrate", size),
            &input,
            |b, input| {
                b.iter(|| {
                    rdf_to_rocrate(
                        black_box(input.turtle.as_str()),
                        RdfFormat::Turtle,
                        Some(BASE_IRI),
                    )
                    .expect("import benchmark iteration")
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("graph_to_rocrate", size),
            &input,
            |b, input| {
                b.iter(|| {
                    rdf_graph_to_rocrate(black_box(input.rdf_graph.clone()))
                        .expect("graph import benchmark iteration")
                });
            },
        );
    }

    group.finish();
}

criterion_group!(rdf_benches, bench_rocrate_to_rdf, bench_rdf_to_rocrate);
criterion_main!(rdf_benches);
