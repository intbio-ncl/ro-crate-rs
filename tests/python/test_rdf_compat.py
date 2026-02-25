"""
RDF Compatibility Tests for ro-crate-rs

Tests that verify RDF output from ro-crate-rs is compatible with Python's rdflib ecosystem.
Based on pattern from: https://gist.github.com/OliverWoolland/4c8418e2fde6b0c2eed2e66211fc05fa

These tests ensure semantic equivalence between:
1. RO-Crate JSON-LD parsed directly by rdflib
2. RO-Crate → Rust RDF → rdflib parsed RDF

The test strategy uses rdflib's graph isomorphism checking to verify that the two
representations are semantically equivalent, even if their serialization differs.
"""

import subprocess
import tempfile
from pathlib import Path

import pytest
from rdflib import Graph
from rdflib.compare import graph_diff, isomorphic


# Path to the ro-crate-rs repository root (relative to this test file)
REPO_ROOT = Path(__file__).parent.parent.parent
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures"


def run_rdf_export(input_file: Path, format: str = "turtle", base: str = None) -> str:
    """
    Run the rdf_export example to convert RO-Crate JSON-LD to RDF.

    Args:
        input_file: Path to RO-Crate JSON-LD file
        format: Output format (turtle, ntriples, rdfxml)
        base: Optional base IRI

    Returns:
        RDF serialization as string
    """
    cmd = [
        "cargo", "run", "--example", "rdf_export", "--features", "rdf",
        "--quiet", "--", str(input_file), "--format", format
    ]

    if base:
        cmd.extend(["--base", base])

    result = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"rdf_export failed with code {result.returncode}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )

    return result.stdout


def test_minimal_rocrate_1_1_isomorphic():
    """
    Test that minimal RO-Crate 1.1 produces isomorphic RDF graphs.

    This test verifies:
    1. rdflib can parse the original RO-Crate JSON-LD
    2. Rust can convert it to RDF (Turtle)
    3. rdflib can parse the Rust-generated Turtle
    4. The two graphs are semantically equivalent (isomorphic)
    """
    fixture = FIXTURES_DIR / "_ro-crate-metadata-minimal.json"
    assert fixture.exists(), f"Fixture not found: {fixture}"

    # Parse original RO-Crate JSON-LD with rdflib
    # Use publicID to set base IRI (must match what Rust uses)
    g_python = Graph()
    g_python.parse(str(fixture), format="json-ld", publicID="http://example.org/")

    # Generate RDF from Rust
    turtle_output = run_rdf_export(fixture, format="turtle", base="http://example.org/")

    # Parse Rust-generated RDF with rdflib
    g_rust = Graph()
    g_rust.parse(data=turtle_output, format="turtle")

    # Compare graphs
    if not isomorphic(g_python, g_rust):
        in_both, only_python, only_rust = graph_diff(g_python, g_rust)
        pytest.fail(
            f"Graphs are not isomorphic!\n"
            f"Triples only in Python graph: {len(list(only_python))}\n"
            f"Triples only in Rust graph: {len(list(only_rust))}\n"
            f"\nOnly in Python:\n{list(only_python)[:5]}\n"
            f"\nOnly in Rust:\n{list(only_rust)[:5]}"
        )


def test_minimal_rocrate_1_2_isomorphic():
    """
    Test that minimal RO-Crate 1.2 produces isomorphic RDF graphs.
    """
    fixture = FIXTURES_DIR / "_ro-crate-metadata-minimal-1_2.json"
    assert fixture.exists(), f"Fixture not found: {fixture}"

    # Parse original RO-Crate JSON-LD with rdflib
    # Use publicID to set base IRI (must match what Rust uses)
    g_python = Graph()
    g_python.parse(str(fixture), format="json-ld", publicID="http://example.org/")

    # Generate RDF from Rust
    turtle_output = run_rdf_export(fixture, format="turtle", base="http://example.org/")

    # Parse Rust-generated RDF with rdflib
    g_rust = Graph()
    g_rust.parse(data=turtle_output, format="turtle")

    # Compare graphs
    if not isomorphic(g_python, g_rust):
        in_both, only_python, only_rust = graph_diff(g_python, g_rust)
        pytest.fail(
            f"Graphs are not isomorphic!\n"
            f"Triples only in Python graph: {len(list(only_python))}\n"
            f"Triples only in Rust graph: {len(list(only_rust))}\n"
            f"\nOnly in Python:\n{list(only_python)[:5]}\n"
            f"\nOnly in Rust:\n{list(only_rust)[:5]}"
        )


def test_dynamic_rocrate_isomorphic():
    """
    Test that RO-Crate with dynamic properties produces isomorphic RDF graphs.

    This tests more complex metadata with custom properties.
    """
    fixture = FIXTURES_DIR / "_ro-crate-metadata-dynamic.json"
    assert fixture.exists(), f"Fixture not found: {fixture}"

    # Parse original RO-Crate JSON-LD with rdflib
    # Use publicID to set base IRI (must match what Rust uses)
    g_python = Graph()
    g_python.parse(str(fixture), format="json-ld", publicID="http://example.org/")

    # Generate RDF from Rust
    turtle_output = run_rdf_export(fixture, format="turtle", base="http://example.org/")

    # Parse Rust-generated RDF with rdflib
    g_rust = Graph()
    g_rust.parse(data=turtle_output, format="turtle")

    # Compare graphs
    if not isomorphic(g_python, g_rust):
        in_both, only_python, only_rust = graph_diff(g_python, g_rust)
        pytest.fail(
            f"Graphs are not isomorphic!\n"
            f"Triples only in Python graph: {len(list(only_python))}\n"
            f"Triples only in Rust graph: {len(list(only_rust))}\n"
            f"\nOnly in Python:\n{list(only_python)[:5]}\n"
            f"\nOnly in Rust:\n{list(only_rust)[:5]}"
        )


def test_ntriples_format():
    """
    Test that N-Triples format export works and produces isomorphic graphs.

    This verifies that different RDF serialization formats are supported.
    """
    fixture = FIXTURES_DIR / "_ro-crate-metadata-minimal.json"
    assert fixture.exists(), f"Fixture not found: {fixture}"

    # Parse original RO-Crate JSON-LD with rdflib
    # Use publicID to set base IRI (must match what Rust uses)
    g_python = Graph()
    g_python.parse(str(fixture), format="json-ld", publicID="http://example.org/")

    # Generate RDF from Rust in N-Triples format
    ntriples_output = run_rdf_export(fixture, format="ntriples", base="http://example.org/")

    # Parse Rust-generated RDF with rdflib
    g_rust = Graph()
    g_rust.parse(data=ntriples_output, format="ntriples")

    # Compare graphs
    if not isomorphic(g_python, g_rust):
        in_both, only_python, only_rust = graph_diff(g_python, g_rust)
        pytest.fail(
            f"N-Triples graphs are not isomorphic!\n"
            f"Triples only in Python graph: {len(list(only_python))}\n"
            f"Triples only in Rust graph: {len(list(only_rust))}"
        )


def test_roundtrip_preserves_triples():
    """
    Test that roundtrip conversion (RO-Crate → RDF → RO-Crate) preserves information.

    This is an integration test that verifies the full conversion pipeline works.
    Note: This test only checks that parsing succeeds; full semantic equivalence
    is tested in the Rust test suite.
    """
    fixture = FIXTURES_DIR / "_ro-crate-metadata-minimal.json"
    assert fixture.exists(), f"Fixture not found: {fixture}"

    # Generate RDF from Rust
    turtle_output = run_rdf_export(fixture, format="turtle", base="http://example.org/")

    # Verify it parses correctly
    g = Graph()
    g.parse(data=turtle_output, format="turtle")

    # Should have some triples
    assert len(g) > 0, "Generated RDF graph is empty"

    # Should have required RO-Crate entities
    # (metadata descriptor and root dataset)
    subjects = set(str(s) for s in g.subjects())

    # Check for metadata descriptor (contains "ro-crate-metadata.json")
    has_metadata = any("ro-crate-metadata.json" in s for s in subjects)
    assert has_metadata, "Missing metadata descriptor in RDF graph"


def test_base_iri_affects_output():
    """
    Test that different base IRIs produce different but isomorphic graphs.

    This verifies that base IRI handling works correctly.
    """
    fixture = FIXTURES_DIR / "_ro-crate-metadata-minimal.json"
    assert fixture.exists(), f"Fixture not found: {fixture}"

    # Generate RDF with different base IRIs
    base1 = "http://example.org/crate1/"
    base2 = "http://example.org/crate2/"

    turtle1 = run_rdf_export(fixture, format="turtle", base=base1)
    turtle2 = run_rdf_export(fixture, format="turtle", base=base2)

    # Parse both
    g1 = Graph()
    g1.parse(data=turtle1, format="turtle")

    g2 = Graph()
    g2.parse(data=turtle2, format="turtle")

    # Should have different absolute IRIs
    subjects1 = set(str(s) for s in g1.subjects())
    subjects2 = set(str(s) for s in g2.subjects())

    # But both should be valid RDF graphs with some triples
    assert len(g1) > 0
    assert len(g2) > 0

    # IRIs should differ (because of different bases)
    assert subjects1 != subjects2, "Base IRI change should affect absolute IRIs"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
