//! End-to-end tests that run the rocraters binary

use serde_json::{json, Value};
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output, Stdio};

fn crate_json() -> Value {
    json!({
        "@context": "https://w3id.org/ro/crate/1.3/context",
        "@graph": [
            {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {"@id": "https://w3id.org/ro/crate/1.3"},
                "about": {"@id": "./"}
            },
            {
                "@id": "./",
                "@type": "Dataset",
                "name": "Example",
                "description": "Example crate",
                "datePublished": "2024",
                "license": {"@id": "https://spdx.org/licenses/MIT"},
                "hasPart": [{"@id": "data.csv"}]
            },
            {"@id": "data.csv", "@type": "File", "name": "Data", "author": {"@id": "#alice"}},
            {"@id": "#alice", "@type": "Person", "name": "Alice"}
        ]
    })
}

/// The example crate with data entities that reference other RO-Crates
fn crate_with_subcrates(ids: &[&str]) -> Value {
    let mut crate_ = crate_json();
    let graph = crate_["@graph"].as_array_mut().unwrap();
    for id in ids {
        graph.push(json!({
            "@id": id,
            "@type": "Dataset",
            "conformsTo": {"@id": "https://w3id.org/ro/crate"}
        }));
    }
    crate_
}

/// A temporary directory holding an example ro-crate-metadata.json
fn example_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    write_json(&dir.path().join("ro-crate-metadata.json"), &crate_json());
    dir
}

fn write_json(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_string_pretty(value).unwrap()).unwrap();
}

fn read_json(path: &Path) -> Value {
    serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

fn metadata(dir: &Path) -> Value {
    read_json(&dir.join("ro-crate-metadata.json"))
}

fn entity(dir: &Path, id: &str) -> Value {
    metadata(dir)["@graph"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entity| entity["@id"] == id)
        .cloned()
        .unwrap_or_else(|| panic!("no entity {id}"))
}

fn run(dir: &Path, args: &[&str], stdin: Option<&str>) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_rocraters"))
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.unwrap_or("").as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn ok(dir: &Path, args: &[&str]) -> String {
    ok_stdin(dir, args, "")
}

fn ok_stdin(dir: &Path, args: &[&str], stdin: &str) -> String {
    let output = run(dir, args, Some(stdin));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        output.status.success(),
        "{args:?} failed\nstdout: {stdout}\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    stdout
}

fn err(dir: &Path, args: &[&str]) -> String {
    let output = run(dir, args, None);
    assert!(!output.status.success(), "{args:?} unexpectedly succeeded");
    String::from_utf8(output.stderr).unwrap()
}

/// Asserts a command fails with a handled error (exit 1) rather than a panic (exit 101)
fn handled_err(dir: &Path, args: &[&str]) -> String {
    let output = run(dir, args, None);
    assert_eq!(output.status.code(), Some(1), "{args:?}");
    String::from_utf8(output.stderr).unwrap()
}

/// Stdout of a command whether or not it succeeds
fn ok_or_fail_text(dir: &Path, args: &[&str]) -> String {
    String::from_utf8(run(dir, args, None).stdout).unwrap()
}

fn lines(text: &str) -> Vec<&str> {
    text.lines().collect()
}

/// Serves `routes` as (path, body) over HTTP for the rest of the test run,
/// answering 404 for any other path, and returns the base URL. `{base}` in a
/// body is replaced with the base URL
fn serve(routes: Vec<(&'static str, String)>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let routes: Vec<(&str, String)> = routes
        .into_iter()
        .map(|(path, body)| (path, body.replace("{base}", &base)))
        .collect();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut request = [0; 8192];
            let read = stream.read(&mut request).unwrap_or(0);
            let request = String::from_utf8_lossy(&request[..read]);
            let path = request.split_whitespace().nth(1).unwrap_or("/");

            let (status, body) = match routes.iter().find(|(route, _)| *route == path) {
                Some((_, body)) => ("200 OK", body.as_str()),
                None => ("404 Not Found", "not found"),
            };
            let response = format!(
                "HTTP/1.1 {}\r\nContent-Type: application/ld+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                status,
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    base
}

mod init {
    use super::*;

    #[test]
    fn reference_context_item() {
        let dir = tempfile::tempdir().unwrap();
        ok(
            dir.path(),
            &[
                "init",
                "--context-item",
                "https://w3id.org/ro/crate/1.2/context",
            ],
        );
        assert_eq!(
            metadata(dir.path())["@context"],
            "https://w3id.org/ro/crate/1.2/context"
        );
    }

    #[test]
    fn extended_context_items_with_minimal_entities() {
        let dir = tempfile::tempdir().unwrap();
        ok(
            dir.path(),
            &[
                "init",
                "-m",
                "--context-item",
                "https://w3id.org/ro/crate/1.3/context",
                "--context-item",
                r#"{"@base": "https://example.org/"}"#,
            ],
        );
        let crate_ = metadata(dir.path());
        assert_eq!(
            crate_["@context"],
            json!(["https://w3id.org/ro/crate/1.3/context", {"@base": "https://example.org/"}])
        );
        assert_eq!(crate_["@graph"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn embedded_context() {
        let dir = tempfile::tempdir().unwrap();
        ok(
            dir.path(),
            &[
                "init",
                "--embedded",
                "--context-item",
                r#"{"name": "http://schema.org/name"}"#,
            ],
        );
        assert_eq!(
            metadata(dir.path())["@context"],
            json!([{"name": "http://schema.org/name"}])
        );
        err(
            dir.path(),
            &[
                "init",
                "--embedded",
                "--context-item",
                "https://example.org/ctx",
            ],
        );
    }

    #[test]
    fn default_is_empty_latest_spec() {
        let dir = tempfile::tempdir().unwrap();
        ok(dir.path(), &["init", "-d"]);
        let crate_ = metadata(dir.path());
        assert_eq!(crate_["@context"], "https://w3id.org/ro/crate/1.3/context");
        assert_eq!(crate_["@graph"], json!([]));
    }

    #[test]
    fn requires_a_mode() {
        let dir = tempfile::tempdir().unwrap();
        err(dir.path(), &["init"]);
        err(dir.path(), &["init", "-c", "9"]);
        err(
            dir.path(),
            &["init", "-d", "--context-item", "https://example.org/ctx"],
        );
    }

    #[test]
    fn interactive_reference_with_minimal_entities() {
        let dir = tempfile::tempdir().unwrap();
        ok_stdin(
            dir.path(),
            &["init", "-c", "1", "-m"],
            "https://w3id.org/ro/crate/1.2/context\n",
        );
        let crate_ = metadata(dir.path());
        assert_eq!(crate_["@context"], "https://w3id.org/ro/crate/1.2/context");
        assert_eq!(crate_["@graph"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn interactive_term_definitions() {
        let dir = tempfile::tempdir().unwrap();
        ok_stdin(
            dir.path(),
            &["init", "-c", "2"],
            "y\nname\nhttp://schema.org/name\nn\n",
        );
        assert_eq!(
            metadata(dir.path())["@context"],
            json!([{"name": "http://schema.org/name"}])
        );
    }

    #[test]
    fn interactive_reference_and_term_definitions() {
        let dir = tempfile::tempdir().unwrap();
        // 1 selects the RO-Crate 1.3 context, then no term definitions
        ok_stdin(dir.path(), &["init", "-c", "3"], "1\nn\n");
        assert_eq!(
            metadata(dir.path())["@context"],
            json!(["https://w3id.org/ro/crate/1.3/context", {}])
        );
    }

    #[test]
    fn minimal_uses_latest_spec() {
        let dir = tempfile::tempdir().unwrap();
        ok(dir.path(), &["init", "-m"]);
        assert_eq!(ok(dir.path(), &["context", "version"]).trim(), "1.3");
    }
}

mod modify {
    use super::*;

    #[test]
    fn add_field_string_and_json() {
        let dir = example_dir();
        let d = dir.path();
        ok(
            d,
            &[
                "modify",
                "add-field",
                "data.csv",
                "encodingFormat",
                "text/csv",
            ],
        );
        ok(
            d,
            &[
                "modify",
                "add-field",
                "data.csv",
                "contentSize",
                "42",
                "--json",
            ],
        );
        ok(
            d,
            &[
                "modify",
                "add-field",
                "data.csv",
                "isPartOf",
                r##"{"@id": "#set"}"##,
                "--json",
            ],
        );
        ok(
            d,
            &["modify", "add-field", "data.csv", "flag", "true", "--json"],
        );
        ok(d, &["modify", "add-field", "data.csv", "name", "Renamed"]);

        let data = entity(d, "data.csv");
        assert_eq!(data["encodingFormat"], "text/csv");
        assert_eq!(data["contentSize"], 42);
        assert_eq!(data["isPartOf"], json!({"@id": "#set"}));
        assert_eq!(data["flag"], true);
        assert_eq!(data["name"], "Renamed");

        err(d, &["modify", "add-field", "missing", "a", "b"]);
        err(
            d,
            &[
                "modify",
                "add-field",
                "data.csv",
                "a",
                "{not json",
                "--json",
            ],
        );
    }

    #[test]
    fn add_and_remove_type() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["modify", "add-type", "data.csv", "SoftwareSourceCode"]);
        assert_eq!(
            entity(d, "data.csv")["@type"],
            json!(["File", "SoftwareSourceCode"])
        );

        ok(d, &["modify", "remove-type", "data.csv", "File"]);
        assert_eq!(entity(d, "data.csv")["@type"], "SoftwareSourceCode");

        err(
            d,
            &["modify", "remove-type", "data.csv", "SoftwareSourceCode"],
        );
        err(d, &["modify", "add-type", "missing", "File"]);
    }

    #[test]
    fn replace_id_updates_links() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["modify", "replace-id", "#alice", "#bob"]);
        assert_eq!(entity(d, "data.csv")["author"], json!({"@id": "#bob"}));
        assert_eq!(entity(d, "#bob")["name"], "Alice");
    }

    #[test]
    fn add_has_part() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["modify", "add-has-part", "other.csv"]);
        assert_eq!(
            entity(d, "./")["hasPart"],
            json!([{"@id": "data.csv"}, {"@id": "other.csv"}])
        );
    }

    #[test]
    fn add_has_part_keeps_single_existing_part() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["modify", "add-id-value", "./", "hasPart", "data.csv"]);
        assert_eq!(entity(d, "./")["hasPart"], json!({"@id": "data.csv"}));

        ok(d, &["modify", "add-has-part", "other.csv"]);
        assert_eq!(
            entity(d, "./")["hasPart"],
            json!([{"@id": "data.csv"}, {"@id": "other.csv"}])
        );
    }

    #[test]
    fn root_and_contextual_entities() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["modify", "add-field", "./", "version", "2", "--json"]);
        ok(d, &["modify", "add-field", "#alice", "age", "30", "--json"]);
        ok(
            d,
            &["modify", "add-field", "ro-crate-metadata.json", "note", "x"],
        );
        assert_eq!(entity(d, "./")["version"], 2);
        assert_eq!(entity(d, "#alice")["age"], 30);
        assert_eq!(entity(d, "ro-crate-metadata.json")["note"], "x");

        ok(d, &["modify", "add-type", "#alice", "Researcher"]);
        ok(d, &["modify", "remove-type", "#alice", "Person"]);
        assert_eq!(entity(d, "#alice")["@type"], "Researcher");
        ok(d, &["modify", "add-type", "./", "SoftwareSourceCode"]);
        ok(d, &["modify", "remove-type", "./", "SoftwareSourceCode"]);
        assert_eq!(entity(d, "./")["@type"], "Dataset");
    }

    #[test]
    fn upsert_inserts_and_overwrites() {
        let dir = example_dir();
        let d = dir.path();
        ok(
            d,
            &[
                "modify",
                "upsert",
                r##"{"@id": "#org", "@type": "Organization", "name": "Org"}"##,
            ],
        );
        assert_eq!(entity(d, "#org")["name"], "Org");

        ok_stdin(
            d,
            &["modify", "upsert", "-"],
            r##"{"@id": "#alice", "@type": "Person", "name": "Alice Smith"}"##,
        );
        assert_eq!(entity(d, "#alice")["name"], "Alice Smith");
        assert_eq!(metadata(d)["@graph"].as_array().unwrap().len(), 5);

        err(d, &["modify", "upsert", r#"{"name": "no id"}"#]);
    }

    #[test]
    fn prefix_ids() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["modify", "prefix-ids", "s3://bucket/"]);
        let ids = ok(d, &["read", "ids"]);
        assert!(lines(&ids).contains(&"s3://bucket/data.csv"));
    }

    #[test]
    fn rejects_read_only_targets() {
        let dir = example_dir();
        let d = dir.path();
        for target in [
            "crate.zip",
            "-",
            "https://example.org/ro-crate-metadata.json",
        ] {
            let stderr = err(
                d,
                &["modify", "add-field", "-t", target, "data.csv", "a", "b"],
            );
            assert!(stderr.contains("Cannot update"), "{stderr}");
        }
    }
}

mod read {
    use super::*;

    #[test]
    fn ids_properties_and_links() {
        let dir = example_dir();
        let d = dir.path();
        assert_eq!(
            lines(&ok(d, &["read", "ids"])),
            ["ro-crate-metadata.json", "./", "data.csv", "#alice"]
        );
        assert_eq!(
            lines(&ok(d, &["read", "properties"])),
            ["author", "hasPart", "name"]
        );
        assert_eq!(lines(&ok(d, &["read", "links", "data.csv"])), ["#alice"]);

        let json: Value = serde_json::from_str(&ok(d, &["read", "ids", "--json"])).unwrap();
        assert_eq!(json.as_array().unwrap().len(), 4);
        err(d, &["read", "links", "missing"]);
    }

    #[test]
    fn crate_from_stdin() {
        let dir = tempfile::tempdir().unwrap();
        let out = ok_stdin(
            dir.path(),
            &["read", "crate", "-t", "-", "--json"],
            &crate_json().to_string(),
        );
        let crate_: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(crate_["@graph"].as_array().unwrap().len(), 4);
    }

    #[test]
    fn crate_from_url() {
        let dir = tempfile::tempdir().unwrap();
        let base = serve(vec![("/ro-crate-metadata.json", crate_json().to_string())]);
        let url = format!("{}/ro-crate-metadata.json", base);
        let ids = ok(dir.path(), &["read", "ids", "-t", &url]);
        assert!(lines(&ids).contains(&"#alice"));
        err(
            dir.path(),
            &["read", "ids", "-t", &format!("{}/missing.json", base)],
        );
    }

    #[test]
    fn entity_output_modes() {
        let dir = example_dir();
        let d = dir.path();
        let json: Value =
            serde_json::from_str(&ok(d, &["read", "entity", "data.csv", "--json"])).unwrap();
        assert_eq!(json, entity(d, "data.csv"));

        assert!(ok(d, &["read", "entity", "data.csv"]).contains("#alice"));
        assert!(ok(d, &["read", "entity", "data.csv", "-f"]).contains("#alice"));
        assert!(ok(d, &["read", "entity", "data.csv", "-r"]).contains("DataEntity"));
        err(d, &["read", "entity", "missing"]);
    }

    #[test]
    fn fields_and_values() {
        let dir = example_dir();
        let d = dir.path();
        let fields: Value =
            serde_json::from_str(&ok(d, &["read", "fields", "name", "--json"])).unwrap();
        assert!(fields
            .as_array()
            .unwrap()
            .contains(&json!(["#alice", "\"Alice\"", 1])));
        assert!(ok(d, &["read", "fields", "name"]).contains("Count"));

        let values: Value =
            serde_json::from_str(&ok(d, &["read", "value", "Alice", "--json"])).unwrap();
        assert_eq!(values, json!([["#alice", "Alice", 1]]));

        // Values inside arrays are counted once, without an entity ID
        ok(
            d,
            &[
                "modify",
                "add-field",
                "#alice",
                "alternateName",
                r#"["Al"]"#,
                "--json",
            ],
        );
        let values: Value =
            serde_json::from_str(&ok(d, &["read", "value", "Al", "--json"])).unwrap();
        assert_eq!(values, json!([["N/A", "Al", 1]]));
        assert!(ok(d, &["read", "value", "Al", "-l"]).contains("Found in value"));
        assert!(ok(d, &["read", "value", "Alice", "-l"]).contains("Found in object"));
        assert!(ok(d, &["read", "value", "Alice"]).contains("#alice"));
    }

    #[test]
    fn crate_output_modes_and_validation_level() {
        let dir = example_dir();
        let d = dir.path();
        assert!(ok(d, &["read", "crate"]).contains("#alice"));
        assert!(ok(d, &["read", "crate", "-f"]).contains("#alice"));
        assert!(ok(d, &["read", "crate", "-r"]).contains("RoCrate {"));

        ok(d, &["modify", "add-field", "data.csv", "notAKey", "x"]);
        err(d, &["read", "crate", "-l", "2"]);
        ok(d, &["read", "crate", "-l", "1", "--json"]);
    }

    #[test]
    fn zip_archive() {
        let parent = tempfile::tempdir().unwrap();
        let d = parent.path().join("mycrate");
        fs::create_dir(&d).unwrap();
        write_json(&d.join("ro-crate-metadata.json"), &crate_json());
        ok(&d, &["package", "zip"]);

        let crate_: Value =
            serde_json::from_str(&ok(&d, &["read", "zip", "-t", "mycrate.zip", "--json"])).unwrap();
        assert_eq!(crate_["@graph"].as_array().unwrap().len(), 4);
        assert!(ok(&d, &["read", "zip", "-t", "mycrate.zip"]).contains("#alice"));
        assert!(ok(&d, &["read", "zip", "-t", "mycrate.zip", "-r"]).contains("RoCrate {"));
        ok(&d, &["read", "zip", "-t", "mycrate.zip", "-l", "2"]);

        err(&d, &["read", "zip", "-t", "ro-crate-metadata.json"]);
        err(&d, &["read", "zip", "-t", "mycrate.zip", "-l", "5"]);
        handled_err(&d, &["read", "zip", "-t", "missing.zip"]);
        fs::write(d.join("not-a.zip"), "plain text").unwrap();
        handled_err(&d, &["read", "zip", "-t", "not-a.zip"]);
        handled_err(&d, &["read", "ids", "-t", "not-a.zip"]);
        handled_err(&d, &["validate", "basic", "-t", "not-a.zip"]);
    }
}

mod entities {
    use super::*;

    #[test]
    fn add() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["add", "new.txt", "File"]);
        assert_eq!(entity(d, "new.txt")["@type"], "File");

        ok(d, &["add", "multi", "Dataset,File"]);
        assert_eq!(entity(d, "multi")["@type"], json!(["Dataset", "File"]));

        // -e prompts for a field type (4: integer), key and value
        ok_stdin(
            d,
            &["add", "sized.txt", "File", "-e"],
            "4\ncontentSize\n42\n",
        );
        assert_eq!(entity(d, "sized.txt")["contentSize"], 42);
    }

    #[test]
    fn delete() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["delete", "#alice"]);
        assert!(!lines(&ok(d, &["read", "ids"])).contains(&"#alice"));
        assert_eq!(entity(d, "data.csv")["author"], json!({"@id": "#alice"}));
    }

    #[test]
    fn delete_recursive() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["delete", "#alice", "-r"]);
        assert!(!lines(&ok(d, &["read", "ids"])).contains(&"#alice"));
        assert!(ok(d, &["read", "links", "data.csv"]).is_empty());
    }

    #[test]
    fn add_id_values() {
        let dir = example_dir();
        let d = dir.path();
        ok(
            d,
            &["modify", "add-id-value", "data.csv", "citation", "#paper"],
        );
        assert_eq!(entity(d, "data.csv")["citation"], json!({"@id": "#paper"}));

        ok(
            d,
            &[
                "modify",
                "add-id-vec-values",
                "./",
                "hasPart",
                "a.txt,b.txt",
            ],
        );
        assert_eq!(
            entity(d, "./")["hasPart"],
            json!([{"@id": "a.txt"}, {"@id": "b.txt"}])
        );
    }

    #[test]
    fn remove_field() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["modify", "remove-field", "data.csv", "author"]);
        assert!(entity(d, "data.csv").get("author").is_none());
    }

    #[test]
    fn add_multiple_prompts() {
        let dir = example_dir();
        let d = dir.path();
        // A string field, continue, then an ID field, then exit
        ok_stdin(
            d,
            &["modify", "add-multiple", "data.csv"],
            "1\ndescription\nA file\nn\n2\nisPartOf\n#set\nY\n",
        );
        let data = entity(d, "data.csv");
        assert_eq!(data["description"], "A file");
        assert_eq!(data["isPartOf"], json!({"@id": "#set"}));

        // A list of IDs
        ok_stdin(
            d,
            &["modify", "add-multiple", "data.csv"],
            "3\nmentions\n#a\nY\n#b\nN\nY\n",
        );
        assert_eq!(
            entity(d, "data.csv")["mentions"],
            json!([{"@id": "#a"}, {"@id": "#b"}])
        );

        // An integer, a float and a boolean
        ok_stdin(
            d,
            &["modify", "add-multiple", "data.csv"],
            "4\ncontentSize\n42\nn\n5\nratio\n0.5\nn\n6\nflag\ntrue\nY\n",
        );
        let data = entity(d, "data.csv");
        assert_eq!(data["contentSize"], 42);
        assert_eq!(data["ratio"], 0.5);
        assert_eq!(data["flag"], true);
    }
}

mod context {
    use super::*;

    #[test]
    fn inspect() {
        let dir = example_dir();
        let d = dir.path();
        assert_eq!(
            ok(d, &["context", "show"]).trim(),
            "\"https://w3id.org/ro/crate/1.3/context\""
        );
        assert_eq!(
            lines(&ok(d, &["context", "list"])),
            ["https://w3id.org/ro/crate/1.3/context"]
        );
        assert_eq!(ok(d, &["context", "version"]).trim(), "1.3");
        err(d, &["context", "urn"]);
        err(d, &["context", "get", "@base"]);
    }

    #[test]
    fn add_replace_remove() {
        let dir = example_dir();
        let d = dir.path();
        ok(
            d,
            &["context", "add", r#"{"@base": "https://example.org/a/"}"#],
        );
        assert_eq!(
            ok(d, &["context", "get", "@base"]).trim(),
            "https://example.org/a/"
        );
        assert_eq!(lines(&ok(d, &["context", "keys"])), ["@base"]);

        ok(
            d,
            &[
                "context",
                "replace",
                r#"{"@base": "https://example.org/a/"}"#,
                r#"{"@base": "https://example.org/b/"}"#,
            ],
        );
        assert_eq!(
            ok(d, &["context", "get", "@base"]).trim(),
            "https://example.org/b/"
        );

        ok(
            d,
            &[
                "context",
                "remove",
                r#"{"@base": "https://example.org/b/"}"#,
            ],
        );
        err(d, &["context", "get", "@base"]);
        err(d, &["context", "remove", "https://example.org/not-there"]);

        ok(d, &["context", "add", "https://example.org/terms"]);
        assert!(lines(&ok(d, &["context", "list"])).contains(&"https://example.org/terms"));
    }

    #[test]
    fn urn_with_non_urn_base() {
        let dir = example_dir();
        let d = dir.path();
        ok(
            d,
            &["context", "add", r#"{"@base": "https://example.org/"}"#],
        );
        handled_err(d, &["context", "urn"]);

        // add-urn replaces the existing @base
        let urn = ok(d, &["context", "add-urn"]);
        assert_eq!(ok(d, &["context", "urn"]), urn);
    }

    #[test]
    fn add_urn_without_embedded_terms() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["context", "add", "https://example.org/terms"]);
        let urn = ok(d, &["context", "add-urn"]);
        assert_eq!(ok(d, &["context", "urn"]), urn);
        assert_eq!(ok(d, &["context", "add-urn"]), urn);
    }

    #[test]
    fn add_urn_needs_room_for_a_base() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        ok(
            d,
            &[
                "init",
                "--embedded",
                "--context-item",
                r#"{"name": "http://schema.org/name"}"#,
            ],
        );
        handled_err(d, &["context", "add-urn"]);
    }

    #[test]
    fn list_and_keys_json() {
        let dir = example_dir();
        let d = dir.path();
        ok(
            d,
            &["context", "add", r#"{"@base": "https://example.org/a/"}"#],
        );
        let list: Value = serde_json::from_str(&ok(d, &["context", "list", "--json"])).unwrap();
        assert_eq!(
            list,
            json!([
                "https://w3id.org/ro/crate/1.3/context",
                "https://example.org/a/"
            ])
        );
        let keys: Value = serde_json::from_str(&ok(d, &["context", "keys", "--json"])).unwrap();
        assert_eq!(keys, json!(["@base"]));
    }

    #[test]
    fn urn() {
        let dir = example_dir();
        let d = dir.path();
        let urn = ok(d, &["context", "add-urn"]);
        assert_eq!(ok(d, &["context", "urn"]), urn);
        assert_eq!(ok(d, &["context", "add-urn"]), urn);
        assert_eq!(
            ok(d, &["context", "get", "@base"]).trim(),
            format!("urn:uuid:{}", urn.trim())
        );
    }
}

mod validate {
    use super::*;

    #[test]
    fn valid_and_invalid() {
        let dir = example_dir();
        let d = dir.path();
        assert_eq!(ok(d, &["validate", "basic"]).trim(), "Crate Valid");

        ok(d, &["modify", "add-field", "data.csv", "notAKey", "x"]);
        let output = run(d, &["validate", "basic", "--json"], None);
        assert!(!output.status.success());
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["is_valid"], false);
        assert_eq!(report["invalid_keys"], json!(["notAKey"]));
        assert_eq!(report["error_type"], Value::Null);
    }

    #[test]
    fn read_errors_are_reported() {
        let dir = tempfile::tempdir().unwrap();
        let output = run(dir.path(), &["validate", "basic", "--json"], None);
        assert!(!output.status.success());
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["error_type"], "IoError");

        fs::write(dir.path().join("ro-crate-metadata.json"), "{not json").unwrap();
        let output = run(dir.path(), &["validate", "basic", "--json"], None);
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["error_type"], "JsonError");
        assert!(ok_or_fail_text(dir.path(), &["validate", "basic"]).contains("JsonError"));
    }

    #[test]
    fn schema_errors_are_reported() {
        let dir = tempfile::tempdir().unwrap();
        let mut crate_ = crate_json();
        crate_["@context"] = json!("https://example.org/not-ro-crate");
        write_json(&dir.path().join("ro-crate-metadata.json"), &crate_);
        let output = run(dir.path(), &["validate", "basic", "--json"], None);
        let report: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["error_type"], "SchemaError");
    }

    #[test]
    fn stdin_target() {
        let dir = tempfile::tempdir().unwrap();
        let out = ok_stdin(
            dir.path(),
            &["validate", "basic", "-t", "-"],
            &crate_json().to_string(),
        );
        assert_eq!(out.trim(), "Crate Valid");
    }
}

#[test]
fn schema() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let summary = ok(d, &["schema"]);
    assert!(summary.contains("@id: https://w3id.org/ro/crate/1.3/context"));

    let terms = ok(d, &["schema", "1.1", "--terms"]);
    assert!(lines(&terms).contains(&"name\thttp://schema.org/name"));

    let full: Value = serde_json::from_str(&ok(d, &["schema", "1.2", "--json"])).unwrap();
    assert_eq!(full["@id"], "https://w3id.org/ro/crate/1.2/context");
    err(d, &["schema", "9.9"]);
}

mod subcrates {
    use super::*;

    /// The crate references sub/, which references sub/nested/, and missing/
    fn dir_with_subcrates() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        let nested = sub.join("nested");
        fs::create_dir_all(&nested).unwrap();
        write_json(&nested.join("ro-crate-metadata.json"), &crate_json());

        let nested_id = format!("{}/", nested.display());
        write_json(
            &sub.join("ro-crate-metadata.json"),
            &crate_with_subcrates(&[&nested_id]),
        );

        let sub_id = format!("{}/", sub.display());
        write_json(
            &dir.path().join("ro-crate-metadata.json"),
            &crate_with_subcrates(&[&sub_id, "missing/"]),
        );
        (dir, sub_id)
    }

    #[test]
    fn list() {
        let (dir, sub_id) = dir_with_subcrates();
        assert_eq!(
            lines(&ok(dir.path(), &["subcrates", "list"])),
            [sub_id.as_str(), "missing/"]
        );
    }

    #[cfg(feature = "subcrate-resolution")]
    #[test]
    fn fetch() {
        let (dir, _) = dir_with_subcrates();
        let d = dir.path();
        err(d, &["subcrates", "fetch"]);

        let fetched: Value =
            serde_json::from_str(&ok(d, &["subcrates", "fetch", "--lenient"])).unwrap();
        assert_eq!(fetched.as_array().unwrap().len(), 1);
        // The subcrate holds the example entities plus its reference to nested/
        assert_eq!(fetched[0]["@graph"].as_array().unwrap().len(), 5);

        let recursive: Value =
            serde_json::from_str(&ok(d, &["subcrates", "fetch", "-r", "--lenient"])).unwrap();
        assert_eq!(recursive.as_array().unwrap().len(), 2);
    }

    #[cfg(feature = "subcrate-resolution")]
    #[test]
    fn fetch_remote() {
        let base = serve(vec![
            (
                "/sub.json",
                crate_with_subcrates(&["{base}/nested.json"]).to_string(),
            ),
            ("/nested.json", crate_json().to_string()),
        ]);
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        let sub = format!("{}/sub.json", base);
        let missing = format!("{}/missing.json", base);
        write_json(
            &d.join("ro-crate-metadata.json"),
            &crate_with_subcrates(&[&sub, &missing]),
        );

        err(d, &["subcrates", "fetch"]);
        let fetched: Value =
            serde_json::from_str(&ok(d, &["subcrates", "fetch", "--lenient"])).unwrap();
        assert_eq!(fetched.as_array().unwrap().len(), 1);
        let recursive: Value =
            serde_json::from_str(&ok(d, &["subcrates", "fetch", "-r", "--lenient"])).unwrap();
        assert_eq!(recursive.as_array().unwrap().len(), 2);
    }

    #[test]
    fn list_json() {
        let (dir, sub_id) = dir_with_subcrates();
        let listed: Value =
            serde_json::from_str(&ok(dir.path(), &["subcrates", "list", "--json"])).unwrap();
        let ids: Vec<&str> = listed
            .as_array()
            .unwrap()
            .iter()
            .map(|entity| entity["@id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, [sub_id.as_str(), "missing/"]);
    }
}

#[cfg(feature = "rdf")]
mod rdf {
    use super::*;

    const BASE: &str = "https://example.org/crate/";

    fn sorted_lines(text: &str) -> Vec<&str> {
        let mut lines = lines(text);
        lines.sort();
        lines
    }

    #[test]
    fn export_formats() {
        let dir = example_dir();
        let d = dir.path();
        let turtle = ok(d, &["rdf", "export", "--base", BASE]);
        assert!(turtle.contains("<https://example.org/crate/data.csv>"));

        let ntriples = ok(d, &["rdf", "export", "--base", BASE, "-f", "nt"]);
        assert!(lines(&ntriples).iter().all(|line| line.ends_with(" .")));

        ok(d, &["rdf", "export", "--base", BASE, "-o", "crate.nt"]);
        let written = fs::read_to_string(d.join("crate.nt")).unwrap();
        assert_eq!(sorted_lines(&written), sorted_lines(&ntriples));

        assert!(ok(d, &["rdf", "export", "--base", BASE, "-f", "rdfxml"]).contains("rdf:RDF"));
        assert!(ok(d, &["rdf", "export", "--allow-relative", "-f", "nt"]).contains("data.csv"));
    }

    #[test]
    fn export_errors() {
        let dir = example_dir();
        let d = dir.path();
        err(d, &["rdf", "export"]);
        err(d, &["rdf", "export", "--base", BASE, "--allow-relative"]);
        ok(d, &["context", "add-urn"]);
        let stderr = handled_err(d, &["rdf", "export"]);
        assert!(stderr.contains("non-hierarchical"), "{stderr}");
        handled_err(d, &["rdf", "roundtrip"]);
    }

    #[test]
    fn opaque_base_option_is_rejected() {
        let dir = example_dir();
        let stderr = handled_err(dir.path(), &["rdf", "export", "--base", "urn:uuid:1234"]);
        assert!(stderr.contains("non-hierarchical"), "{stderr}");
    }

    #[test]
    fn expand_with_urn_base() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["context", "add-urn"]);
        assert_eq!(
            ok(d, &["rdf", "expand", "name"]).trim(),
            "http://schema.org/name"
        );
        assert_eq!(ok(d, &["rdf", "expand", "data.csv"]).trim(), "data.csv");
        handled_err(d, &["rdf", "expand", "data.csv", "--strict"]);
    }

    #[test]
    fn import_and_roundtrip() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["rdf", "export", "--base", BASE, "-o", "crate.ttl"]);

        ok(d, &["rdf", "import", "crate.ttl", "-o", "imported.json"]);
        let ids = lines(&ok(d, &["read", "ids", "-t", "imported.json"])).len();
        assert_eq!(ids, 4);

        let turtle = fs::read_to_string(d.join("crate.ttl")).unwrap();
        let stdout = ok_stdin(d, &["rdf", "import", "-", "-o", "-"], &turtle);
        let imported: Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(imported["@graph"].as_array().unwrap().len(), 4);

        let roundtrip: Value =
            serde_json::from_str(&ok(d, &["rdf", "roundtrip", "--base", BASE])).unwrap();
        assert_eq!(roundtrip["@graph"].as_array().unwrap().len(), 4);

        err(d, &["rdf", "import", "missing.ttl"]);
    }

    #[test]
    fn expand_and_compact() {
        let dir = example_dir();
        let d = dir.path();
        assert_eq!(
            ok(d, &["rdf", "expand", "name"]).trim(),
            "http://schema.org/name"
        );
        assert_eq!(
            ok(d, &["rdf", "compact", "http://schema.org/name"]).trim(),
            "name"
        );
        assert_eq!(ok(d, &["rdf", "expand", "data.csv"]).trim(), "data.csv");
        err(d, &["rdf", "expand", "data.csv", "--strict"]);
    }

    #[test]
    fn preloaded_context() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["context", "add", "https://example.org/terms"]);
        write_json(
            &d.join("terms.json"),
            &json!({"@context": {"foo": "https://example.org/terms#foo"}}),
        );

        err(d, &["rdf", "expand", "foo"]);
        let context = "https://example.org/terms=terms.json";
        assert_eq!(
            ok(d, &["rdf", "expand", "foo", "--context", context]).trim(),
            "https://example.org/terms#foo"
        );
        assert_eq!(
            ok(
                d,
                &[
                    "rdf",
                    "compact",
                    "https://example.org/terms#foo",
                    "--context",
                    context
                ]
            )
            .trim(),
            "foo"
        );
        err(d, &["rdf", "expand", "foo", "--context", "no-equals-sign"]);
        err(
            d,
            &[
                "rdf",
                "expand",
                "foo",
                "--context",
                "https://example.org/terms=missing.json",
            ],
        );
    }

    #[test]
    fn remote_context() {
        let base = serve(vec![(
            "/terms",
            json!({"@context": {"foo": "https://example.org/terms#foo"}}).to_string(),
        )]);
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["context", "add", &format!("{}/terms", base)]);
        ok(d, &["modify", "add-field", "data.csv", "foo", "bar"]);

        err(d, &["rdf", "expand", "foo"]);
        assert_eq!(
            ok(d, &["rdf", "expand", "foo", "--allow-remote"]).trim(),
            "https://example.org/terms#foo"
        );
        err(d, &["rdf", "export", "--base", BASE]);
        let exported = ok(
            d,
            &[
                "rdf",
                "export",
                "--base",
                BASE,
                "--allow-remote",
                "-f",
                "nt",
            ],
        );
        assert!(exported.contains("<https://example.org/terms#foo> \"bar\""));
    }

    #[test]
    fn formats_from_file_extensions() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["rdf", "export", "--base", BASE, "-o", "crate.nq"]);
        let nquads = fs::read_to_string(d.join("crate.nq")).unwrap();
        assert!(lines(&nquads).iter().all(|line| line.ends_with(" .")));

        ok(d, &["rdf", "export", "--base", BASE, "-o", "crate.rdf"]);
        assert!(fs::read_to_string(d.join("crate.rdf"))
            .unwrap()
            .contains("rdf:RDF"));

        for (input, output) in [("crate.rdf", "from-rdf.json"), ("crate.nq", "from-nq.json")] {
            ok(d, &["rdf", "import", input, "-o", output]);
            assert_eq!(lines(&ok(d, &["read", "ids", "-t", output])).len(), 4);
        }

        // An explicit format overrides the extension
        ok(
            d,
            &[
                "rdf",
                "export",
                "--base",
                BASE,
                "-f",
                "turtle",
                "-o",
                "turtle.nt",
            ],
        );
        ok(
            d,
            &[
                "rdf",
                "import",
                "turtle.nt",
                "-f",
                "ttl",
                "-o",
                "from-turtle.json",
            ],
        );
        err(
            d,
            &["rdf", "import", "turtle.nt", "-o", "wrong-format.json"],
        );
    }

    #[test]
    fn roundtrip_to_file() {
        let dir = example_dir();
        let d = dir.path();
        ok(
            d,
            &["rdf", "roundtrip", "--base", BASE, "-o", "roundtrip.json"],
        );
        assert_eq!(
            lines(&ok(d, &["read", "ids", "-t", "roundtrip.json"])).len(),
            4
        );
        err(d, &["rdf", "roundtrip"]);
    }
}

mod package {
    use super::*;

    #[test]
    fn zip_defaults_to_current_directory() {
        let parent = tempfile::tempdir().unwrap();
        let d = parent.path().join("mycrate");
        fs::create_dir(&d).unwrap();
        write_json(&d.join("ro-crate-metadata.json"), &crate_json());
        fs::write(d.join("data.csv"), "a,b\n").unwrap();

        ok(&d, &["package", "zip"]);
        assert!(d.join("mycrate.zip").exists());
        let ids = lines(&ok(&d, &["read", "ids", "-t", "mycrate.zip"])).len();
        assert_eq!(ids, 4);
    }

    /// A crate directory named `mycrate` with a data file in a subfolder
    fn crate_dir_with_subfolder(parent: &Path) -> std::path::PathBuf {
        let d = parent.join("mycrate");
        fs::create_dir_all(d.join("sub")).unwrap();
        fs::write(d.join("sub").join("data.csv"), "a,b\n").unwrap();
        let mut crate_ = crate_json();
        crate_["@graph"][2]["@id"] = json!("sub/data.csv");
        write_json(&d.join("ro-crate-metadata.json"), &crate_);
        d
    }

    #[test]
    fn zip_keeps_or_flattens_folders() {
        let parent = tempfile::tempdir().unwrap();
        let d = crate_dir_with_subfolder(parent.path());
        ok(&d, &["package", "zip"]);
        let ids = ok(&d, &["read", "ids", "-t", "mycrate.zip"]);
        assert!(lines(&ids).contains(&"sub/data.csv"));

        let parent = tempfile::tempdir().unwrap();
        let d = crate_dir_with_subfolder(parent.path());
        ok(&d, &["package", "zip", "-f"]);
        let ids = ok(&d, &["read", "ids", "-t", "mycrate.zip"]);
        assert!(lines(&ids).contains(&"data.csv"));
    }

    #[test]
    fn zip_unique_name() {
        let dir = example_dir();
        let d = dir.path();
        let urn = ok(d, &["context", "add-urn"]);
        ok(d, &["package", "zip", "-u"]);
        assert!(d.join(format!("{}.zip", urn.trim())).exists());
    }

    #[test]
    fn zip_validation_level() {
        let dir = example_dir();
        let d = dir.path();
        ok(d, &["modify", "add-field", "data.csv", "notAKey", "x"]);
        err(d, &["package", "zip", "-l", "2"]);
        ok(d, &["package", "zip", "-l", "1"]);
    }

    #[test]
    fn zip_external_is_opt_in() {
        let parent = tempfile::tempdir().unwrap();
        let d = parent.path().join("mycrate");
        fs::create_dir(&d).unwrap();
        fs::write(parent.path().join("outside.txt"), "x").unwrap();
        let mut crate_ = crate_json();
        crate_["@graph"][2]["@id"] = json!("../outside.txt");
        write_json(&d.join("ro-crate-metadata.json"), &crate_);

        ok(&d, &["package", "zip"]);
        let ids = ok(&d, &["read", "ids", "-t", "mycrate.zip"]);
        assert!(lines(&ids).contains(&"../outside.txt"));

        ok(&d, &["package", "zip", "-e"]);
        let ids = ok(&d, &["read", "ids", "-t", "mycrate.zip"]);
        assert!(lines(&ids).contains(&"external/outside.txt"));

        ok(&d, &["package", "zip", "--external", "false"]);
        err(&d, &["package", "zip", "-t", "missing"]);
    }

    #[test]
    fn zip_include_hidden() {
        let parent = tempfile::tempdir().unwrap();
        let d = parent.path().join("mycrate");
        fs::create_dir_all(d.join(".config")).unwrap();
        write_json(&d.join("ro-crate-metadata.json"), &crate_json());
        fs::write(d.join(".config").join("settings.toml"), "x").unwrap();
        fs::write(d.join(".zenodo.json"), "{}").unwrap();
        fs::write(d.join(".env"), "SECRET=1").unwrap();

        let args = ["package", "zip", "-i", ".zenodo.json", "--include-hidden", ".config"];
        ok(&d, &args);
        let archive = zip::ZipArchive::new(fs::File::open(d.join("mycrate.zip")).unwrap()).unwrap();
        let names: Vec<&str> = archive.file_names().collect();
        assert!(names.contains(&".zenodo.json"));
        assert!(names.contains(&".config/settings.toml"));
        assert!(!names.contains(&".env"));

        err(&d, &["package", "zip", "-i", ".missing"]);
    }
}

#[cfg(feature = "parquet")]
#[test]
fn convert_tabular() {
    let dir = example_dir();
    let d = dir.path();
    err(d, &["convert", "csv", "-o", "crate.csv"]);

    ok(d, &["context", "add-urn"]);
    ok(d, &["convert", "csv", "-o", "crate.csv"]);
    ok(d, &["convert", "parquet", "-o", "crate.parquet"]);
    let csv = fs::read_to_string(d.join("crate.csv")).unwrap();
    assert_eq!(lines(&csv)[0], "uuid,id,key,value");
    assert!(fs::metadata(d.join("crate.parquet")).unwrap().len() > 0);
}
