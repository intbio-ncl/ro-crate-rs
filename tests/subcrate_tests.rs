#[cfg(test)]
pub mod subcrate_tests {
    use mockito::Matcher;
    use rocraters::ro_crate::rocrate::RoCrate;
    use rocraters::ro_crate::subcrate_resolution::{fetch_subcrates, fetch_subcrates_recursive};
    use serde_json::json;
    use sha1::Digest;
    use std::io::Write;
    use tempfile::tempdir;
    use zip::write::SimpleFileOptions;
    use zip::ZipWriter;

    #[test]
    fn test_try_resolve_local_files() {
        let subdir1 = tempdir().unwrap();
        let subdir2 = tempdir().unwrap();
        let subdir3 = tempdir().unwrap();

        let subcrate1 = json!(
        {
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.2"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": "Dataset",
              "name": "Root Research Crate with Multiple Subcrates",
              "description": "Subcrate 1 Test",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "hasPart": [
                {"@id": "subcrate1/data.csv"},
                {"@id": "subcrate1/README.md"}
                    ]
            },
            {
              "@id": "data.csv",
              "@type": "File",
              "name": "Sample Data",
              "encodingFormat": "text/csv"
            },
            {
              "@id": "README.md",
              "@type": "File",
              "name": "Subcrate Documentation",
              "encodingFormat": "text/markdown"
            }
            ]
        });

        let subcrate1_path = subdir1.path().join("ro-crate-metadata.json");
        let mut tmpfile = std::fs::File::create(subcrate1_path.clone()).unwrap();
        tmpfile.write(subcrate1.to_string().as_bytes()).unwrap();

        let subcrate2 = json!(
        {
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.2"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": "Dataset",
              "name": "Subcrate 2: With Explicit RO-Crate Metadata Reference",
              "description": "Subcrate 2",
              "subjectOf": "subcrate2/ro-crate-metadata.json",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "hasPart": [
                {"@id": "subcrate2/ro-crate-metadata.json"},
                {"@id": "subcrate2/analysis.py"}
              ]
            },
            {
              "@id": "subcrate2/ro-crate-metadata.json",
              "@type": "CreativeWork",
              "encodingFormat": "application/json+ld",
              "about": {
                "@id": "subcrate2/"
              },
              "name": "Subcrate 2 Metadata",
              "description": "Separate RO-Crate metadata for subcrate2"
            },
            {
              "@id": "subcrate2/analysis.py",
              "@type": ["File", "SoftwareSourceCode"],
              "name": "Analysis Script",
              "programmingLanguage": "Python",
              "encodingFormat": "text/x-python"
            }]
        });

        let subcrate2_path = subdir2.path().join("ro-crate-metadata.json");
        let mut tmpfile = std::fs::File::create(subcrate2_path.clone()).unwrap();
        tmpfile.write(subcrate2.to_string().as_bytes()).unwrap();

        let subcrate3 = json!(
        {
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.2"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": ["Dataset", "CreativeWork"],
              "name": "Subcrate 3: With Provenance Information",
              "description": "Subcrate 3 Test",
              "datePublished": "2026-01-06",
              "distribution": "subcrate3/ro-crate-metadata.json",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "author": {
                "@id": "https://orcid.org/0000-0002-1825-0097"
              },
              "dateCreated": "2025-12-15",
              "dateModified": "2026-01-05",
              "isPartOf": {
                "@id": "./"
              },
              "hasPart": [
                {"@id": "subcrate3/experiment_results.json"},
                {"@id": "subcrate3/ro-crate-metadata.json"}
              ]
            },
            {
              "@id": "https://orcid.org/0000-0002-1825-0097",
              "@type": "Person",
              "name": "Jane Researcher"
            },
            {
              "@id": "subcrate3/experiment_results.json",
              "@type": "File",
              "name": "Experiment Results",
              "encodingFormat": "application/json"
            },
            {
              "@id": "subcrate3/ro-crate-metadata.json",
              "@type": "CreativeWork",
              "encodingFormat": "application/json+ld",
              "about": {
                "@id": "subcrate3/"
              },
              "name": "Subcrate 3 Metadata",
              "description": "Separate RO-Crate metadata for subcrate3"
            }
            ]}
        );

        let subcrate3_path = subdir3.path().join("ro-crate-metadata.json");
        let mut tmpfile = std::fs::File::create(subcrate3_path.clone()).unwrap();
        tmpfile.write(subcrate3.to_string().as_bytes()).unwrap();

        let base_crate = json!({
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.1"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": "Dataset",
              "name": "Root Research Crate with Multiple Subcrates",
              "description": "A comprehensive example demonstrating various ways to define subcrates within an RO-Crate",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "hasPart": [
                {"@id": "subcrate1/"},
                {"@id": "subcrate2/"},
                {"@id": "subcrate3/"},
              ]
            },
            // First case:
            // Local directory without subjectOf and distribution
            {
              "@id": subcrate1_path.to_string_lossy(),
              "@type": "Dataset",
              "name": "Subcrate 1: Basic Directory Reference",
              "description": "Simplest form - just a directory marked as Dataset with hasPart listing its contents",
              "conformsTo": { "@id": "https://w3id.org/ro/crate" },
              "hasPart": [
                {"@id": "subcrate1/data.csv"},
                {"@id": "subcrate1/README.md"},
                {"@id": "subcrate1/ro-crate-metadata.json"}
              ]
            },
            {
              "@id": "subcrate1/data.csv",
              "@type": "File",
              "name": "Sample Data",
              "encodingFormat": "text/csv"
            },
            {
              "@id": "subcrate1/README.md",
              "@type": "File",
              "name": "Subcrate Documentation",
              "encodingFormat": "text/markdown"
            },
            {
              "@id": "subcrate1/ro-crate-metadata.json",
              "@type": "File",
              "name": "RO-Crate metadata file",
              "encodingFormat": "application/json+ld"
            },
            // Second case:
            // Local directory with subjectOf that defines the location of metadatafile
            {
              "@id": "subcrate2/",
              "@type": "Dataset",
              "name": "Subcrate 2: With Explicit RO-Crate Metadata Reference",
              "description": "Subcrate that references its own ro-crate-metadata.json file",
              "conformsTo": { "@id": "https://w3id.org/ro/crate" },
              "subjectOf": { "@id": subcrate2_path.to_string_lossy()},
              "hasPart": [
                {"@id": "subcrate2/ro-crate-metadata.json"},
                {"@id": "subcrate2/analysis.py"}
              ]
            },
            {
              "@id": "subcrate2/ro-crate-metadata.json",
              "@type": "CreativeWork",
              "encodingFormat": "application/json+ld",
              "about": {
                "@id": "subcrate2/"
              },
              "name": "Subcrate 2 Metadata",
              "description": "Separate RO-Crate metadata for subcrate2"
            },
            {
              "@id": "subcrate2/analysis.py",
              "@type": ["File", "SoftwareSourceCode"],
              "name": "Analysis Script",
              "programmingLanguage": "Python",
              "encodingFormat": "text/x-python"
            },
            // Third case:
            // Local directory with distribution that defines the location of metadatafile
            {
              "@id": "subcrate3/",
              "@type": ["Dataset", "CreativeWork"],
              "name": "Subcrate 3: With Provenance Information",
              "description": "Subcrate with detailed provenance and authorship",
              "conformsTo": { "@id": "https://w3id.org/ro/crate" },
              "distribution": { "@id": subcrate3_path.to_string_lossy() },
              "author": {
                "@id": "https://orcid.org/0000-0002-1825-0097"
              },
              "dateCreated": "2025-12-15",
              "dateModified": "2026-01-05",
              "isPartOf": {
                "@id": "./"
              },
              "hasPart": [
                {"@id": "subcrate3/experiment_results.json"},
                {"@id": "subcrate3/ro-crate-metadata.json"}
              ]
            },
            {
              "@id": "https://orcid.org/0000-0002-1825-0097",
              "@type": "Person",
              "name": "Jane Researcher"
            },
            {
              "@id": "subcrate3/experiment_results.json",
              "@type": "File",
              "name": "Experiment Results",
              "encodingFormat": "application/json"
            },
            {
              "@id": "subcrate3/ro-crate-metadata.json",
              "@type": "CreativeWork",
              "encodingFormat": "application/json+ld",
              "about": {
                "@id": "subcrate3/"
              },
              "name": "Subcrate 3 Metadata",
              "description": "Separate RO-Crate metadata for subcrate3"
            },
          ]
        });

        let root: RoCrate = serde_json::from_value(base_crate).unwrap();
        let subcrates = fetch_subcrates(&root).unwrap();

        assert_eq!(subcrates.len(), 3);

        // Because Vec and HashMap order is not neccessarily the same, this fails
        // assert_eq!(serde_json::to_string(&subcrate1).unwrap(), serde_json::to_string(&subcrates[0]).unwrap());
        // assert_eq!(serde_json::to_string(&subcrate2).unwrap(), serde_json::to_string(&subcrates[1]).unwrap());
        // assert_eq!(serde_json::to_string(&subcrate3).unwrap(), serde_json::to_string(&subcrates[2]).unwrap());
    }

    #[test]
    fn test_try_direct_delivery_subcrate() {
        let mut server = mockito::Server::new();

        let url = server.url();

        let remote_subcrate = json!({
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.2"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": "Dataset",
              "name": "Subcrate 1: With External Identifier and Publisher",
              "description": "Subcrate that has been published as a separate entity",
              "identifier": "https://doi.org/10.5281/zenodo.1234567",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "publisher": {
                "@id": "https://zenodo.org"
              },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },

              "hasPart": [
                {"@id": "README.md"}
              ]
            },
            {
              "@id": "https://zenodo.org",
              "@type": "Organization",
              "name": "Zenodo"
            },
            {
              "@id": "README.md",
              "@type": "File",
              "name": "Readme file"
            }
            ]
        });

        let _: RoCrate = serde_json::from_value(remote_subcrate.clone()).unwrap();

        // Direct delivery of ro-crate
        let mock = server
            .mock("GET", "/subcrate1")
            .with_header("Content-Type", "application/json+ld")
            .with_body(serde_json::to_string_pretty(&remote_subcrate).unwrap())
            .create();

        let base_crate = json!({
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.1"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": "Dataset",
              "name": "Root Research Crate with Multiple Subcrates",
              "description": "A comprehensive example demonstrating various ways to define subcrates within an RO-Crate",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "hasPart": [
                {"@id": "subcrate1/"}
              ]
            },
            // First case:
            // Direct delivery of ro-crate
            {
              "@id": "https://doi.org/10.5281/zenodo.1234567",
              "@type": "Dataset",
              "name": "Subcrate 1: Direct delivery of ro-crate",
              "description": "Subcrate that has been published as a separate entity",
              "identifier": "https://doi.org/10.5281/zenodo.1234567",
              "subjectOf": { "@id": format!("{}/subcrate1", url) },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },
            }
          ]
        });

        let root: RoCrate = serde_json::from_value(base_crate).unwrap();

        let subcrates = fetch_subcrates(&root).unwrap();
        assert_eq!(subcrates.len(), 1);

        mock.assert();
    }

    #[test]
    fn test_signposting_subcrates() {
        let mut server = mockito::Server::new();
        let url = server.url();

        let mut remotes = Vec::new();

        for n in 1..4 {
            let remote_subcrate = json!({
              "@context": "https://w3id.org/ro/crate/1.2/context",
              "@graph": [
                {
                  "@id": "ro-crate-metadata.json",
                  "@type": "CreativeWork",
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate/1.2"
                  },
                  "about": {
                    "@id": "./"
                  }
                },
                {
                  "@id": "./",
                  "@type": "Dataset",
                  "name": format!("Subcrate {n}: With External Identifier and Publisher"),
                  "description": "Subcrate that has been published as a separate entity",
                  "identifier": "https://doi.org/10.5281/zenodo.1234567",
                  "datePublished": "2026-01-06",
                  "license": "https://creativecommons.org/licenses/by/4.0/",
                  "publisher": {
                    "@id": "https://zenodo.org"
                  },
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate"
                  },

                  "hasPart": [
                    {"@id": "README.md"}
                  ]
                },
                {
                  "@id": "https://zenodo.org",
                  "@type": "Organization",
                  "name": "Zenodo"
                },
                {
                  "@id": "README.md",
                  "@type": "File",
                  "name": "Readme file"
                }
                ]
            });
            remotes.push(remote_subcrate);
        }

        let mut mocks = Vec::new();

        // Signposting
        let mock1a = server
            .mock("GET", "/subcrate1")
            .with_header(
                "Link",
                format!("{}/subcrate1-link; rel=\"describedBy\"", url).as_str(),
            )
            .create();
        mocks.push(mock1a);

        let mock1b = server
            .mock("GET", "/subcrate1-link")
            .with_header("Content-Type", "application/json+ld")
            .with_body(remotes.get(1).unwrap().to_string())
            .create();
        mocks.push(mock1b);

        let mock2a = server
            .mock("GET", "/subcrate2")
            .with_header(
                "Link",
                format!("<{}/subcrate2-link>; rel=\"item\"", url).as_str(),
            )
            .create();
        mocks.push(mock2a);

        let mock2b = server
            .mock("GET", "/subcrate2-link")
            .with_header("Content-Type", "application/json+ld")
            .with_body(remotes.get(1).unwrap().to_string())
            .create();
        mocks.push(mock2b);

        let mock3a = server
            .mock("GET", "/subcrate3")
            .with_header(
                "Link",
                format!("<{}/subcrate3-link>; rel=\"item\"; rel=\"describedBy\"; profile=\"https://w3id.org/ro/crate\"", url).as_str(),
            )
            .create();
        mocks.push(mock3a);

        let mock3b = server
            .mock("GET", "/subcrate3-link")
            .with_header("Content-Type", "application/json+ld")
            .with_body(remotes.get(1).unwrap().to_string())
            .create();
        mocks.push(mock3b);

        let base_crate = json!({
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.1"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": "Dataset",
              "name": "Root Research Crate with Multiple Subcrates",
              "description": "A comprehensive example demonstrating various ways to define subcrates within an RO-Crate",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "hasPart": [
                {"@id": "subcrate1/"},
                {"@id": "subcrate2/"},
                {"@id": "subcrate3/"},
              ]
            },
            // First case:
            // Signposting with `rel=describedBy`
            {
              "@id": "subcrate1/",
              "@type": "Dataset",
              "name": "Subcrate 1: Signposting with describedBy",
              "description": "Subcrate that has been published as a separate entity",
              "identifier": "https://doi.org/10.5281/zenodo.1234567",
              "subjectOf": { "@id": format!("{}/subcrate1", url) },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },
            },
            // Second case:
            // Signposting with `rel=item`
            {
              "@id": "subcrate2/",
              "@type": "Dataset",
              "name": "Subcrate 2: Signposting with item",
              "description": "Subcrate that has been published as a separate entity",
              "identifier": "https://doi.org/10.5281/zenodo.1234567",
              "subjectOf": { "@id": format!("{}/subcrate2", url) },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },
            },
            // Third case:
            // Signposting with `rel=item` and `rel=describedBy` and prefer
            // `profile="https://w3id.org/ro/crate"`
            {
              "@id": "subcrate3/",
              "@type": "Dataset",
              "name": "Subcrate 3: Signposting with profile",
              "description": "Subcrate that has been published as a separate entity",
              "distribution": { "@id": format!("{}/subcrate3", url) },
              "publisher": {
                "@id": "https://zenodo.org"
              },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },
            },
          ]
        });

        let root: RoCrate = serde_json::from_value(base_crate).unwrap();
        let subcrates = fetch_subcrates(&root).unwrap();

        assert_eq!(subcrates.len(), 3);

        for m in mocks {
            m.assert();
        }
    }

    #[test]
    fn test_content_negotiation_subcrates() {
        let mut server = mockito::Server::new();
        let url = server.url();

        let remote_subcrate = json!({
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.2"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": "Dataset",
              "name": "Subcrate 1: With External Identifier and Publisher",
              "description": "Subcrate that has been published as a separate entity",
              "identifier": "https://doi.org/10.5281/zenodo.1234567",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "publisher": {
                "@id": "https://zenodo.org"
              },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },

              "hasPart": [
                {"@id": "README.md"}
              ]
            },
            {
              "@id": "https://zenodo.org",
              "@type": "Organization",
              "name": "Zenodo"
            },
            {
              "@id": "README.md",
              "@type": "File",
              "name": "Readme file"
            }
            ]
        });

        let mut mocks = Vec::new();

        // Content negotiation
        let mock1a = server
            .mock("GET", "/subcrate1")
            .match_header("Accept", Matcher::Regex(r"application/json.*".to_string()))
            .with_header("Content-Type", "application/json+ld")
            .with_body(serde_json::to_string(&remote_subcrate).unwrap())
            .create();
        mocks.push(mock1a);

        let mock2a = server
            .mock("GET", "/subcrate2")
            .match_header("Accept", Matcher::Regex(r"application/json.*".to_string()))
            .with_status(300)
            .create();
        mocks.push(mock2a);

        let mock2b = server
            .mock("GET", "/subcrate2")
            .match_header("Accept", Matcher::Regex(r"application/zip.*".to_string()))
            .with_header("Content-Type", "application/zip")
            .with_body_from_file("tests/fixtures/zip_test/fixtures.zip")
            .create();
        mocks.push(mock2b);

        let base_crate = json!({
            "@context": "https://w3id.org/ro/crate/1.2/context",
            "@graph": [
              {
                "@id": "ro-crate-metadata.json",
                "@type": "CreativeWork",
                "conformsTo": {
                  "@id": "https://w3id.org/ro/crate/1.2"
                },
                "about": {
                  "@id": "./"
                }
              },
              {
                "@id": "./",
                "@type": "Dataset",
                "name": "Root ro crate for testing subcrates",
                "description": "Subcrate that has been published as a separate entity",
                "identifier": "https://doi.org/10.5281/zenodo.1234567",
                "datePublished": "2026-01-06",
                "license": "https://creativecommons.org/licenses/by/4.0/",
                "publisher": {
                  "@id": "https://zenodo.org"
                },
                "conformsTo": {
                  "@id": "https://w3id.org/ro/crate"
                },

                "hasPart": [
                  {"@id": "README.md"}
                ]
              },
              // First case:
              // Server-side content negotiation
              {
                "@id": "https://doi.org/10.5281/zenodo.1234567",
                "@type": "Dataset",
                "name": "Subcrate 1: With server-side content negotiation",
                "description": "Subcrate that has been published as a separate entity",
                "distribution": { "@id": format!("{url}/subcrate1") },
                "publisher": {
                  "@id": "https://zenodo.org"
                },
                "conformsTo": {
                  "@id": "https://w3id.org/ro/crate"
                },
              },
              // Second case:
              // Client-side content negotiation
              {
                "@id": "https://doi.org/10.5281/zenodo.1234567",
                "@type": "Dataset",
                "name": "Subcrate 2: With client-side content negotiation",
                "description": "Subcrate that has been published as a separate entity",
                "distribution": { "@id": format!("{url}/subcrate2") },
                "publisher": {
                  "@id": "https://zenodo.org"
                },
                "conformsTo": {
                  "@id": "https://w3id.org/ro/crate"
                },
              },
            ]
        });

        let root: RoCrate = serde_json::from_value(base_crate).unwrap();
        let subcrates = fetch_subcrates(&root).unwrap();

        assert_eq!(subcrates.len(), 2);

        for m in mocks {
            m.assert();
        }
    }

    #[test]
    fn test_guessing_subcrates() {
        let mut server = mockito::Server::new();
        let url = server.url();

        let remote_subcrate = json!({
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.2"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": "Dataset",
              "name": "Subcrate 1: With External Identifier and Publisher",
              "description": "Subcrate that has been published as a separate entity",
              "identifier": "https://doi.org/10.5281/zenodo.1234567",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "publisher": {
                "@id": "https://zenodo.org"
              },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },

              "hasPart": [
                {"@id": "README.md"}
              ]
            },
            {
              "@id": "https://zenodo.org",
              "@type": "Organization",
              "name": "Zenodo"
            },
            {
              "@id": "README.md",
              "@type": "File",
              "name": "Readme file"
            }
            ]
        });

        let index_html = "<!DOCTYPE html>
<html>
<body>

<h1>RO-Crate website</h1>

</body>
</html>"
            .to_string();

        // Content negotiation
        let mut mocks = Vec::new();

        let mock = server
            .mock("GET", "/subcrate1")
            .with_header("Location", "/subcrate1/index.html")
            .with_status(308)
            .expect_at_least(1)
            .create();
        mocks.push(mock);
        let mock = server
            .mock("GET", "/subcrate1/index.html")
            .with_header("Content-Type", "text/html")
            .with_body(index_html)
            .expect_at_least(1)
            .create();
        mocks.push(mock);

        let mock = server
            .mock("GET", "/subcrate1/ro-crate-metadata.json")
            .with_header("Content-Type", "application/json+ld")
            .with_body(remote_subcrate.to_string())
            .create();
        mocks.push(mock);

        let base_crate = json!({
              "@context": "https://w3id.org/ro/crate/1.2/context",
              "@graph": [
                {
                  "@id": "ro-crate-metadata.json",
                  "@type": "CreativeWork",
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate/1.2"
                  },
                  "about": {
                    "@id": "./"
                  }
                },
                {
                  "@id": "./",
                  "@type": "Dataset",
                  "name": format!("Subcrate 1: With External Identifier and Publisher"),
                  "description": "Subcrate that has been published as a separate entity",
                  "identifier": "https://doi.org/10.5281/zenodo.1234567",
                  "datePublished": "2026-01-06",
                  "license": "https://creativecommons.org/licenses/by/4.0/",
                  "publisher": {
                    "@id": "https://zenodo.org"
                  },
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate"
                  },

                  "hasPart": [
                    {"@id": "README.md"}
                  ]
                },
                // Guess location
                {
                  "@id": "https://doi.org/10.5281/zenodo.1234567",
                  "@type": "Dataset",
                  "name": "Subcrate 6: Guess URL",
                  "description": "Subcrate that has been published as a separate entity",
                  "distribution": {"@id": format!("{url}/subcrate1")},
                  "publisher": {
                    "@id": "https://zenodo.org"
                  },
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate"
                  },
                }
            ]
        });

        let root: RoCrate = serde_json::from_value(base_crate).unwrap();
        let subcrates = fetch_subcrates(&root).unwrap();

        assert_eq!(subcrates.len(), 1);
        for mock in mocks {
            mock.assert();
        }
    }

    #[test]
    fn test_zip() {
        let mut server = mockito::Server::new();
        let url = server.url();

        let remote_subcrate = json!({
          "@context": "https://w3id.org/ro/crate/1.2/context",
          "@graph": [
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.2"
              },
              "about": {
                "@id": "./"
              }
            },
            {
              "@id": "./",
              "@type": "Dataset",
              "name": format!("Subcrate 1: With External Identifier and Publisher"),
              "description": "Subcrate that has been published as a separate entity",
              "identifier": "https://doi.org/10.5281/zenodo.1234567",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "publisher": {
                "@id": "https://zenodo.org"
              },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },

              "hasPart": [
                {"@id": "README.md"}
              ]
            },
            {
              "@id": "https://zenodo.org",
              "@type": "Organization",
              "name": "Zenodo"
            },
            {
              "@id": "README.md",
              "@type": "File",
              "name": "Readme file"
            }
            ]
        });

        let mut mocks = Vec::new();
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);

        // Create zip archive with ro-crate-metada.json file
        let buffer = Vec::new();
        let cursor = std::io::Cursor::new(buffer);
        let mut archive = ZipWriter::new(cursor);
        archive
            .start_file("ro-crate-metadata.json", options)
            .unwrap();
        archive
            .write_all(&serde_json::to_vec(&remote_subcrate).unwrap())
            .unwrap();
        let file = archive.finish().unwrap();

        let mock = server
            .mock("GET", "/subcrate1")
            .with_header("Content-Type", "application/zip")
            .with_body(file.get_ref())
            .create();
        mocks.push(mock);

        // Create zip archive with ro-crate-metada.json file
        // inside base_folder
        let buffer = Vec::new();
        let cursor = std::io::Cursor::new(buffer);
        let mut archive = ZipWriter::new(cursor);
        archive.add_directory("base_folder", options).unwrap();
        archive
            .start_file("ro-crate-metadata.json", options)
            .unwrap();
        archive
            .write_all(&serde_json::to_vec(&remote_subcrate).unwrap())
            .unwrap();
        let file = archive.finish().unwrap();

        let mock = server
            .mock("GET", "/subcrate2")
            .with_header("Content-Type", "application/zip")
            .with_body(file.get_ref())
            .create();
        mocks.push(mock);

        // Create zipped bagit archive with sha256 hashes
        let buffer = Vec::new();
        let cursor = std::io::Cursor::new(buffer);
        let mut archive = ZipWriter::new(cursor);
        archive.add_directory("data", options).unwrap();

        let crate_slice = serde_json::to_vec(&remote_subcrate).unwrap();
        let hash = format!(
            "{} ro-crate-metadata.json",
            hex::encode(sha2::Sha256::digest(&crate_slice))
        );
        archive.start_file("manifest-sha256.txt", options).unwrap();
        archive.write_all(hash.as_bytes()).unwrap();

        archive
            .start_file("ro-crate-metadata.json", options)
            .unwrap();
        archive.write_all(&crate_slice).unwrap();
        let file = archive.finish().unwrap();

        let mock = server
            .mock("GET", "/subcrate3")
            .with_header("Content-Type", "application/zip")
            .with_body(file.get_ref())
            .create();
        mocks.push(mock);

        // Create zipped bagit archive with sha256 hashes
        let buffer = Vec::new();
        let cursor = std::io::Cursor::new(buffer);
        let mut archive = ZipWriter::new(cursor);
        archive.add_directory("data", options).unwrap();

        let crate_slice = serde_json::to_vec(&remote_subcrate).unwrap();
        let hash = format!(
            "{} ro-crate-metadata.json",
            hex::encode(sha2::Sha512::digest(&crate_slice))
        );
        archive.start_file("manifest-sha512.txt", options).unwrap();
        archive.write_all(hash.as_bytes()).unwrap();

        archive
            .start_file("ro-crate-metadata.json", options)
            .unwrap();
        archive.write_all(&crate_slice).unwrap();
        let file = archive.finish().unwrap();

        let mock = server
            .mock("GET", "/subcrate4")
            .with_header("Content-Type", "application/zip")
            .with_body(file.get_ref())
            .create();
        mocks.push(mock);

        let base_crate = json!({
              "@context": "https://w3id.org/ro/crate/1.2/context",
              "@graph": [
                {
                  "@id": "ro-crate-metadata.json",
                  "@type": "CreativeWork",
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate/1.2"
                  },
                  "about": {
                    "@id": "./"
                  }
                },
                {
                  "@id": "./",
                  "@type": "Dataset",
                  "name": format!("Subcrate 1: With External Identifier and Publisher"),
                  "description": "Subcrate that has been published as a separate entity",
                  "identifier": "https://doi.org/10.5281/zenodo.1234567",
                  "datePublished": "2026-01-06",
                  "license": "https://creativecommons.org/licenses/by/4.0/",
                  "publisher": {
                    "@id": "https://zenodo.org"
                  },
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate"
                  },

                  "hasPart": [
                    {"@id": "README.md"}
                  ]
                },
                // First case:
                // Zip with ro-crate-metdata.json
                {
                  "@id": "https://doi.org/10.5281/zenodo.1234567",
                  "@type": "Dataset",
                  "name": "Subcrate 1: Zipped rocrate metadata file",
                  "description": "Subcrate that has been published as a separate entity",
                  "distribution": {"@id": format!("{url}/subcrate1")},
                  "publisher": {
                    "@id": "https://zenodo.org"
                  },
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate"
                  },
                },
                // Second case:
                // Zip with folder/ro-crate-metadata.json
                {
                  "@id": "https://doi.org/10.5281/zenodo.1234567",
                  "@type": "Dataset",
                  "name": "Subcrate 2: Zipped folder with ro-crate",
                  "description": "Subcrate that has been published as a separate entity",
                  "distribution": {"@id": format!("{url}/subcrate2")},
                  "publisher": {
                    "@id": "https://zenodo.org"
                  },
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate"
                  },
                },
                // Third case:
                // Zip+Bagit with SHA256
                {
                  "@id": "https://doi.org/10.5281/zenodo.1234567",
                  "@type": "Dataset",
                  "name": "Subcrate 3: Zipped bagit with ro-crate",
                  "description": "Subcrate that has been published as a separate entity",
                  "distribution": {"@id": format!("{url}/subcrate3")},
                  "publisher": {
                    "@id": "https://zenodo.org"
                  },
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate"
                  },
                },
                // Fourth case:
                // Zip+Bagit with SHA512
                {
                  "@id": "https://doi.org/10.5281/zenodo.1234567",
                  "@type": "Dataset",
                  "name": "Subcrate 4: Zipped bagit with ro-crate",
                  "description": "Subcrate that has been published as a separate entity",
                  "distribution": {"@id": format!("{url}/subcrate4")},
                  "publisher": {
                    "@id": "https://zenodo.org"
                  },
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate"
                  },
                }
            ]
        });

        let root: RoCrate = serde_json::from_value(base_crate).unwrap();

        let subcrates = fetch_subcrates(&root).unwrap();
        assert_eq!(subcrates.len(), 4);
        for m in mocks {
            m.assert();
        }
    }

    #[test]
    fn test_recursive() {
        let mut outer_graph = vec![
            json!(
            {
              "@id": "ro-crate-metadata.json",
              "@type": "CreativeWork",
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate/1.2"
              },
              "about": {
                "@id": "./"
              }
            }),
            json!({
              "@id": "./",
              "@type": "Dataset",
              "name": format!("Subcrate 1: With External Identifier and Publisher"),
              "description": "Subcrate that has been published as a separate entity",
              "identifier": "https://doi.org/10.5281/zenodo.1234567",
              "datePublished": "2026-01-06",
              "license": "https://creativecommons.org/licenses/by/4.0/",
              "publisher": {
                "@id": "https://zenodo.org"
              },
              "conformsTo": {
                "@id": "https://w3id.org/ro/crate"
              },

              "hasPart": [
                {"@id": "README.md"}
              ]
            }),
        ];

        let mut mocks = Vec::new();
        let mut server = mockito::Server::new();
        let url = server.url();

        for h in 1..3 {
            let path = format!("/subcrate{}", h);

            println!("{}", path);

            outer_graph.push(json!(
                {
                  "@id": "https://doi.org/10.5281/zenodo.1234567",
                  "@type": "Dataset",
                  "name": "Subcrate {h}",
                  "description": "Subcrate that has been published as a separate entity",
                  "distribution": {"@id": format!("{}{}",url, path)},
                  "publisher": {
                    "@id": "https://zenodo.org"
                  },
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate"
                  },
                }
            ));

            let mut inner_sub_layer_graph = vec![
                json!({
                  "@id": "ro-crate-metadata.json",
                  "@type": "CreativeWork",
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate/1.2"
                  },
                  "about": {
                    "@id": "./"
                  }
                }),
                json!({
                  "@id": "./",
                  "@type": "Dataset",
                  "name": format!("Subcrate {h}"),
                  "description": "Subcrate that has been published as a separate entity",
                  "identifier": "https://doi.org/10.5281/zenodo.1234567",
                  "datePublished": "2026-01-06",
                  "license": "https://creativecommons.org/licenses/by/4.0/",
                  "publisher": {
                    "@id": "https://zenodo.org"
                  },
                  "conformsTo": {
                    "@id": "https://w3id.org/ro/crate"
                  },

                  "hasPart": [
                    {"@id": "README.md"}
                  ]
                }),
            ];

            for j in 1..3 {
                let path = format!("/subcrate{}/subsubcrate{}", h, j);
                println!("{}", path);

                inner_sub_layer_graph.push(json!(
                    {
                      "@id": "https://doi.org/10.5281/zenodo.1234567",
                      "@type": "Dataset",
                      "name": "SubSubcrate {j}",
                      "description": "Subcrate that has been published as a separate entity",
                      "distribution": {"@id": format!("{}{}",url, path)},
                      "publisher": {
                        "@id": "https://zenodo.org"
                      },
                      "conformsTo": {
                        "@id": "https://w3id.org/ro/crate"
                      },
                    }
                ));

                let mut inner_sub_sub_layer_graph = vec![
                    json!({
                      "@id": "ro-crate-metadata.json",
                      "@type": "CreativeWork",
                      "conformsTo": {
                        "@id": "https://w3id.org/ro/crate/1.2"
                      },
                      "about": {
                        "@id": "./"
                      }
                    }),
                    json!({
                      "@id": "./",
                      "@type": "Dataset",
                      "name": format!("SubSubcrate {j}"),
                      "description": "Subcrate that has been published as a separate entity",
                      "identifier": "https://doi.org/10.5281/zenodo.1234567",
                      "datePublished": "2026-01-06",
                      "license": "https://creativecommons.org/licenses/by/4.0/",
                      "publisher": {
                        "@id": "https://zenodo.org"
                      },
                      "conformsTo": {
                        "@id": "https://w3id.org/ro/crate"
                      },

                      "hasPart": [
                        {"@id": "README.md"}
                      ]
                    }),
                ];
                for k in 1..3 {
                    let path = format!("/subcrate{}/subsubcrate{}/subsubsubcrate{}", h, j, k);
                    println!("{}", path);

                    inner_sub_sub_layer_graph.push(json!(
                        {
                          "@id": "https://doi.org/10.5281/zenodo.1234567",
                          "@type": "Dataset",
                          "name": "SubSubSubcrate {k}",
                          "description": "Subcrate that has been published as a separate entity",
                          "distribution": {"@id": format!("{}{}",url, path)},
                          "publisher": {
                            "@id": "https://zenodo.org"
                          },
                          "conformsTo": {
                            "@id": "https://w3id.org/ro/crate"
                          },
                        }
                    ));

                    let mut inner_sub_sub_sub_layer_graph = vec![
                        json!({
                          "@id": "ro-crate-metadata.json",
                          "@type": "CreativeWork",
                          "conformsTo": {
                            "@id": "https://w3id.org/ro/crate/1.2"
                          },
                          "about": {
                            "@id": "./"
                          }
                        }),
                        json!({
                          "@id": "./",
                          "@type": "Dataset",
                          "name": format!("SubSubSubcrate {k}"),
                          "description": "Subcrate that has been published as a separate entity",
                          "identifier": "https://doi.org/10.5281/zenodo.1234567",
                          "datePublished": "2026-01-06",
                          "license": "https://creativecommons.org/licenses/by/4.0/",
                          "publisher": {
                            "@id": "https://zenodo.org"
                          },
                          "conformsTo": {
                            "@id": "https://w3id.org/ro/crate"
                          },

                          "hasPart": [
                            {"@id": "README.md"}
                          ]
                        }),
                    ];
                    for l in 1..3 {
                        let path = format!(
                            "/subcrate{}/subsubcrate{}/subsubsubcrate{}/subsubsubsubcrate{}",
                            h, j, k, l
                        );
                        println!("{}", path);

                        inner_sub_sub_sub_layer_graph.push(json!(
                        {
                          "@id": "https://doi.org/10.5281/zenodo.1234567",
                          "@type": "Dataset",
                          "name": "SubSubSubcrate {k}",
                          "description": "Subcrate that has been published as a separate entity",
                          "distribution": {"@id": format!("{}{}",url, path)},
                          "publisher": {
                            "@id": "https://zenodo.org"
                          },
                          "conformsTo": {
                            "@id": "https://w3id.org/ro/crate"
                          },
                        }
                    ));

                        let subsubsubsubcrate = json!({
                        "@context": "https://w3id.org/ro/crate/1.2/context",
                        "@graph": [{
                                        "@id": "ro-crate-metadata.json",
                                        "@type": "CreativeWork",
                                        "conformsTo": {
                                          "@id": "https://w3id.org/ro/crate/1.2"
                                        },
                                        "about": {
                                          "@id": "./"
                                        }
                                      },
                                      {
                                        "@id": "./",
                                        "@type": "Dataset",
                                        "name": format!("SubSubSubcrate {k}"),
                                        "description": "Subcrate that has been published as a separate entity",
                                        "identifier": "https://doi.org/10.5281/zenodo.1234567",
                                        "datePublished": "2026-01-06",
                                        "license": "https://creativecommons.org/licenses/by/4.0/",
                                        "publisher": {
                                          "@id": "https://zenodo.org"
                                        },
                                        "conformsTo": {
                                          "@id": "https://w3id.org/ro/crate"
                                        },

                                        "hasPart": [
                                          {"@id": "README.md"}
                                        ]
                                      }]});

                        let _: RoCrate = serde_json::from_value(subsubsubsubcrate.clone()).unwrap();

                        // Direct delivery of ro-crate
                        let mock = server
                            .mock("GET", path.as_str())
                            .with_header("Content-Type", "application/json+ld")
                            .with_body(serde_json::to_string_pretty(&subsubsubsubcrate).unwrap())
                            .create();
                        mocks.push(mock);
                    }

                    let subsubsubcrate = json!({
              "@context": "https://w3id.org/ro/crate/1.2/context",
              "@graph": inner_sub_sub_sub_layer_graph});

                    let _: RoCrate = serde_json::from_value(subsubsubcrate.clone()).unwrap();

                    // Direct delivery of ro-crate
                    let mock = server
                        .mock("GET", path.as_str())
                        .with_header("Content-Type", "application/json+ld")
                        .with_body(serde_json::to_string_pretty(&subsubsubcrate).unwrap())
                        .create();
                    mocks.push(mock);
                }

                let subsubcrate = json!({
              "@context": "https://w3id.org/ro/crate/1.2/context",
              "@graph": inner_sub_sub_layer_graph});

                let _: RoCrate = serde_json::from_value(subsubcrate.clone()).unwrap();

                // Direct delivery of ro-crate
                let mock = server
                    .mock("GET", path.as_str())
                    .with_header("Content-Type", "application/json+ld")
                    .with_body(serde_json::to_string_pretty(&subsubcrate).unwrap())
                    .create();
                mocks.push(mock);
            }

            let subcrate = json!({
              "@context": "https://w3id.org/ro/crate/1.2/context",
              "@graph": inner_sub_layer_graph});

            let _: RoCrate = serde_json::from_value(subcrate.clone()).unwrap();

            // Direct delivery of ro-crate
            let mock = server
                .mock("GET", path.as_str())
                .with_header("Content-Type", "application/json+ld")
                .with_body(serde_json::to_string_pretty(&subcrate).unwrap())
                .create();
            mocks.push(mock);
        }

        let base_crate = json!({
        "@context": "https://w3id.org/ro/crate/1.2/context",
        "@graph": outer_graph});

        let root: RoCrate = serde_json::from_value(base_crate).unwrap();

        let subcrates = fetch_subcrates_recursive(&root).unwrap();

        assert_eq!(subcrates.len(), 30);

        for m in mocks {
            m.assert();
        }

    }
}
