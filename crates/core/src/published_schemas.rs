//! Every JSON Schema the library ships, each under the file name the library reads it from.

use crate::manifest;
use crate::profile;
use crate::profile_comparison;
use crate::scenario;

pub struct PublishedSchema {
    /// The name `doppeldata schemas show` takes.
    pub name: &'static str,
    pub file_name: &'static str,
    pub derive: fn() -> String,
}

pub fn all() -> Vec<PublishedSchema> {
    vec![
        PublishedSchema {
            name: "profile",
            file_name: "profile.json",
            derive: profile::json_schema,
        },
        PublishedSchema {
            name: "manifest",
            file_name: "manifest.json",
            derive: manifest::json_schema,
        },
        PublishedSchema {
            name: "faults",
            file_name: "faults.json",
            derive: manifest::faults_json_schema,
        },
        PublishedSchema {
            name: "check",
            file_name: "check.json",
            derive: profile_comparison::check_json_schema,
        },
        PublishedSchema {
            name: "diff",
            file_name: "diff.json",
            derive: profile_comparison::diff_json_schema,
        },
        PublishedSchema {
            name: "scenario",
            file_name: "scenario.json",
            derive: scenario::json_schema,
        },
    ]
}
