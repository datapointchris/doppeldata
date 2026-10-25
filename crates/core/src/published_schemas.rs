//! Every JSON Schema the library ships, each under the file name the library reads it from.

use crate::manifest;
use crate::profile;
use crate::profile_comparison;

pub struct PublishedSchema {
    /// The name `doppeldata schemas show` takes.
    pub name: &'static str,
    pub file_name: String,
    pub derive: fn() -> String,
}

pub fn all() -> Vec<PublishedSchema> {
    vec![
        PublishedSchema {
            name: "profile",
            file_name: format!("profile.v{}.json", profile::FORMAT_VERSION),
            derive: profile::json_schema,
        },
        PublishedSchema {
            name: "manifest",
            file_name: format!("manifest.v{}.json", manifest::MANIFEST_VERSION),
            derive: manifest::json_schema,
        },
        PublishedSchema {
            name: "check",
            file_name: format!("check.v{}.json", profile_comparison::CHECK_VERSION),
            derive: profile_comparison::check_json_schema,
        },
        PublishedSchema {
            name: "diff",
            file_name: format!("diff.v{}.json", profile_comparison::DIFF_VERSION),
            derive: profile_comparison::diff_json_schema,
        },
    ]
}
