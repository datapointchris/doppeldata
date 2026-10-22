//! Every JSON Schema the library ships, each under the file name the library reads it from.

use crate::profile;

pub struct PublishedSchema {
    /// The name `doppeldata schema show` takes.
    pub name: &'static str,
    pub file_name: String,
    pub derive: fn() -> String,
}

pub fn all() -> Vec<PublishedSchema> {
    vec![PublishedSchema {
        name: "profile",
        file_name: format!("profile.v{}.json", profile::FORMAT_VERSION),
        derive: profile::json_schema,
    }]
}
