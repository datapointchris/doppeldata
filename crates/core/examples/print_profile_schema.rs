//! Prints the profile's JSON Schema. `task generate` writes it into the library.

fn main() {
    print!("{}", doppeldata_core::profile::json_schema());
}
