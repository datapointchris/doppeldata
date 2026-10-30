//! Writes every published JSON Schema into the directory the first argument names. `task generate`
//! runs it. Each file is staged beside its target and renamed over it, so a failed run leaves the
//! committed file whole.

use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;

fn main() -> io::Result<()> {
    let dir = PathBuf::from(env::args().nth(1).expect("usage: write_schemas DIR"));
    for schema in doppeldata_core::published_schemas::all() {
        let staged = dir.join(format!(".{}.tmp", schema.file_name));
        fs::write(&staged, (schema.derive)())?;
        fs::rename(&staged, dir.join(schema.file_name))?;
    }
    Ok(())
}
