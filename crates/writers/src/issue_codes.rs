//! Every code a write refuses or fails with. Each is declared once, as a static `ALL` lists, and
//! the library binds the code it raises for the same fault by the static's name.

doppeldata_core::issue_codes! {
    INVALID_LAYOUT = "E-WRITE-201", exit 2, "the layout asked for cannot be written: file rows, partition keys or a style the plan cannot hold";
    INVALID_SETTINGS = "E-WRITE-202", exit 2, "the format's writer settings do not apply to it",
        hint "doppeldata schemas show manifest names each format's settings";
    UNSUPPORTED_COLUMN = "E-WRITE-203", exit 2, "a column's type is one the format, or Spark, cannot hold";
    DESTINATION_NOT_EMPTY = "E-WRITE-204", exit 2, "the destination already holds objects",
        hint "rerun with --replace, or replace=True, to replace what an earlier doppeldata write left there";
    FOREIGN_OBJECT = "E-WRITE-205", exit 2, "replace was set, and the destination holds an object no doppeldata manifest there accounts for",
        hint "remove the object, or write somewhere else";
    UNCLAIMABLE = "E-WRITE-206", exit 2, "the store cannot create an object only where none exists, so the write cannot claim the destination",
        hint "enable conditional put on the store";
    INVALID_OBJECT_NAME = "E-WRITE-207", exit 2, "an object name the write would store is one the store refuses";
    DESTINATION_CLAIMED = "E-WRITE-208", exit 1, "another write claimed the destination first, and two writes never share one";
    CURRENT_THREAD_RUNTIME = "E-WRITE-209", exit 1, "the runtime supplied is current-thread, which nothing drives while the write blocks its caller";
    IDENTITY_MISMATCH = "E-WRITE-210", exit 1, "the plan's dataset identity is not the one the manifest records";
    PARTIAL_MANIFEST = "E-WRITE-211", exit 1, "the manifest is partial, so it lists only what a stopped run completed",
        hint "write the dataset again";
    REGENERATED_FILES_DIFFER = "E-WRITE-212", exit 1, "objects regenerated from a manifest differ from its record of them";
    CANCELLED = "E-WRITE-213", exit 1, "the write was cancelled, or a signal interrupted it";
    RANGE_OUT_OF_LAYOUT = "E-WRITE-214", exit 2, "a range asked for is past the layout's ranges";
    RANGES_UNRECORDED = "E-WRITE-215", exit 1, "a commit found ranges of the layout with no object recorded";
    UNFORMATTABLE_VALUE = "E-WRITE-216", exit 1, "a value would not format as text";
    BLOCK_OUT_OF_RANGE = "E-WRITE-217", exit 1, "a write asked for a block past the plan's";
    ARROW = "E-WRITE-218", exit 1, "Arrow refused a batch while the write encoded it";
    PARQUET = "E-WRITE-219", exit 1, "the Parquet encoder refused a batch";
    CLAIM_NOT_REMOVED = "E-WRITE-220", exit 1, "the write stored its manifest, then could not remove its claim _manifest.partial.json",
        hint "delete _manifest.partial.json at the destination";
    MANIFEST_UNREADABLE = "E-WRITE-221", exit 2, "a manifest given to commit or regenerate from does not read as one";
    UNRETURNED_RANGE = "E-WRITE-222", exit 2, "a commit was given a range record write_ranges did not return";

    NO_CREDENTIALS = "E-IO-201", exit 1, "no credentials were found to sign a request to the store, or another AWS service, with",
        hint "configure AWS credentials, such as with aws configure sso or AWS_PROFILE";
    STORE_REFUSED = "E-IO-202", exit 1, "the store refused a request as not permitted";
    STORE = "E-IO-203", exit 1, "the store, or another AWS service, answered a request with an error";
    NO_ANSWER = "E-IO-204", exit 4,
        "the store, or another service, never answered: a connection or request failed or timed out on every retry";
    LOCAL_IO = "E-IO-205", exit 1, "a local file could not be read or written";
    FILE_METADATA_UNREADABLE = "E-IO-206", exit 2, "a Parquet or Arrow IPC file's footer or schema does not read";
}
