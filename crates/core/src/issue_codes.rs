//! Every code the engine refuses with. Each is declared once, as a static `ALL` lists, and the
//! library binds the code it raises for the same fault by the static's name.

/// A refusal's stable code, `E-<AREA>-<NNN>`, with what raises it and the exit status a command
/// ends on: 1 for a failure, 2 for a call or file that must change, 3 for a finding a person acts
/// on, and 4 when no answer came.
#[derive(Debug, PartialEq, Eq)]
pub struct IssueCode {
    /// The static's name, by which the library binds the code it raises for the same fault.
    pub name: &'static str,
    pub code: &'static str,
    pub raised_when: &'static str,
    /// The next thing to try. One starting `doppeldata` is a command that runs as written.
    pub hint: Option<&'static str>,
    pub exit_code: u8,
}

impl IssueCode {
    /// The area the code names, such as `PROFILE` in `E-PROFILE-101`.
    pub fn area(&self) -> &'static str {
        self.code.split('-').nth(1).unwrap_or_default()
    }
}

impl std::fmt::Display for IssueCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code)
    }
}

/// Declares each code as a static `IssueCode` named for it, and `ALL`, listing them in order.
#[macro_export]
macro_rules! issue_codes {
    ($($name:ident = $code:literal, exit $exit:literal, $raised_when:literal $(, hint $hint:literal)?;)*) => {
        $(
            pub static $name: $crate::issue_codes::IssueCode = $crate::issue_codes::IssueCode {
                name: stringify!($name),
                code: $code,
                raised_when: $raised_when,
                hint: $crate::optional_hint!($($hint)?),
                exit_code: $exit,
            };
        )*
        /// Every code declared here, in declaration order.
        pub static ALL: &[&$crate::issue_codes::IssueCode] = &[$(&$name),*];
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! optional_hint {
    () => {
        None
    };
    ($hint:literal) => {
        Some($hint)
    };
}

issue_codes! {
    BUNDLE_GIVEN = "E-PROFILE-101", exit 2, "a bundle was given where one table is planned";
    NO_COLUMNS = "E-PROFILE-102", exit 2, "the profile lists no columns to generate";
    DUPLICATE_COLUMN = "E-PROFILE-103", exit 2, "two columns share a name, so both would draw the same stream";
    UNSUPPORTED_TYPE = "E-PROFILE-104", exit 2, "a column's physical_type names an Arrow type the engine does not write";
    NOT_GENERATED_YET = "E-PROFILE-105", exit 2, "a column holds a statistic the engine does not generate from yet";
    INVALID_FIT = "E-PROFILE-106", exit 2, "a fit names a family the engine inverts, with a parameter outside its domain";
    INVALID_PATTERN = "E-PROFILE-107", exit 2, "a pattern does not parse, or no string matches it";
    INVALID_QUANTILES = "E-PROFILE-108", exit 2, "quantiles or histogram edges fall, or a quantile's name names no level";
    UNUSABLE_FRACTIONS = "E-PROFILE-109", exit 2, "fractions no alias table can draw from, such as every one zero";
    INVALID_EXTREME = "E-PROFILE-110", exit 2, "a temporal extreme does not parse as its column's type";
    STRUCT_WITHOUT_FIELDS = "E-PROFILE-111", exit 2, "a struct column lists no fields";
    TOKENS_EXCEED_NULL_FRACTION = "E-PROFILE-112", exit 2, "a column's missing_token_fractions sum above its null_fraction";
    UNKNOWN_COLUMN = "E-PROFILE-113", exit 2, "a dependence or a relationship names a column its table lacks";
    INVALID_DEPENDENCE = "E-PROFILE-114", exit 2,
        "a dependence cannot be drawn: an expression that does not parse, a column of a kind it cannot apply to, a value its column cannot hold, or a column depending on itself";
    TABLE_NOT_INLINE = "E-PROFILE-115", exit 2, "a bundle's table is given by its file's path, which the library writes inline before planning";
    NO_ROWS = "E-PROFILE-116", exit 2, "a table states no dataset.rows, and no row count was given",
        hint "write dataset.rows into the profile, or give the rows to generate";
    MISMATCHED_COLUMNS = "E-PROFILE-117", exit 2, "a relationship's from and to differ in length, or one side names columns of several tables";
    SELF_REFERENCE = "E-PROFILE-118", exit 2, "a relationship runs from a table to itself, which is not generated yet";
    RELATIONSHIP_CYCLE = "E-PROFILE-119", exit 2, "the bundle's tables refer to each other in a loop, so none can be drawn first";
    PARENT_KEY_UNADDRESSABLE = "E-PROFILE-120", exit 2,
        "a child refers to a parent key no row can be asked for: not a sequence, a uuid, an integer or text";
    INCOMPATIBLE_TYPES = "E-PROFILE-121", exit 2, "a child column's type cannot hold the values it takes from its parent";
    INVALID_FANOUT = "E-PROFILE-122", exit 2, "a relationship's fan-out quantiles fall, or no counts can hold its top_fractions";
    PROFILE_MODEL_MISMATCH = "E-PROFILE-123", exit 1,
        "a profile the library validated does not read as the engine's model, as a library and engine of different releases give";
    TABLE_NOT_BUNDLE = "E-PROFILE-124", exit 2, "a table profile was given where a bundle is planned";

    UNKNOWN_SCENARIO_COLUMN = "E-SCENARIO-101", exit 2, "an op or the patch names a column the profile does not hold";
    OP_NOT_FOR_COLUMN = "E-SCENARIO-102", exit 2, "an op cannot act on its column's type, such as noise on a string";
    INVALID_PARAMETER = "E-SCENARIO-103", exit 2, "a parameter is out of range, or names what this profile or these files cannot hold",
        hint "doppeldata scenario show OP lists the op's parameters";
    DUPLICATE_PERTURBATION_ID = "E-SCENARIO-104", exit 2, "two perturbations share an id, so both would draw the same stream";
    OP_NOT_FOR_FORMAT = "E-SCENARIO-105", exit 2, "an op or a fault kind is one the output format cannot hold",
        hint "doppeldata capabilities show lists the formats each op takes";
    NO_PRIMARY_KEY = "E-SCENARIO-106", exit 2,
        "a change stream has no one column to key it by: no key, and the profile states no primary key or one of several columns";
    OP_NEEDS_ONE_PROCESS = "E-SCENARIO-107", exit 2, "an op that orders the whole table, such as cdc, was asked of a write split across processes",
        hint "write it with doppeldata.write or doppeldata data generate";
    UNKNOWN_TABLE = "E-SCENARIO-108", exit 2,
        "a scenario names no table of the bundle, or one it lacks, or a composition bends more than one table";
    UNKNOWN_RELATIONSHIP = "E-SCENARIO-109", exit 2, "the patch names a relationship that draws no column of the scenario's table";
    UNKNOWN_METRIC = "E-SCENARIO-110", exit 2, "an expectation names a metric doppeldata does not measure",
        hint "doppeldata scenario list names every metric";
    UNKNOWN_FAULT_KIND = "E-SCENARIO-111", exit 2, "malformed_rows names a fault kind doppeldata does not write",
        hint "doppeldata scenario show malformed_rows lists every kind";
    SCENARIO_MODEL_MISMATCH = "E-SCENARIO-112", exit 1,
        "a scenario the library validated does not read as the engine's model, as a library and engine of different releases give";

    INVALID_SCALE = "E-USAGE-101", exit 2, "a bundle's scale is not a positive finite number";
    INVALID_PROFILE_DIGEST = "E-USAGE-102", exit 2, "a profile digest is not sha256: and 64 lowercase hex digits";
    INVALID_SCENARIO_DIGEST = "E-USAGE-103", exit 2, "a scenario digest is not sha256: and 64 lowercase hex digits";
    INVALID_OVERRIDE = "E-USAGE-104", exit 2,
        "overrides are not a mapping of column names to values, or name a column the table lacks, or a value its column cannot hold";
    BLOCKS_OUTSIDE_PLAN = "E-USAGE-105", exit 2, "the blocks asked for are not a run within the plan's blocks";
    UNKNOWN_CHECK_DIGIT = "E-USAGE-106", exit 2, "a check digit's kind is not one the engine writes";
    UNKNOWN_FORMAT = "E-USAGE-107", exit 2, "a format is not one doppeldata writes",
        hint "doppeldata capabilities show lists the formats";
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    #[test]
    fn every_code_is_declared_once_in_the_form_and_area_the_library_takes() {
        let areas = ["PROFILE", "SCENARIO", "POLICY", "DATA", "WRITE", "IMPORT", "EXPORT", "IO", "USAGE"];
        let mut seen = BTreeSet::new();
        for declared in super::ALL {
            let digits = declared.code.rsplit('-').next().unwrap_or_default();
            assert!(declared.code.starts_with("E-") && digits.len() == 3 && digits.bytes().all(|b| b.is_ascii_digit()));
            assert!(areas.contains(&declared.area()), "{} names no area", declared.code);
            assert!(seen.insert(declared.code), "{} is declared twice", declared.code);
        }
    }
}
