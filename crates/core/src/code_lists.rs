//! The code lists the engine ships: the healthcare detector pack types a column by membership in
//! one, and the `icd10cm` and `hcpcs_level2` providers draw from one. Each file holds one code a
//! line, without its dot, after a first line naming the list's release and where it was taken from.
//! A generated value is a list's code at a drawn index, so a list's contents are part of the stream.

use std::sync::LazyLock;

use crate::profile::Provider;

/// A shipped code list: its name, the release its first line names, and its codes in file order.
#[derive(Debug)]
pub struct CodeList {
    pub name: &'static str,
    pub release: &'static str,
    pub codes: Vec<&'static str>,
}

impl CodeList {
    fn parsed(name: &'static str, text: &'static str) -> Self {
        let mut lines = text.lines();
        let release = lines.next().unwrap_or_default().trim_start_matches('#').trim();
        let codes = lines
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .collect();
        Self { name, release, codes }
    }
}

static ICD10CM: LazyLock<CodeList> = LazyLock::new(|| CodeList::parsed("icd10cm", include_str!("../code_lists/icd10cm.txt")));
static HCPCS_LEVEL2: LazyLock<CodeList> =
    LazyLock::new(|| CodeList::parsed("hcpcs_level2", include_str!("../code_lists/hcpcs_level2.txt")));

/// The shipped list named, or None where the engine ships no list of that name.
pub fn code_list(name: &str) -> Option<&'static CodeList> {
    match name {
        "icd10cm" => Some(&ICD10CM),
        "hcpcs_level2" => Some(&HCPCS_LEVEL2),
        _ => None,
    }
}

/// The list a provider draws from, or None where it draws from no shipped list.
pub(crate) fn provider_list(provider: Provider) -> Option<&'static CodeList> {
    match provider {
        Provider::Icd10cm => code_list("icd10cm"),
        Provider::HcpcsLevel2 => code_list("hcpcs_level2"),
        _ => None,
    }
}
