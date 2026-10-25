//! File sizes in rows drawn from a lognormal, in place of the layout's. The writers draw them as
//! they draw a profile's `rows_per_file`, from quantiles of the lognormal.

use std::collections::BTreeMap;

use super::Planned;
use super::Planning;
use super::ScenarioFault;
use super::Z_99;
use crate::plan::PlanError;
use crate::profile::Quantiles;
use crate::samplers::normal_quantile;
use crate::scenario::SmallFiles;

/// The levels the lognormal is written at, by the names a profile's quantiles take.
const LEVELS: [(&str, f64); 9] = [
    ("p01", 0.01),
    ("p05", 0.05),
    ("p10", 0.10),
    ("p25", 0.25),
    ("p50", 0.50),
    ("p75", 0.75),
    ("p90", 0.90),
    ("p95", 0.95),
    ("p99", 0.99),
];

pub(super) fn plan(op: &SmallFiles, planning: &Planning<'_>) -> Result<Planned, PlanError> {
    let median = op.file_rows.median;
    if median < 1.0 {
        return Err(planning.refuse(ScenarioFault::InvalidParameter, "/file_rows/median", "a file holds at least 1 row"));
    }
    let sigma = match op.file_rows.p99 {
        Some(p99) if p99 <= median => {
            return Err(planning.refuse(
                ScenarioFault::InvalidParameter,
                "/file_rows/p99",
                "the file size's p99 must lie above its median",
            ));
        }
        Some(p99) => libm::log(p99 / median) / Z_99,
        None => 1.0,
    };
    let quantiles: BTreeMap<String, f64> = LEVELS
        .iter()
        .map(|&(name, level)| (name.to_string(), (median * libm::exp(sigma * normal_quantile(level))).max(1.0)))
        .collect();
    Ok(Planned::SmallFiles(Quantiles(quantiles)))
}
