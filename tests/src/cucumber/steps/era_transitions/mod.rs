//! RFC 439 era-transition experiments.

use std::path::Path;

use crate::cucumber::{error::StepResult, world::CucumberWorld};

mod events;
mod evidence;
mod lifecycle;
pub mod recovery;
pub mod scenario;

#[cfg(test)]
mod tests;

pub use evidence::{
    capture_node_configuration_evidence, capture_scenario_evidence, initialize_evidence_file,
};

const RECOVERY_TAG: &str = "era_boundary_recovery";

#[must_use]
pub fn has_recovery_tag(tags: &[String]) -> bool {
    tags.iter().any(|tag| tag == RECOVERY_TAG)
}

pub fn prepare_recovery_scenario(world: &mut CucumberWorld, scenario_dir: &Path) -> StepResult {
    world.era_transition = Some(crate::cucumber::world::EraTransitionScenarioState::default());
    crate::cucumber::steps::nodes::era_boundaries::initialize_evidence_file(scenario_dir)?;
    initialize_evidence_file(scenario_dir).map_err(|error| {
        crate::cucumber::error::StepError::StepFail {
            message: format!("Cannot initialize era-transition evidence: {error}"),
        }
    })
}

pub const fn preserves_scenario_artifacts(world: &CucumberWorld) -> bool {
    world.era_transition.is_some()
}

#[expect(
    clippy::cognitive_complexity,
    reason = "cleanup intentionally records each independent failure while guaranteeing ordinary node shutdown and evidence capture"
)]
pub async fn finish_recovery_scenario(world: &mut CucumberWorld) -> Vec<String> {
    if !preserves_scenario_artifacts(world) {
        return Vec::new();
    }
    let mut failures = Vec::new();
    if let Err(error) = capture_node_configuration_evidence(world) {
        tracing::error!(%error, "failed to capture era-transition configuration evidence");
        failures.push(format!("configuration evidence: {error:?}"));
    }
    if let Err(error) = scenario::capture_runtime_bindings(world) {
        tracing::error!(%error, "failed to preserve era-transition runtime ownership before shutdown");
        failures.push(format!("runtime ownership snapshot: {error:?}"));
    }
    scenario::restore_publication(world).await;
    if !world.nodes_info.is_empty()
        && let Err(error) = crate::cucumber::steps::nodes::stop_all_nodes(world).await
    {
        tracing::error!(%error, "failed to stop era-transition participants during scenario cleanup");
        failures.push(format!("node cleanup: {error:?}"));
    }
    if let Err(error) = capture_scenario_evidence(world) {
        tracing::error!(%error, "failed to preserve era-transition scenario evidence");
        failures.push(format!("scenario evidence: {error:?}"));
    }
    failures
}
