//! Declarative experiment participants and startup configuration.

use std::collections::{BTreeMap, HashSet};

use cucumber::{gherkin::Step, given, then, when};
use lb_libp2p::identity::{Keypair, ed25519};

use crate::cucumber::{
    error::{StepError, StepResult},
    steps::nodes::start_node,
    world::{
        ConfigOverride, CucumberWorld, EraTransitionParticipant, EraTransitionParticipantRole,
        EraTransitionRuntimeBinding, EraTransitionScenarioState,
    },
};

fn failure(message: impl Into<String>) -> StepError {
    StepError::StepFail {
        message: message.into(),
    }
}

#[given("the era-transition participants are:")]
fn declare_participants(world: &mut CucumberWorld, step: &Step) -> StepResult {
    let state = world
        .era_transition
        .get_or_insert_with(EraTransitionScenarioState::default);
    if !state.participants.is_empty() {
        return Err(failure("era-transition participants were already declared"));
    }
    let rows = &step.table.as_ref().ok_or(StepError::MissingTable)?.rows;
    state.participants = parse_participants(rows).map_err(failure)?;
    Ok(())
}

pub(super) fn parse_participants(
    rows: &[Vec<String>],
) -> Result<BTreeMap<String, EraTransitionParticipant>, String> {
    if rows.first().map(Vec::as_slice)
        != Some(&[
            "node".to_owned(),
            "role".to_owned(),
            "canonical_reference".to_owned(),
        ])
    {
        return Err("participant table requires node, role and canonical_reference columns".into());
    }

    let mut participants = BTreeMap::new();
    let mut provider_count = 0;
    let mut requester_count = 0;
    let mut unselected_requester_count = 0;
    let mut canonical_reference_count = 0;
    for (startup_index, row) in rows[1..].iter().enumerate() {
        let [node_name, role, canonical] = row.as_slice() else {
            return Err("each participant row requires exactly three values".into());
        };
        let node_name = node_name.trim();
        if node_name.is_empty() || participants.contains_key(node_name) {
            return Err(format!(
                "participant name `{node_name}` is empty or duplicated"
            ));
        }
        let role = match role.as_str() {
            "provider" => {
                provider_count += 1;
                EraTransitionParticipantRole::Provider
            }
            "requester" => {
                requester_count += 1;
                EraTransitionParticipantRole::Requester
            }
            "unselected_requester" => {
                unselected_requester_count += 1;
                EraTransitionParticipantRole::UnselectedRequester
            }
            "observer" => EraTransitionParticipantRole::Observer,
            other => {
                return Err(format!(
                    "unknown era-transition role `{other}` for {node_name}"
                ));
            }
        };
        let is_reference = match canonical.as_str() {
            "yes" => true,
            "no" => false,
            other => {
                return Err(format!(
                    "canonical_reference for {node_name} must be yes or no, got `{other}`"
                ));
            }
        };
        if is_reference {
            canonical_reference_count += 1;
        }
        participants.insert(
            node_name.to_owned(),
            EraTransitionParticipant {
                node_name: node_name.to_owned(),
                role,
                canonical_reference: is_reference,
                startup_index,
            },
        );
    }

    if provider_count != 1 || requester_count != 1 {
        return Err(format!(
            "recovery requires exactly one provider and one requester; found {provider_count} provider(s) and {requester_count} requester(s)"
        ));
    }
    if unselected_requester_count > 1 {
        return Err(format!(
            "recovery supports at most one unselected requester; found {unselected_requester_count}"
        ));
    }
    if canonical_reference_count > 1 {
        return Err("only one canonical reference may be declared".into());
    }
    Ok(participants)
}

pub(super) fn participants(
    world: &CucumberWorld,
) -> Result<&BTreeMap<String, EraTransitionParticipant>, StepError> {
    let state = world
        .era_transition
        .as_ref()
        .ok_or_else(|| failure("era-transition scenario state is not initialized"))?;
    if state.participants.is_empty() {
        return Err(failure("era-transition participants were not declared"));
    }
    Ok(&state.participants)
}

fn node_for_role(
    world: &CucumberWorld,
    role: EraTransitionParticipantRole,
) -> Result<String, StepError> {
    participants(world)?
        .values()
        .find(|participant| participant.role == role)
        .map(|participant| participant.node_name.clone())
        .ok_or_else(|| failure(format!("no {role:?} participant was declared")))
}

pub(super) fn provider_name(world: &CucumberWorld) -> Result<String, StepError> {
    node_for_role(world, EraTransitionParticipantRole::Provider)
}

pub(super) fn requester_name(world: &CucumberWorld) -> Result<String, StepError> {
    node_for_role(world, EraTransitionParticipantRole::Requester)
}

pub(super) fn unselected_requester_name(
    world: &CucumberWorld,
) -> Result<Option<String>, StepError> {
    Ok(participants(world)?
        .values()
        .find(|participant| participant.role == EraTransitionParticipantRole::UnselectedRequester)
        .map(|participant| participant.node_name.clone()))
}

fn canonical_reference_name(world: &CucumberWorld) -> Result<String, StepError> {
    participants(world)?
        .values()
        .find(|participant| participant.canonical_reference)
        .map(|participant| participant.node_name.clone())
        .ok_or_else(|| failure("no canonical-reference participant was declared"))
}

#[when(
    "I capture a non-genesis finalized block before the era boundary on the canonical reference participant"
)]
async fn capture_canonical_reference_finality(world: &mut CucumberWorld) -> StepResult {
    let canonical = canonical_reference_name(world)?;
    crate::cucumber::steps::nodes::era_boundaries::capture_pre_boundary_lib(world, canonical).await
}

pub(super) fn validate_participant_topology(world: &CucumberWorld) -> Result<(), StepError> {
    validate_participant_topology_impl(world, true)
}

pub(super) fn validate_started_participant_topology(
    world: &CucumberWorld,
) -> Result<(), StepError> {
    validate_participant_topology_impl(world, false)
}

fn validate_participant_topology_impl(
    world: &CucumberWorld,
    require_all_started: bool,
) -> Result<(), StepError> {
    let declared = participants(world)?;
    let expected = declared.keys().cloned().collect::<HashSet<_>>();
    let state = world
        .era_transition
        .as_ref()
        .ok_or_else(|| failure("era-transition scenario state is not initialized"))?;
    let started_all = world.nodes_info.keys().cloned().collect::<HashSet<_>>();
    if started_all.iter().any(|name| !expected.contains(name)) {
        return Err(failure(format!(
            "started era-transition nodes {started_all:?} include undeclared participants (declared {expected:?})"
        )));
    }
    let started = started_all
        .intersection(&expected)
        .cloned()
        .collect::<HashSet<_>>();
    if started.is_empty() {
        if !require_all_started {
            return Err(failure("no era-transition participants have started"));
        }
        let bound = state
            .runtime_bindings
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        if bound != expected {
            return Err(failure(format!(
                "declared participants {expected:?} have no matching captured runtime bindings {bound:?}"
            )));
        }
        if state
            .runtime_bindings
            .iter()
            .any(|(name, binding)| name != &binding.node_name)
        {
            return Err(failure(
                "captured participant runtime binding names conflict with their registry keys",
            ));
        }
        validate_runtime_bindings(state.runtime_bindings.values())?;
        return Ok(());
    }
    if require_all_started && started != expected {
        return Err(failure(format!(
            "declared era-transition participants {expected:?} do not match started nodes {started:?}"
        )));
    }
    let mut bindings = Vec::new();
    for participant in declared
        .values()
        .filter(|participant| started.contains(&participant.node_name))
    {
        let node = world
            .nodes_info
            .get(&participant.node_name)
            .ok_or_else(|| {
                failure(format!(
                    "participant `{}` has not started",
                    participant.node_name
                ))
            })?;
        if node.name != participant.node_name {
            return Err(failure(format!(
                "runtime node identity `{}` does not match declared participant `{}`",
                node.name, participant.node_name
            )));
        }
        if !node.runtime_dir.is_dir() {
            return Err(failure(format!(
                "participant `{}` runtime directory is missing: {}",
                participant.node_name,
                node.runtime_dir.display()
            )));
        }
        let peer_id = world
            .cluster
            .node_peer_ids
            .get(&participant.node_name)
            .ok_or_else(|| {
                failure(format!(
                    "participant `{}` has no registered runtime peer identity",
                    participant.node_name
                ))
            })?;
        bindings.push(EraTransitionRuntimeBinding {
            node_name: participant.node_name.clone(),
            runtime_name: node.runtime_name.clone(),
            runtime_dir: node.runtime_dir.clone(),
            peer_id: peer_id.to_string(),
        });
    }
    validate_runtime_bindings(bindings.iter())
}

fn validate_runtime_bindings<'a>(
    bindings: impl Iterator<Item = &'a EraTransitionRuntimeBinding>,
) -> Result<(), StepError> {
    let mut runtime_names = HashSet::new();
    let mut runtime_directories = HashSet::new();
    let mut peer_identities = HashSet::new();
    for binding in bindings {
        if binding.runtime_name.is_empty() || !runtime_names.insert(binding.runtime_name.as_str()) {
            return Err(failure(format!(
                "participant `{}` has a missing or conflicting framework runtime name `{}`",
                binding.node_name, binding.runtime_name
            )));
        }
        if !binding.runtime_dir.is_dir() {
            return Err(failure(format!(
                "participant `{}` runtime directory is missing: {}",
                binding.node_name,
                binding.runtime_dir.display()
            )));
        }
        let runtime_directory = binding.runtime_dir.canonicalize().map_err(|error| {
            failure(format!(
                "cannot resolve runtime directory for `{}` at {}: {error}",
                binding.node_name,
                binding.runtime_dir.display()
            ))
        })?;
        if !runtime_directories.insert(runtime_directory) {
            return Err(failure(format!(
                "participant `{}` shares its runtime directory with another declared node",
                binding.node_name
            )));
        }
        if binding.peer_id.is_empty() || !peer_identities.insert(binding.peer_id.as_str()) {
            return Err(failure(format!(
                "participant `{}` has a missing or shared runtime peer identity",
                binding.node_name
            )));
        }
    }
    Ok(())
}

pub fn capture_runtime_bindings(world: &mut CucumberWorld) -> Result<(), StepError> {
    validate_participant_topology(world)?;
    let bindings = participants(world)?
        .values()
        .map(|participant| {
            let node = world
                .nodes_info
                .get(&participant.node_name)
                .ok_or_else(|| {
                    failure(format!(
                        "participant `{}` is not running",
                        participant.node_name
                    ))
                })?;
            let peer_id = world
                .cluster
                .node_peer_ids
                .get(&participant.node_name)
                .ok_or_else(|| {
                    failure(format!(
                        "participant `{}` has no peer identity",
                        participant.node_name
                    ))
                })?;
            Ok((
                participant.node_name.clone(),
                EraTransitionRuntimeBinding {
                    node_name: participant.node_name.clone(),
                    runtime_name: node.runtime_name.clone(),
                    runtime_dir: node.runtime_dir.clone(),
                    peer_id: peer_id.to_string(),
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, StepError>>()?;
    world
        .era_transition
        .as_mut()
        .ok_or_else(|| failure("era-transition scenario state is not initialized"))?
        .runtime_bindings = bindings;
    Ok(())
}

pub(super) fn runtime_binding(
    world: &CucumberWorld,
    node_name: &str,
) -> Result<EraTransitionRuntimeBinding, StepError> {
    if let Some(node) = world.nodes_info.get(node_name) {
        let peer_id =
            world.cluster.node_peer_ids.get(node_name).ok_or_else(|| {
                failure(format!("participant `{node_name}` has no peer identity"))
            })?;
        return Ok(EraTransitionRuntimeBinding {
            node_name: node_name.to_owned(),
            runtime_name: node.runtime_name.clone(),
            runtime_dir: node.runtime_dir.clone(),
            peer_id: peer_id.to_string(),
        });
    }
    world
        .era_transition
        .as_ref()
        .and_then(|state| state.runtime_bindings.get(node_name))
        .cloned()
        .ok_or_else(|| {
            failure(format!(
                "participant `{node_name}` has no captured runtime binding"
            ))
        })
}

#[given("I configure initial proposal publication disabled for all era-transition participants")]
fn configure_initial_suppression(world: &mut CucumberWorld) -> StepResult {
    if !world.nodes_info.is_empty() {
        return Err(failure(
            "initial publication state must be configured before starting nodes",
        ));
    }
    let state = world
        .era_transition
        .get_or_insert_with(EraTransitionScenarioState::default);
    if state.proposal_publishing_suppression_required {
        return Err(failure(
            "proposal-publishing suppression was already declared",
        ));
    }
    state.proposal_publishing_suppression_required = true;
    for name in state.participants.keys() {
        state
            .initial_proposal_publication
            .insert(name.clone(), false);
    }
    Ok(())
}

#[given(expr = "I configure initial proposal publication {word} for participant {string}")]
#[expect(
    clippy::needless_pass_by_value,
    reason = "Cucumber converts captured arguments to owned strings"
)]
fn configure_initial_for_participant(
    world: &mut CucumberWorld,
    state: String,
    name: String,
) -> StepResult {
    let enabled = match state.as_str() {
        "enabled" => true,
        "disabled" => false,
        _ => return Err(failure("expected enabled or disabled")),
    };
    if world.nodes_info.contains_key(&name) {
        return Err(failure("initial state must be set before node startup"));
    }
    let scenario = world
        .era_transition
        .as_mut()
        .ok_or_else(|| failure("participants are not declared"))?;
    if !scenario.participants.contains_key(&name) {
        return Err(failure(format!("undeclared participant {name}")));
    }
    scenario.initial_proposal_publication.insert(name, enabled);
    Ok(())
}

fn publication_override(world: &CucumberWorld, name: &str) -> Result<ConfigOverride, StepError> {
    let enabled = world
        .era_transition
        .as_ref()
        .and_then(|state| state.initial_proposal_publication.get(name))
        .copied()
        .ok_or_else(|| {
            failure(format!(
                "initial proposal publication state for {name} is not configured"
            ))
        })?;
    Ok(ConfigOverride {
        path: "cryptarchia.leader.testing_proposal_publication_enabled".into(),
        value: serde_yaml::Value::Bool(enabled),
    })
}

pub(super) async fn set_publication(
    world: &CucumberWorld,
    name: &str,
    enabled: bool,
) -> StepResult {
    let node = world
        .nodes_info
        .get(name)
        .ok_or_else(|| failure(format!("participant {name} is not running")))?;
    let actual = node
        .client
        .set_proposal_publication(enabled)
        .await
        .map_err(|error| failure(format!("publication control for {name}: {error}")))?;
    if actual != enabled {
        return Err(failure(format!(
            "publication control for {name} returned {actual}"
        )));
    }
    Ok(())
}

#[when(expr = "I {word} proposal publishing on participant {string}")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require a mutable world"
)]
async fn set_participant_publication(
    world: &mut CucumberWorld,
    action: String,
    name: String,
) -> StepResult {
    if !participants(world)?.contains_key(&name) {
        return Err(failure(format!("undeclared participant {name}")));
    }
    let enabled = match action.as_str() {
        "enable" => true,
        "disable" => false,
        _ => return Err(failure("expected enable or disable")),
    };
    set_publication(world, &name, enabled).await
}

#[when(expr = "I {word} proposal publishing on all era-transition participants")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require a mutable world"
)]
async fn set_all_publication(world: &mut CucumberWorld, action: String) -> StepResult {
    let enabled = match action.as_str() {
        "enable" => true,
        "disable" => false,
        _ => return Err(failure("expected enable or disable")),
    };
    let names = participants(world)?.keys().cloned().collect::<Vec<_>>();
    for name in names {
        set_publication(world, &name, enabled).await?;
    }
    Ok(())
}

#[then(expr = "all era-transition participants report proposal publishing {word}")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require a mutable world"
)]
async fn verify_all_publication(world: &mut CucumberWorld, expected: String) -> StepResult {
    let enabled = match expected.as_str() {
        "enabled" => true,
        "disabled" => false,
        _ => return Err(failure("expected enabled or disabled")),
    };
    let names = participants(world)?.keys().cloned().collect::<Vec<_>>();
    for name in names {
        let node = world
            .nodes_info
            .get(&name)
            .ok_or_else(|| failure(format!("participant {name} is not running")))?;
        let actual = node
            .client
            .proposal_publication_enabled()
            .await
            .map_err(|error| failure(format!("publication query for {name}: {error}")))?;
        if actual != enabled {
            return Err(failure(format!(
                "participant {name} publication state is {actual}, expected {enabled}"
            )));
        }
        if !enabled {
            super::evidence::append_record(
                world,
                "assertion",
                serde_json::json!({"assertion":"proposal_publishing_suppressed", "node":name, "status":"passed", "publication_enabled":actual}),
            )?;
        }
    }
    Ok(())
}

#[then(expr = "participant {string} reports proposal publishing {word}")]
#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "Cucumber step callbacks require a mutable world"
)]
async fn verify_participant_publication(
    world: &mut CucumberWorld,
    name: String,
    expected: String,
) -> StepResult {
    if !participants(world)?.contains_key(&name) {
        return Err(failure(format!("undeclared participant {name}")));
    }
    let enabled = match expected.as_str() {
        "enabled" => true,
        "disabled" => false,
        _ => return Err(failure("expected enabled or disabled")),
    };
    let node = world
        .nodes_info
        .get(&name)
        .ok_or_else(|| failure(format!("participant {name} is not running")))?;
    let actual = node
        .client
        .proposal_publication_enabled()
        .await
        .map_err(|error| failure(format!("publication query for {name}: {error}")))?;
    if actual != enabled {
        return Err(failure(format!(
            "participant {name} publication state is {actual}, expected {enabled}"
        )));
    }
    Ok(())
}

pub(super) async fn restore_publication(world: &CucumberWorld) {
    for node in world.nodes_info.values() {
        if let Err(error) = node.client.set_proposal_publication(true).await {
            tracing::warn!(%error, node = %node.name, "failed to restore proposal publication during cleanup");
        }
    }
}

#[given(expr = "the provider participant holds its response stream after {int} blocks")]
fn configure_provider_hold(world: &mut CucumberWorld, block_count: u64) -> StepResult {
    let block_count = usize::try_from(block_count)
        .map_err(|_| failure("provider hold block count does not fit usize"))?;
    if block_count == 0 {
        return Err(failure(
            "provider hold must follow at least one successful response",
        ));
    }
    let state = world.era_transition.as_mut().ok_or_else(|| {
        failure("declare era-transition participants before configuring the hold")
    })?;
    let provider = state
        .participants
        .values()
        .any(|participant| participant.role == EraTransitionParticipantRole::Provider);
    if !provider {
        return Err(failure(
            "provider hold has no declared provider participant",
        ));
    }
    if state
        .provider_hold_after_blocks
        .replace(block_count)
        .is_some()
    {
        return Err(failure(
            "provider response hold was configured more than once",
        ));
    }
    Ok(())
}

#[given("the provider response hold selects the requester participant by peer identity")]
fn select_requester_for_provider_hold(world: &mut CucumberWorld) -> StepResult {
    let requester = requester_name(world)?;
    let peer_id = if let Some(peer_id) = world.cluster.node_peer_ids.get(&requester) {
        peer_id.to_string()
    } else {
        let participant = participants(world)?
            .get(&requester)
            .ok_or_else(|| failure(format!("requester `{requester}` was not declared")))?;
        let config_index = participant.startup_index;
        let cluster = world
            .cluster
            .logos_cluster()
            .ok_or_else(|| failure("peer selection requires the prepared Logos cluster"))?;
        let plan = cluster
            .deployment()
            .as_ref()
            .ok_or_else(|| failure("prepared Logos cluster has no node deployment plan"))?
            .nodes()
            .get(config_index)
            .ok_or_else(|| {
                failure(format!(
                    "no prepared node config at requester config index {config_index}"
                ))
            })?;
        Keypair::from(ed25519::Keypair::from(
            plan.general.network_config.backend.swarm.node_key.clone(),
        ))
        .public()
        .to_peer_id()
        .to_string()
    };
    let state = world
        .era_transition
        .as_mut()
        .ok_or_else(|| failure("declare era-transition participants first"))?;
    if state.provider_hold_peer_id.replace(peer_id).is_some() {
        return Err(failure(
            "provider hold peer selector was configured more than once",
        ));
    }
    Ok(())
}

#[given("I start the provider participant with its configured era-transition response hold")]
async fn start_provider_with_response_hold(world: &mut CucumberWorld) -> StepResult {
    let node_name = provider_name(world)?;
    let block_count = world
        .era_transition
        .as_ref()
        .and_then(|state| state.provider_hold_after_blocks)
        .ok_or_else(|| failure("provider response hold was not configured in Gherkin"))?;
    let mut hold_settings = serde_yaml::Mapping::new();
    hold_settings.insert(
        serde_yaml::Value::String("hold_provider_after_blocks".to_owned()),
        serde_yaml::to_value(block_count)
            .map_err(|error| failure(format!("Invalid hold block count: {error}")))?,
    );
    if let Some(peer_id) = world
        .era_transition
        .as_ref()
        .and_then(|state| state.provider_hold_peer_id.as_ref())
    {
        hold_settings.insert(
            serde_yaml::Value::String("hold_provider_for_peer_id".to_owned()),
            serde_yaml::Value::String(peer_id.clone()),
        );
    }
    if world
        .era_transition
        .as_ref()
        .is_some_and(|state| state.provider_hold_excludes_genesis_tip)
    {
        let genesis_id = world.chain.genesis_block_id.ok_or_else(|| {
            failure("the genesis-tip selector requires a prepared genesis identity")
        })?;
        hold_settings.insert(
            serde_yaml::Value::String("hold_provider_for_requester_tip_not_equal_to".to_owned()),
            serde_yaml::Value::String(format!("{genesis_id:?}")),
        );
    }
    let override_value = ConfigOverride {
        path: "network.backend.swarm.chain_sync.testing_era_boundaries".to_owned(),
        value: serde_yaml::Value::Mapping(hold_settings),
    };
    start_node(
        world,
        "start declared provider with configured era-transition response hold",
        &node_name,
        &[],
        &[],
        false,
        &[override_value, publication_override(world, &node_name)?],
    )
    .await
}

#[given("the provider response hold excludes the requester's genesis-tip IBD request")]
fn exclude_genesis_tip_request_from_provider_hold(world: &mut CucumberWorld) -> StepResult {
    let state = world.era_transition.as_mut().ok_or_else(|| {
        failure("declare era-transition participants before configuring the hold")
    })?;
    if !state.orphan_recovery {
        return Err(failure(
            "the genesis-tip hold selector is only valid for runtime orphan recovery",
        ));
    }
    if state.provider_hold_excludes_genesis_tip {
        return Err(failure(
            "the provider response hold genesis-tip selector was configured more than once",
        ));
    }
    state.provider_hold_excludes_genesis_tip = true;
    Ok(())
}

#[given(
    "I start the requester participant connected to the provider participant with tip polling disabled"
)]
#[when(
    "I start the requester participant connected to the provider participant with tip polling disabled"
)]
async fn start_requester_with_recovery_observation(world: &mut CucumberWorld) -> StepResult {
    let requester = requester_name(world)?;
    let provider = provider_name(world)?;
    let overrides = [
        publication_override(world, &requester)?,
        ConfigOverride {
            path: "network.backend.swarm.chain_sync.testing_era_boundaries".to_owned(),
            value: serde_yaml::Value::Mapping(serde_yaml::Mapping::default()),
        },
        ConfigOverride {
            path: "cryptarchia.network.sync.tip_poll.enabled".to_owned(),
            value: serde_yaml::Value::Bool(false),
        },
    ];
    start_node(
        world,
        "start declared requester with evidence and tip polling disabled",
        &requester,
        &[],
        &[provider],
        false,
        &overrides,
    )
    .await
}

#[when(
    "I start the unselected requester participant connected to the provider participant with tip polling enabled"
)]
async fn start_unselected_requester(world: &mut CucumberWorld) -> StepResult {
    let requester = unselected_requester_name(world)?
        .ok_or_else(|| failure("no unselected requester participant was declared"))?;
    let provider = provider_name(world)?;
    let overrides = [
        publication_override(world, &requester)?,
        ConfigOverride {
            path: "network.backend.swarm.chain_sync.testing_era_boundaries".to_owned(),
            value: serde_yaml::Value::Mapping(serde_yaml::Mapping::default()),
        },
        ConfigOverride {
            path: "cryptarchia.network.sync.tip_poll.enabled".to_owned(),
            value: serde_yaml::Value::Bool(true),
        },
    ];
    start_node(
        world,
        "start declared unselected requester with evidence and tip polling enabled",
        &requester,
        &[],
        &[provider],
        false,
        &overrides,
    )
    .await
}

#[when("I start all declared observer participants connected to the provider participant")]
async fn start_observer_participants(world: &mut CucumberWorld) -> StepResult {
    let provider = provider_name(world)?;
    let observers = participants(world)?
        .values()
        .filter(|participant| participant.role == EraTransitionParticipantRole::Observer)
        .map(|participant| participant.node_name.clone())
        .collect::<Vec<_>>();
    for observer in observers {
        let overrides = [
            publication_override(world, &observer)?,
            ConfigOverride {
                path: "network.backend.swarm.chain_sync.testing_era_boundaries".to_owned(),
                value: serde_yaml::Value::Mapping(serde_yaml::Mapping::default()),
            },
        ];
        start_node(
            world,
            "start declared observer with era-transition evidence",
            &observer,
            &[],
            std::slice::from_ref(&provider),
            false,
            &overrides,
        )
        .await?;
    }
    validate_participant_topology(world)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(entries: &[(&str, &str, &str)]) -> Vec<Vec<String>> {
        std::iter::once(vec![
            "node".into(),
            "role".into(),
            "canonical_reference".into(),
        ])
        .chain(entries.iter().map(|(node, role, canonical)| {
            vec![(*node).into(), (*role).into(), (*canonical).into()]
        }))
        .collect()
    }

    #[test]
    fn parses_provider_requester_and_observers() {
        let parsed = parse_participants(&rows(&[
            ("NODE_1", "provider", "yes"),
            ("NODE_2", "requester", "no"),
            ("NODE_3", "observer", "no"),
        ]))
        .unwrap();
        assert_eq!(parsed.len(), 3);
        assert!(parsed["NODE_1"].canonical_reference);
    }

    #[test]
    fn parses_one_unselected_requester_for_concurrent_recovery() {
        let parsed = parse_participants(&rows(&[
            ("NODE_1", "provider", "yes"),
            ("NODE_2", "requester", "no"),
            ("NODE_3", "unselected_requester", "no"),
        ]))
        .unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(
            parsed["NODE_3"].role,
            EraTransitionParticipantRole::UnselectedRequester
        );
        assert_eq!(parsed["NODE_3"].startup_index, 2);
        assert!(
            parse_participants(&rows(&[
                ("NODE_1", "provider", "yes"),
                ("NODE_2", "requester", "no"),
                ("NODE_3", "unselected_requester", "no"),
                ("NODE_4", "unselected_requester", "no"),
            ]))
            .unwrap_err()
            .contains("at most one unselected requester")
        );
    }

    #[test]
    fn rejects_duplicate_names_unknown_roles_missing_roles_and_bad_references() {
        assert!(
            parse_participants(&rows(&[
                ("NODE_1", "provider", "yes"),
                ("NODE_1", "requester", "no"),
            ]))
            .unwrap_err()
            .contains("duplicated")
        );
        assert!(
            parse_participants(&rows(&[
                ("NODE_1", "provider", "yes"),
                ("NODE_2", "peer", "no"),
            ]))
            .unwrap_err()
            .contains("unknown")
        );
        assert!(
            parse_participants(&rows(&[("NODE_1", "provider", "yes")]))
                .unwrap_err()
                .contains("exactly one provider and one requester")
        );
        assert!(
            parse_participants(&rows(&[
                ("NODE_1", "provider", "yes"),
                ("NODE_2", "requester", "yes"),
            ]))
            .unwrap_err()
            .contains("only one canonical")
        );
        let observer_reference = parse_participants(&rows(&[
            ("NODE_1", "provider", "no"),
            ("NODE_2", "requester", "no"),
            ("NODE_3", "observer", "yes"),
        ]))
        .unwrap();
        assert!(observer_reference["NODE_3"].canonical_reference);
        assert!(
            parse_participants(&rows(&[
                ("NODE_1", "provider", "no"),
                ("NODE_2", "requester", "no"),
            ]))
            .is_ok()
        );
    }

    #[test]
    fn runtime_ownership_requires_unique_names_directories_and_peer_ids() {
        let directory_one = tempfile::tempdir().unwrap();
        let directory_two = tempfile::tempdir().unwrap();
        let binding_one = EraTransitionRuntimeBinding {
            node_name: "NODE_1".into(),
            runtime_name: "runtime-1".into(),
            runtime_dir: directory_one.path().to_owned(),
            peer_id: "peer-1".into(),
        };
        let binding_two = EraTransitionRuntimeBinding {
            node_name: "NODE_2".into(),
            runtime_name: "runtime-2".into(),
            runtime_dir: directory_two.path().to_owned(),
            peer_id: "peer-2".into(),
        };
        assert!(validate_runtime_bindings([&binding_one, &binding_two].into_iter()).is_ok());

        let duplicate_runtime = EraTransitionRuntimeBinding {
            runtime_name: binding_one.runtime_name.clone(),
            ..binding_two.clone()
        };
        assert!(validate_runtime_bindings([&binding_one, &duplicate_runtime].into_iter()).is_err());

        let shared_directory = EraTransitionRuntimeBinding {
            runtime_dir: binding_one.runtime_dir.clone(),
            ..binding_two.clone()
        };
        assert!(validate_runtime_bindings([&binding_one, &shared_directory].into_iter()).is_err());

        let duplicate_peer = EraTransitionRuntimeBinding {
            peer_id: binding_one.peer_id.clone(),
            ..binding_two.clone()
        };
        assert!(validate_runtime_bindings([&binding_one, &duplicate_peer].into_iter()).is_err());

        let missing_peer = EraTransitionRuntimeBinding {
            peer_id: String::new(),
            ..binding_two
        };
        assert!(validate_runtime_bindings([&binding_one, &missing_peer].into_iter()).is_err());

        let missing_directory = EraTransitionRuntimeBinding {
            runtime_dir: directory_one.path().join("missing"),
            ..binding_one
        };
        assert!(validate_runtime_bindings([&missing_directory].into_iter()).is_err());
    }
}
