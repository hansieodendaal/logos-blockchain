use std::{
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    num::{NonZero, NonZeroU32},
    path::PathBuf,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use lb_chain_service::Epoch;
pub use lb_config::consensus::SdpFundingConfig;
use lb_config::preparation::prepare_network;
use lb_core::{block::genesis::GenesisBlock, mantle::GenesisTime};
use lb_node::config::{
    RunConfig,
    deployment::{DeploymentSettings, EraDeclarations, era::ruleset::EraRuleset},
};
use lb_utils::math::NonNegativeRatio;
use rand::{Rng, SeedableRng as _};
use testing_framework_core::topology::{DeploymentProvider, DeploymentSeed, DynTopologyError};
use thiserror::Error;

use super::{
    Libp2pNetworkLayout, NetworkParams,
    wallet::{WalletConfig, WalletConfigError},
};
use crate::{DeploymentPlan, config::PreparedDeployment, get_reserved_available_udp_port};

pub type DynError = Box<dyn Error + Send + Sync + 'static>;
const DEFAULT_SLOT_TIME_IN_SECS: u64 = 1;
const DEFAULT_ACTIVE_SLOT_COEFF: NonNegativeRatio =
    NonNegativeRatio::new(1, NonZeroU32::new(10).unwrap());
const DEFAULT_SECURITY_PARAM: NonZeroU32 = NonZeroU32::new(20).unwrap();

static RESERVED_AUTOMATIC_GENESIS_TIMES: LazyLock<Mutex<HashSet<GenesisTime>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// Reserves a near-current genesis second for a new automatically configured
/// deployment in this process.
#[must_use]
pub fn resolve_automatic_genesis_time() -> GenesisTime {
    resolve_automatic_genesis_time_after(0)
}

/// Reserves a genesis second at least `seconds_from_now` in the future.
#[must_use]
pub fn resolve_automatic_genesis_time_after(seconds_from_now: u64) -> GenesisTime {
    let seconds_from_now = i64::try_from(seconds_from_now)
        .expect("automatic genesis time delay should fit in i64 seconds");
    let requested = (time::OffsetDateTime::now_utc() + time::Duration::seconds(seconds_from_now))
        .try_into()
        .expect("current time should fit in GenesisTime");
    let mut reserved = RESERVED_AUTOMATIC_GENESIS_TIMES
        .lock()
        .expect("automatic genesis time reservation lock should not be poisoned");

    let mut candidate = requested;
    while !reserved.insert(candidate) {
        let next = time::OffsetDateTime::from(candidate) + time::Duration::seconds(1);
        candidate = next
            .try_into()
            .expect("automatic genesis time should fit in GenesisTime");
    }

    candidate
}

#[derive(Debug, Error)]
pub enum TopologyBuildError {
    #[error("internal config vector mismatch for {label} (expected {expected}, got {actual})")]
    VectorLenMismatch {
        label: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("failed to allocate blend UDP ports for topology")]
    BlendPortAllocation,
    #[error(transparent)]
    InvalidWallet(#[from] WalletConfigError),
}

/// Consecutive era transition used by the real-node happy-path fixture.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EraBoundarySchedule {
    previous_era: u32,
    target_era: u32,
    activation_epoch: u32,
}

impl EraBoundarySchedule {
    pub fn new(
        previous_era: u32,
        target_era: u32,
        activation_epoch: u32,
    ) -> Result<Self, &'static str> {
        if previous_era.checked_add(1) != Some(target_era) {
            return Err("The target era must immediately follow the previous era");
        }
        if activation_epoch <= previous_era {
            return Err("The activation epoch must leave room for all preceding era declarations");
        }
        Ok(Self {
            previous_era,
            target_era,
            activation_epoch,
        })
    }

    #[must_use]
    pub const fn previous_era(self) -> u32 {
        self.previous_era
    }

    #[must_use]
    pub const fn target_era(self) -> u32 {
        self.target_era
    }

    #[must_use]
    pub const fn activation_epoch(self) -> u32 {
        self.activation_epoch
    }
}

/// Defines the profile of the node binary to be used in the deployment.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub enum NodeBinaryProfile {
    #[default]
    Normal,
    TokioConsole,
}

/// Environment variable name for the node binary profile.
pub const NODE_BINARY_PROFILE: &str = "NODE_BINARY_PROFILE";
///  Environment variable value for the normal node binary profile.
const NODE_BINARY_PROFILE_NORMAL: &str = "normal";
/// Environment variable value for the tokio-console node binary profile.
const NODE_BINARY_PROFILE_TOKIO_CONSOLE: &str = "tokio-console";

impl NodeBinaryProfile {
    /// Resolves an explicit environment selection over the profile configured
    /// for the deployment.
    #[must_use]
    pub fn resolve(configured: Self, environment_override: Option<&str>) -> Self {
        environment_override.map_or(configured, Self::from_string)
    }

    /// Resolves the profile using the shared `NODE_BINARY_PROFILE` override.
    #[must_use]
    pub fn from_env_or(configured: Self) -> Self {
        let environment_override = std::env::var(NODE_BINARY_PROFILE).ok();
        Self::resolve(configured, environment_override.as_deref())
    }

    #[must_use]
    pub fn from_string(s: &str) -> Self {
        match s {
            NODE_BINARY_PROFILE_TOKIO_CONSOLE => Self::TokioConsole,
            _ => Self::Normal,
        }
    }

    #[must_use]
    pub const fn to_string(&self) -> &'static str {
        match self {
            Self::Normal => NODE_BINARY_PROFILE_NORMAL,
            Self::TokioConsole => NODE_BINARY_PROFILE_TOKIO_CONSOLE,
        }
    }
}

/// High-level topology settings used to generate node configs for a scenario.
#[derive(Clone)]
pub struct TopologyConfig {
    pub n_nodes: usize,
    pub blend_core_nodes: usize,
    pub sdp_funding_config: SdpFundingConfig,
    pub network_params: Arc<NetworkParams>,
    pub wallet_config: WalletConfig,
    pub scenario_base_dir: PathBuf,
    pub genesis_block: Option<GenesisBlock>,
    requested_genesis_time: Option<GenesisTime>,
    genesis_time: Option<GenesisTime>,
    pub slot_duration: Option<Duration>,
    pub active_slot_coeff: NonNegativeRatio,
    pub security_param: NonZeroU32,
    era_boundary_schedule: Option<EraBoundarySchedule>,
    node_config_overrides: HashMap<usize, RunConfig>,
    allow_multiple_genesis_tokens: bool,
    allow_zero_value_genesis_tokens: bool,
    pub test_context: Option<String>,
    pub(super) node_binary_profile: NodeBinaryProfile,
    node_binary_profile_resolved: bool,
}

impl TopologyConfig {
    fn with_node_count(nodes: usize) -> Self {
        Self {
            n_nodes: nodes,
            blend_core_nodes: nodes,
            ..Self::default()
        }
    }

    #[must_use]
    pub const fn with_allow_multiple_genesis_tokens(mut self, allow_multiple: bool) -> Self {
        self.allow_multiple_genesis_tokens = allow_multiple;
        self
    }

    #[must_use]
    pub const fn with_allow_zero_value_genesis_tokens(mut self, allow_multiple: bool) -> Self {
        self.allow_zero_value_genesis_tokens = allow_multiple;
        self
    }

    #[must_use]
    pub fn with_test_context(mut self, test_context: Option<String>) -> Self {
        self.test_context = test_context;
        self
    }

    #[must_use]
    pub const fn with_genesis_time(mut self, genesis_time: GenesisTime) -> Self {
        self.requested_genesis_time = Some(genesis_time);
        self
    }

    /// Returns the genesis time resolved while building this deployment.
    #[must_use]
    pub const fn genesis_time(&self) -> GenesisTime {
        self.genesis_time
            .expect("genesis time is available only on a built deployment")
    }

    #[must_use]
    pub const fn with_node_binary_profile(
        mut self,
        node_binary_profile: NodeBinaryProfile,
    ) -> Self {
        self.node_binary_profile = node_binary_profile;
        self.node_binary_profile_resolved = false;
        self
    }

    /// Sets a profile that has already been resolved for this deployment.
    ///
    /// Scenario runners use this after preflight so the selected profile is
    /// carried unchanged into local node startup.
    #[must_use]
    pub const fn with_resolved_node_binary_profile(
        mut self,
        node_binary_profile: NodeBinaryProfile,
    ) -> Self {
        self.node_binary_profile = node_binary_profile;
        self.node_binary_profile_resolved = true;
        self
    }

    /// Returns the effective node binary profile retained by this topology.
    #[must_use]
    pub const fn node_binary_profile(&self) -> &NodeBinaryProfile {
        &self.node_binary_profile
    }

    fn resolve_node_binary_profile(&mut self) {
        if !self.node_binary_profile_resolved {
            self.node_binary_profile =
                NodeBinaryProfile::from_env_or(self.node_binary_profile.clone());
            self.node_binary_profile_resolved = true;
        }
    }

    #[must_use]
    pub fn empty() -> Self {
        Self::with_node_count(0)
    }

    #[must_use]
    pub fn with_node_numbers(nodes: usize) -> Self {
        Self::with_node_count(nodes)
    }

    #[must_use]
    pub const fn with_blend_core_nodes(mut self, blend_core_nodes: usize) -> Self {
        self.blend_core_nodes = blend_core_nodes;
        self
    }

    #[must_use]
    pub const fn with_sdp_funding_config(mut self, sdp_funding_config: SdpFundingConfig) -> Self {
        self.sdp_funding_config = sdp_funding_config;
        self
    }

    #[must_use]
    pub fn node_config_override(&self, index: usize) -> Option<&RunConfig> {
        self.node_config_overrides.get(&index)
    }

    /// A real-epoch era fixture with 1-second slots and reduced uncle W.
    /// Every declared era keeps exactly the same ruleset.
    #[must_use]
    pub const fn with_era_boundary_schedule(mut self, schedule: EraBoundarySchedule) -> Self {
        self.era_boundary_schedule = Some(schedule);
        self.security_param = NonZeroU32::new(8).unwrap();
        self.active_slot_coeff = NonNegativeRatio::new(1, NonZeroU32::new(2).unwrap());
        self
    }

    pub(crate) fn apply_deployment_overrides(&self, settings: &mut DeploymentSettings) {
        settings
            .update_genesis_era_parameters(|parameters| {
                let cryptarchia = &mut parameters.cryptarchia;
                cryptarchia.security_param = self.security_param;
                cryptarchia.slot_activation_coeff = self
                    .active_slot_coeff
                    .try_into()
                    .expect("The topology's slot activation coefficient must be valid.");
                // A lower `k` narrows the widest uncle reference window the era
                // allows.
                cryptarchia.uncle_reference_window_in_block = cryptarchia
                    .uncle_reference_window_in_block
                    .min(cryptarchia.widest_uncle_reference_window());
                if self.era_boundary_schedule.is_some() {
                    cryptarchia.uncle_reference_window_in_block =
                        cryptarchia.uncle_reference_window_in_block.min(1);
                    parameters.time.slot_duration_in_seconds = NonZero::new(1).unwrap();
                }
            })
            .expect("The changed deployment must be valid.");
        if let Some(schedule) = self.era_boundary_schedule {
            let ruleset = EraRuleset::V1(settings.genesis_era_parameters().clone());
            let declarations = (0..=schedule.target_era)
                .map(|era| {
                    let first_epoch = if era == schedule.target_era {
                        schedule.activation_epoch
                    } else {
                        era
                    };
                    (Epoch::new(first_epoch), ruleset.clone())
                })
                .collect::<BTreeMap<_, _>>();
            let declarations = EraDeclarations::try_from(declarations)
                .expect("The era-boundary test schedule must be well-formed");
            *settings = DeploymentSettings::new(&declarations, settings.genesis_block().clone())
                .expect("The accelerated era-boundary deployment must validate");
        }
    }
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            n_nodes: 0,
            blend_core_nodes: 0,
            sdp_funding_config: SdpFundingConfig::default(),
            network_params: Arc::new(NetworkParams::default()),
            wallet_config: WalletConfig::default(),
            scenario_base_dir: std::env::temp_dir(),
            genesis_block: None,
            requested_genesis_time: None,
            genesis_time: None,
            slot_duration: Some(Duration::from_secs(DEFAULT_SLOT_TIME_IN_SECS)),
            active_slot_coeff: DEFAULT_ACTIVE_SLOT_COEFF,
            security_param: DEFAULT_SECURITY_PARAM,
            era_boundary_schedule: None,
            node_config_overrides: HashMap::new(),
            allow_multiple_genesis_tokens: false,
            allow_zero_value_genesis_tokens: false,
            test_context: None,
            node_binary_profile: NodeBinaryProfile::default(),
            node_binary_profile_resolved: false,
        }
    }
}

/// Deployment-facing builder.
#[derive(Clone)]
pub struct DeploymentBuilder {
    config: TopologyConfig,
    seed: Option<DeploymentSeed>,
    blend_ports: Vec<u16>,
}

impl DeploymentBuilder {
    #[must_use]
    pub const fn new(config: TopologyConfig) -> Self {
        Self {
            config,
            seed: None,
            blend_ports: Vec::new(),
        }
    }

    /// Marks the already-resolved profile as launch metadata for this
    /// deployment.
    #[must_use]
    pub fn with_resolved_node_binary_profile(
        mut self,
        node_binary_profile: NodeBinaryProfile,
    ) -> Self {
        self.config = self
            .config
            .with_resolved_node_binary_profile(node_binary_profile);
        self
    }

    #[must_use]
    pub const fn with_deployment_seed(mut self, seed: DeploymentSeed) -> Self {
        self.seed = Some(seed);
        self
    }

    /// Gives the nodes these Blend ports, one per node, instead of newly
    /// allocated ones. The genesis block declares each node's Blend port, so a
    /// deployment rebuilt with the same seed and ports has the same genesis
    /// block.
    #[must_use]
    pub fn with_blend_ports(mut self, ports: Vec<u16>) -> Self {
        self.blend_ports = ports;
        self
    }

    #[must_use]
    pub const fn with_node_count(mut self, nodes: usize) -> Self {
        self.config.n_nodes = nodes;
        self
    }

    #[must_use]
    pub const fn nodes(self, nodes: usize) -> Self {
        self.with_node_count(nodes)
    }

    #[must_use]
    pub fn scenario_base_dir(mut self, dir: PathBuf) -> Self {
        self.config.scenario_base_dir = dir;
        self
    }

    #[must_use]
    pub fn with_network_layout(mut self, layout: Libp2pNetworkLayout) -> Self {
        self.config.network_params = Arc::new(NetworkParams {
            libp2p_network_layout: layout,
        });
        self
    }

    #[must_use]
    pub fn with_wallet_config(mut self, wallet: WalletConfig) -> Self {
        self.config.wallet_config = wallet;
        self
    }

    /// Overrides the node deployment's Cryptarchia security parameter.
    #[must_use]
    pub const fn with_security_param(mut self, security_param: NonZeroU32) -> Self {
        self.config.security_param = security_param;
        self
    }

    /// Overrides the node deployment's Cryptarchia slot activation coefficient.
    #[must_use]
    pub const fn with_slot_activation_coeff(
        mut self,
        numerator: u32,
        denominator: NonZeroU32,
    ) -> Self {
        self.config.active_slot_coeff = NonNegativeRatio::new(numerator, denominator);
        self
    }

    #[must_use]
    pub fn with_test_context(mut self, test_context: &str) -> Self {
        self.config.test_context = Some(test_context.to_owned());
        self
    }

    #[must_use]
    pub const fn with_genesis_time(mut self, genesis_time: GenesisTime) -> Self {
        self.config.requested_genesis_time = Some(genesis_time);
        self
    }

    pub fn build(self) -> Result<DeploymentPlan, TopologyBuildError> {
        super::build_plan(self.prepare()?)
    }

    /// Prepares the shared network before constructing any Logos service
    /// config. Other implementations can consume these inputs without
    /// building a Logos plan.
    pub fn prepare(mut self) -> Result<PreparedDeployment, TopologyBuildError> {
        self.config.resolve_node_binary_profile();
        let genesis_time = self
            .config
            .requested_genesis_time
            .unwrap_or_else(resolve_automatic_genesis_time);
        self.config.genesis_time = Some(genesis_time);

        self.config.wallet_config.validate(
            self.config.allow_multiple_genesis_tokens,
            self.config.allow_zero_value_genesis_tokens,
        )?;

        let node_count = self.config.n_nodes;
        if node_count == 0 {
            return Ok(PreparedDeployment {
                config: self.config,
                network: None,
            });
        }

        assert!(
            self.config.blend_core_nodes <= node_count,
            "blend_core_nodes({}) must be <= n_nodes({node_count})",
            self.config.blend_core_nodes
        );

        let ids = generate_node_ids(node_count, self.seed.as_ref());

        let wallet_accounts = self
            .config
            .wallet_config
            .accounts
            .iter()
            .map(|account| (account.secret_key.clone(), account.value))
            .collect::<Vec<_>>();

        let blend_ports = if self.blend_ports.is_empty() {
            allocate_blend_ports(node_count)?
        } else {
            self.blend_ports.clone()
        };
        let network = prepare_network(
            &ids,
            &blend_ports,
            self.config.blend_core_nodes,
            wallet_accounts.len(),
            &wallet_accounts,
            self.config.sdp_funding_config,
            self.config.test_context.as_deref(),
            genesis_time,
        );
        self.config.genesis_block = Some(network.genesis.clone());

        Ok(PreparedDeployment {
            config: self.config,
            network: Some(network),
        })
    }
}

fn allocate_blend_ports(node_count: usize) -> Result<Vec<u16>, TopologyBuildError> {
    let mut ports = Vec::with_capacity(node_count);

    for _ in 0..node_count {
        let Some(port) = get_reserved_available_udp_port() else {
            return Err(TopologyBuildError::BlendPortAllocation);
        };
        ports.push(port);
    }

    Ok(ports)
}

fn generate_node_ids(node_count: usize, seed: Option<&DeploymentSeed>) -> Vec<[u8; 32]> {
    let mut ids = vec![[0; 32]; node_count];
    if let Some(seed) = seed {
        let mut rng = rand::rngs::StdRng::from_seed(*seed.bytes());
        fill_node_ids(&mut ids, &mut rng);
        return ids;
    }

    let mut rng = rand::thread_rng();
    fill_node_ids(&mut ids, &mut rng);

    ids
}

fn fill_node_ids<R>(ids: &mut [[u8; 32]], rng: &mut R)
where
    R: Rng + ?Sized,
{
    for id in ids {
        rng.fill(id);
    }
}

impl DeploymentProvider<DeploymentPlan> for DeploymentBuilder {
    fn build(&self, seed: Option<&DeploymentSeed>) -> Result<DeploymentPlan, DynTopologyError> {
        let builder = seed.map_or_else(
            || self.clone(),
            |seed| self.clone().with_deployment_seed(seed.clone()),
        );

        builder
            .build()
            .map_err(|error| Box::new(error) as DynTopologyError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_binary_profile_environment_override_matches_startup_selection() {
        let cases = [
            (NodeBinaryProfile::Normal, None, NodeBinaryProfile::Normal),
            (
                NodeBinaryProfile::TokioConsole,
                None,
                NodeBinaryProfile::TokioConsole,
            ),
            (
                NodeBinaryProfile::TokioConsole,
                Some("normal"),
                NodeBinaryProfile::Normal,
            ),
            (
                NodeBinaryProfile::Normal,
                Some("tokio-console"),
                NodeBinaryProfile::TokioConsole,
            ),
        ];

        for (configured, environment_override, expected) in cases {
            assert_eq!(
                NodeBinaryProfile::resolve(configured, environment_override),
                expected
            );
        }
    }

    #[test]
    fn sequential_prepared_deployments_keep_their_resolved_profiles() {
        let parent_profile = std::env::var_os(NODE_BINARY_PROFILE);
        {
            let first = DeploymentBuilder::new(
                TopologyConfig::empty()
                    .with_resolved_node_binary_profile(NodeBinaryProfile::TokioConsole),
            )
            .prepare()
            .expect("Tokio-console deployment should prepare");

            assert_eq!(
                first.config().node_binary_profile(),
                &NodeBinaryProfile::TokioConsole
            );
        };

        let second = DeploymentBuilder::new(
            TopologyConfig::empty().with_resolved_node_binary_profile(NodeBinaryProfile::Normal),
        )
        .prepare()
        .expect("normal deployment should prepare");

        assert_eq!(
            second.config().node_binary_profile(),
            &NodeBinaryProfile::Normal
        );
        assert_eq!(std::env::var_os(NODE_BINARY_PROFILE), parent_profile);
    }

    #[test]
    fn direct_topology_profile_resolves_the_environment_override_during_preparation() {
        let configured_profile = NodeBinaryProfile::TokioConsole;
        let expected = NodeBinaryProfile::from_env_or(configured_profile.clone());
        let prepared = DeploymentBuilder::new(
            TopologyConfig::empty().with_node_binary_profile(configured_profile),
        )
        .prepare()
        .expect("deployment should prepare");

        assert_eq!(prepared.config().node_binary_profile(), &expected);
    }

    #[test]
    fn automatic_genesis_times_are_unique_within_a_process() {
        let first = DeploymentBuilder::new(TopologyConfig::empty())
            .build()
            .expect("first deployment should build");
        let second = DeploymentBuilder::new(TopologyConfig::empty())
            .build()
            .expect("second deployment should build");

        assert_ne!(
            first.config().genesis_time(),
            second.config().genesis_time()
        );
    }

    #[test]
    fn automatic_genesis_time_respects_a_future_delay() {
        let before = time::OffsetDateTime::now_utc().unix_timestamp();
        let delay = 10;
        let genesis_time = resolve_automatic_genesis_time_after(delay);

        assert!(time::OffsetDateTime::from(genesis_time).unix_timestamp() >= before + delay as i64);
    }

    #[test]
    fn explicit_genesis_times_can_be_shared() {
        let genesis_time = GenesisTime::new(1_000);
        let first = DeploymentBuilder::new(TopologyConfig::empty())
            .with_genesis_time(genesis_time)
            .build()
            .expect("first deployment should build");
        let second = DeploymentBuilder::new(TopologyConfig::empty())
            .with_genesis_time(genesis_time)
            .build()
            .expect("second deployment should build");

        assert_eq!(first.config().genesis_time(), genesis_time);
        assert_eq!(second.config().genesis_time(), genesis_time);
    }

    #[test]
    fn era_zero_to_era_one_deployment_uses_the_requested_boundary() {
        let topology = DeploymentBuilder::new(
            TopologyConfig::with_node_numbers(2).with_era_boundary_schedule(
                EraBoundarySchedule::new(0, 1, 1).expect("schedule should be valid"),
            ),
        )
        .build()
        .expect("two node fixture must build");
        let settings = super::super::deployment_settings_for_topology(
            topology.config().genesis_block.as_ref().unwrap(),
            topology.config(),
        );
        let eras = settings.era_schedule().iter().collect::<Vec<_>>();
        assert_eq!(eras.len(), 2);
        assert_eq!(eras[0].first_slot.into_inner(), 0);
        assert_eq!(eras[1].first_epoch.into_inner(), 1);
        assert_eq!(eras[1].first_slot.into_inner(), 160);
        assert_ne!(
            eras[0].parameters.fork_digest,
            eras[1].parameters.fork_digest
        );
        let era_zero_rules =
            serde_yaml::to_string(&eras[0].parameters.ruleset).expect("era rules should serialize");
        let era_one_rules =
            serde_yaml::to_string(&eras[1].parameters.ruleset).expect("era rules should serialize");
        assert_eq!(era_zero_rules, era_one_rules);
        assert_eq!(
            settings
                .genesis_era_parameters()
                .cryptarchia
                .uncle_reference_window_in_block,
            1
        );
    }

    #[test]
    fn era_one_to_era_two_deployment_keeps_the_genesis_era_declaration() {
        let topology = DeploymentBuilder::new(
            TopologyConfig::with_node_numbers(2).with_era_boundary_schedule(
                EraBoundarySchedule::new(1, 2, 3).expect("schedule should be valid"),
            ),
        )
        .build()
        .expect("two node fixture must build");
        let settings = super::super::deployment_settings_for_topology(
            topology.config().genesis_block.as_ref().unwrap(),
            topology.config(),
        );
        let eras = settings.era_schedule().iter().collect::<Vec<_>>();

        assert_eq!(eras.len(), 3);
        assert_eq!(
            eras.iter()
                .map(|era| era.first_epoch.into_inner())
                .collect::<Vec<_>>(),
            [0, 1, 3]
        );
        assert_eq!(
            eras.iter()
                .map(|era| era.first_slot.into_inner())
                .collect::<Vec<_>>(),
            [0, 160, 480]
        );
        assert_ne!(
            eras[1].parameters.fork_digest,
            eras[2].parameters.fork_digest
        );
        let era_one_rules =
            serde_yaml::to_string(&eras[1].parameters.ruleset).expect("era rules should serialize");
        let era_two_rules =
            serde_yaml::to_string(&eras[2].parameters.ruleset).expect("era rules should serialize");
        assert_eq!(era_one_rules, era_two_rules);
    }

    #[test]
    fn era_boundary_schedule_rejects_nonconsecutive_eras_and_insufficient_epochs() {
        assert!(EraBoundarySchedule::new(0, 2, 3).is_err());
        assert!(EraBoundarySchedule::new(1, 2, 1).is_err());
    }
}
