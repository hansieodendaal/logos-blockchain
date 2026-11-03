use std::{
    collections::HashMap,
    env, fs, io,
    net::{Ipv4Addr, UdpSocket},
    num::NonZeroU64,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use config::{api, mempool, sdp, state, storage, wallet};
use flate2::read::GzDecoder;
use lb_config::kms::key_id_for_preload_backend;
use lb_core::mantle;
use lb_key_management_system_service::keys::{Key, secured_key::SecuredKey as _};
use lb_libp2p::Multiaddr;
use lb_node::{
    UserConfig,
    config::{
        self, RunConfig,
        deployment::DeploymentSettings,
        sdp::serde::ActiveMessageTrackerConfig,
        tracing::serde::{
            Level,
            logger::{self, AppenderType},
        },
    },
};
use rand::Rng as _;
use testing_framework_core::scenario::{Application, DynError, PeerSelection, StartNodeOptions};
use testing_framework_runner_local::{
    BinaryProviderRef, BuildBinaryProvider, BuildCommand, DownloadBinaryProvider, DownloadChecksum,
    DownloadUrl, EnvBinaryProvider, FallbackBinaryProvider, LaunchEnvVar, LaunchFile,
    LocalBuildContext, LocalDeployerEnv, LocalPeerNode, NodeEndpointPort, NodeEndpoints,
    PathBinaryProvider, PreparedNode, ProcessSpawnError, env::Node, process::LaunchSpec,
};
use tracing::{debug, info};

use crate::{
    DeploymentPlan, LOG_LEVEL, LbcEnv, LbcNodeConfig, NodeHttpClient, NodePlan,
    config::{
        Config, Libp2pNetworkLayout, NetworkParams, create_node_config_for_node,
        deployment::TopologyConfig, deployment_settings_for_topology,
    },
    configs::deployment::NodeBinaryProfile,
    diagnostics::{
        record_system_monitor_event, register_system_monitor_output_file,
        unregister_system_monitor_output_file,
    },
    env as tf_env,
};

const LOGS_PREFIX: &str = "__logs";
const DEFAULT_BLEND_NETWORK_PORT: u16 = 3400;
/// URL of a Logos node release binary downloaded by the local TF deployer.
///
/// Use an immutable URL, or set [`LOGOS_BLOCKCHAIN_NODE_DOWNLOAD_SHA256`]. An
/// archive republished at the same URL is otherwise served from the download
/// cache without being fetched again.
pub const LOGOS_BLOCKCHAIN_NODE_DOWNLOAD_URL: &str = "LOGOS_BLOCKCHAIN_NODE_DOWNLOAD_URL";
/// Optional SHA-256 checksum for [`LOGOS_BLOCKCHAIN_NODE_DOWNLOAD_URL`].
pub const LOGOS_BLOCKCHAIN_NODE_DOWNLOAD_SHA256: &str = "LOGOS_BLOCKCHAIN_NODE_DOWNLOAD_SHA256";
/// The default filename for the user config.
pub const USER_CONFIG_FILE: &str = "node.yaml";
/// The default filename for the deployment config.
pub const DEPLOYMENT_CONFIG_FILE: &str = "deployment.yaml";

struct PlannedLocalNodeConfig {
    config: Config,
    descriptor_override: Option<RunConfig>,
    deployment_settings: DeploymentSettings,
    port_strategy: PortStrategy,
}

#[derive(Clone, Copy)]
enum PortStrategy {
    PreservePlannedPorts,
    AllocateEphemeralPorts,
}

#[async_trait]
impl LocalDeployerEnv for LbcEnv {
    fn prepare_local_cluster(topology: &Self::Deployment) {
        register_system_monitor_output_file(
            &topology
                .config()
                .scenario_base_dir
                .join("system_stats.ndjson"),
        );
    }

    fn cleanup_local_cluster(topology: &Self::Deployment) {
        unregister_system_monitor_output_file(
            &topology
                .config()
                .scenario_base_dir
                .join("system_stats.ndjson"),
        );
    }

    fn build_node_config(
        context: LocalBuildContext<'_, Self>,
    ) -> Result<PreparedNode<Self::NodeConfig>, DynError> {
        let peer_ports = context
            .peers
            .iter()
            .map(LocalPeerNode::network_port)
            .collect::<Vec<_>>();
        let peer_ports_by_name = context
            .peers
            .iter()
            .filter_map(|peer| {
                peer.name()
                    .map(|name| (name.to_owned(), peer.network_port()))
            })
            .collect();
        build_dynamic_node_config(
            context.topology,
            context.index,
            &peer_ports_by_name,
            context.options,
            &peer_ports,
            context.template_config,
        )
    }

    fn build_initial_node_configs(
        topology: &Self::Deployment,
    ) -> Result<Vec<PreparedNode<<Self as Application>::NodeConfig>>, ProcessSpawnError> {
        topology
            .nodes()
            .iter()
            .map(|node| {
                let label = format!("node-{}", node.index());
                let config = build_node_run_config(
                    topology,
                    node,
                    topology.config().node_config_override(node.index()),
                )
                .map_err(|source| ProcessSpawnError::Config { source })?;
                Ok::<_, ProcessSpawnError>(PreparedNode {
                    name: label,
                    network_port: config.user.network.backend.swarm.port,
                    config: local_node_config(topology, config),
                })
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn initial_persist_dir(
        topology: &Self::Deployment,
        node_name: &str,
        _index: usize,
    ) -> Option<PathBuf> {
        Some(topology.config().scenario_base_dir.join(node_name))
    }

    async fn build_launch_spec(
        config: &<Self as Application>::NodeConfig,
        dir: &Path,
        label: &str,
    ) -> Result<LaunchSpec, DynError> {
        let launch_config = config;
        let mut config = launch_config.run_config().clone();

        record_system_monitor_event(
            "node_runtime_prepared",
            format!("{label}:{}", dir.display()),
        );

        if let Some(level) = configured_node_log_level() {
            config.user.tracing.level = level;
        }

        if !tf_env::debug_tracing() {
            let log_prefix = format!("{LOGS_PREFIX}-{label}");
            config.user.tracing.logger = configure_logging(dir, &log_prefix);
        }

        config.user.state.base_folder = dir.to_path_buf();
        "db".clone_into(&mut config.user.storage.backend.folder_name);

        if let config::tracing::serde::console::Layer::Console(console) =
            &mut config.user.tracing.console
            && let Some(recording_path) = &mut console.recording_path
        {
            if recording_path.is_relative() {
                let relative_path = recording_path.clone();
                *recording_path = dir.join(relative_path);
            }
            if let Some(parent) = recording_path.parent() {
                fs::create_dir_all(parent).map_err(|source| {
                    io::Error::other(format!(
                        "failed to prepare Tokio console recording path `{}`: {source}",
                        recording_path.display()
                    ))
                })?;
            }
        }

        let user_yaml = serde_yaml::to_string(&config.user).map_err(io::Error::other)?;
        let deployment_yaml =
            serde_yaml::to_string(&config.deployment).map_err(io::Error::other)?;

        build_node_launch_spec(
            dir,
            user_yaml,
            deployment_yaml,
            launch_config,
            node_binary_provider,
        )
        .await
    }

    fn node_endpoints(
        config: &<Self as Application>::NodeConfig,
    ) -> Result<NodeEndpoints, DynError> {
        let mut endpoints = NodeEndpoints {
            api: config.user.api.backend.listen_address,
            ..Default::default()
        };

        add_endpoint_ports(&mut endpoints, config);

        Ok(endpoints)
    }

    fn node_client(endpoints: &NodeEndpoints) -> Result<Self::NodeClient, DynError> {
        Ok(NodeHttpClient::new(endpoints.api))
    }

    async fn wait_readiness_stable(nodes: &[Node<Self>]) -> Result<(), DynError> {
        super::readiness::wait_readiness_stable(nodes).await
    }
}

fn add_endpoint_ports(endpoints: &mut NodeEndpoints, config: &RunConfig) {
    endpoints.insert_port(
        NodeEndpointPort::Network,
        config.user.network.backend.swarm.port,
    );
}

fn allocate_udp_port(label: &'static str) -> Result<u16, DynError> {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).map_err(|source| {
        io::Error::other(format!("failed to allocate {label} UDP port: {source}"))
    })?;

    socket
        .local_addr()
        .map(|addr| addr.port())
        .map_err(|source| {
            io::Error::other(format!("failed to read {label} UDP port: {source}")).into()
        })
}

async fn build_node_launch_spec(
    dir: &Path,
    user_yaml: String,
    deployment_yaml: String,
    node_config: &LbcNodeConfig,
    provider_for_profile: impl FnOnce(&NodeBinaryProfile) -> Result<BinaryProviderRef, DynError>,
) -> Result<LaunchSpec, DynError> {
    let config_path = dir.join(USER_CONFIG_FILE);
    let deployment_path = dir.join(DEPLOYMENT_CONFIG_FILE);
    let time_backend =
        env::var("LOGOS_BLOCKCHAIN_TIME_BACKEND").unwrap_or_else(|_| "monotonic".to_owned());
    let node_binary_profile = node_config.node_binary_profile();
    let provider = provider_for_profile(node_binary_profile)?;
    let binary = resolve_node_binary_with_provider(provider).await?;
    info!(
        profile = node_binary_profile.to_string(),
        node_binary = %binary.display(),
        "Resolved node binary for node startup"
    );

    Ok(LaunchSpec {
        binary,
        files: vec![
            launch_file(USER_CONFIG_FILE, user_yaml.into_bytes()),
            launch_file(DEPLOYMENT_CONFIG_FILE, deployment_yaml.into_bytes()),
        ],
        args: vec![
            config_path.to_string_lossy().to_string(),
            "--deployment".to_owned(),
            deployment_path.to_string_lossy().to_string(),
        ],
        env: vec![LaunchEnvVar::new(
            "LOGOS_BLOCKCHAIN_TIME_BACKEND",
            time_backend,
        )],
    })
}

fn inherited_rustflags() -> Result<Vec<String>, DynError> {
    let rustflags = unicode_environment_value("RUSTFLAGS")?;
    let encoded_rustflags = unicode_environment_value("CARGO_ENCODED_RUSTFLAGS")?;

    Ok(effective_rustflags_from(
        rustflags.as_deref(),
        encoded_rustflags.as_deref(),
    ))
}

fn unicode_environment_value(name: &str) -> Result<Option<String>, DynError> {
    env::var_os(name)
        .map(|value| {
            value.into_string().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{name} must contain valid UTF-8 to build the Tokio-console binary"),
                )
                .into()
            })
        })
        .transpose()
}

fn effective_rustflags_from(
    rustflags: Option<&str>,
    encoded_rustflags: Option<&str>,
) -> Vec<String> {
    if let Some(encoded) = encoded_rustflags {
        return if encoded.is_empty() {
            Vec::new()
        } else {
            encoded.split('\u{1f}').map(str::to_owned).collect()
        };
    }

    rustflags
        .map(|flags| flags.split_whitespace().map(str::to_owned).collect())
        .unwrap_or_default()
}

fn rustflags_with_tokio_unstable_from(current: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut flags = Vec::new();
    let mut found_tokio_unstable = false;
    let mut iter = current.into_iter().peekable();

    while let Some(flag) = iter.next() {
        if flag == "--cfg" && iter.peek().is_some_and(|value| value == "tokio_unstable") {
            let cfg = iter.next().expect("peeked cfg value should exist");
            if !found_tokio_unstable {
                flags.extend([flag, cfg]);
                found_tokio_unstable = true;
            }
        } else if flag == "--cfg=tokio_unstable" {
            if !found_tokio_unstable {
                flags.push(flag);
                found_tokio_unstable = true;
            }
        } else {
            flags.push(flag);
        }
    }

    if !found_tokio_unstable {
        flags.extend(["--cfg".to_owned(), "tokio_unstable".to_owned()]);
    }

    flags
}

fn launch_file(relative_path: &str, contents: Vec<u8>) -> LaunchFile {
    LaunchFile {
        relative_path: PathBuf::from(relative_path),
        contents,
    }
}

/// Resolves (building if necessary) the node binary for `node_binary_profile`
/// and populates the process-local binary cache.
pub async fn ensure_node_binary_built(
    node_binary_profile: &NodeBinaryProfile,
) -> Result<PathBuf, DynError> {
    resolve_node_binary_for_profile(node_binary_profile).await
}

async fn resolve_node_binary_for_profile(
    node_binary_profile: &NodeBinaryProfile,
) -> Result<PathBuf, DynError> {
    let provider = node_binary_provider(node_binary_profile)?;
    resolve_node_binary_with_provider(provider).await
}

async fn resolve_node_binary_with_provider(
    provider: BinaryProviderRef,
) -> Result<PathBuf, DynError> {
    provider.resolve().await.map_err(Into::into)
}

fn node_binary_provider(
    node_binary_profile: &NodeBinaryProfile,
) -> Result<BinaryProviderRef, DynError> {
    let release_download_requested =
        env::var_os(LOGOS_BLOCKCHAIN_NODE_DOWNLOAD_URL).is_some_and(|url| !url.is_empty());
    validate_node_binary_selection(node_binary_profile, release_download_requested)?;

    let mut providers: Vec<BinaryProviderRef> = vec![Arc::new(EnvBinaryProvider::new(
        "LOGOS_BLOCKCHAIN_NODE_BIN",
    ))];

    if release_download_requested {
        providers.push(Arc::new(release_binary_provider()));
    }

    providers.push(match node_binary_profile {
        NodeBinaryProfile::Normal => default_node_binary_provider(),
        NodeBinaryProfile::TokioConsole => tokio_console_node_binary_provider()?,
    });

    Ok(Arc::new(FallbackBinaryProvider::new(providers)))
}

fn validate_node_binary_selection(
    node_binary_profile: &NodeBinaryProfile,
    release_download_requested: bool,
) -> Result<(), DynError> {
    if release_download_requested && *node_binary_profile == NodeBinaryProfile::TokioConsole {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{LOGOS_BLOCKCHAIN_NODE_DOWNLOAD_URL} cannot be used with the tokio-console node binary profile"
            ),
        )
        .into());
    }

    Ok(())
}

fn release_binary_provider() -> DownloadBinaryProvider {
    DownloadBinaryProvider {
        url: DownloadUrl::Env(LOGOS_BLOCKCHAIN_NODE_DOWNLOAD_URL.to_owned()),
        sha256: Some(DownloadChecksum::Env(
            LOGOS_BLOCKCHAIN_NODE_DOWNLOAD_SHA256.to_owned(),
        )),
        cache_dir: None,
        processor: None,
    }
    .with_processor_fn("logos-node-tar-gz-v1", extract_release_binary)
}

fn extract_release_binary(artifact: &Path, output: &Path) -> Result<(), DynError> {
    let decoder = GzDecoder::new(fs::File::open(artifact)?);
    let mut archive = tar::Archive::new(decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;
        if entry
            .path()?
            .file_name()
            .is_some_and(|name| name == "logos-blockchain-node")
        {
            entry.unpack(output)?;
            return Ok(());
        }
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "release archive does not contain logos-blockchain-node",
    )
    .into())
}

fn default_node_binary_provider() -> BinaryProviderRef {
    if running_in_ci() {
        Arc::new(PathBinaryProvider::new(release_node_binary_path()))
    } else {
        Arc::new(BuildBinaryProvider {
            command: BuildCommand::new("cargo").with_args([
                "build",
                "--locked",
                "--release",
                "-p",
                "logos-blockchain-node",
                "--features",
                "testing",
            ]),
            output_path: release_node_binary_path(),
            working_dir: Some(workspace_root()),
            lock_dir: Some(workspace_root().join("target").join(".tf-binaries")),
        })
    }
}

fn release_node_binary_path() -> PathBuf {
    workspace_root()
        .join("target")
        .join("release")
        .join("logos-blockchain-node")
}

fn tokio_console_node_binary_provider() -> Result<BinaryProviderRef, DynError> {
    if running_in_ci() {
        Ok(Arc::new(
            PathBinaryProvider::new(release_node_binary_path()),
        ))
    } else {
        Ok(Arc::new(BuildBinaryProvider {
            command: tokio_console_build_command(inherited_rustflags()?),
            output_path: release_profiling_node_binary_path(),
            working_dir: Some(workspace_root()),
            lock_dir: Some(workspace_root().join("target").join(".tf-binaries")),
        }))
    }
}

fn tokio_console_build_command(rustflags: Vec<String>) -> BuildCommand {
    cargo_command_with_tokio_console_flags(
        [
            "build".to_owned(),
            "--locked".to_owned(),
            "--profile".to_owned(),
            "release-profiling".to_owned(),
            "-p".to_owned(),
            "logos-blockchain-node".to_owned(),
            "--features".to_owned(),
            "testing,tokio-console".to_owned(),
        ],
        rustflags,
    )
}

fn cargo_command_with_tokio_console_flags(
    build_args: impl IntoIterator<Item = String>,
    rustflags: Vec<String>,
) -> BuildCommand {
    let rustflags = rustflags_with_tokio_unstable_from(rustflags);
    let encoded_rustflags = rustflags.join("\u{1f}");

    BuildCommand::new("env").with_args(
        [
            format!("CARGO_ENCODED_RUSTFLAGS={encoded_rustflags}"),
            "cargo".to_owned(),
        ]
        .into_iter()
        .chain(build_args),
    )
}

fn release_profiling_node_binary_path() -> PathBuf {
    workspace_root()
        .join("target")
        .join("release-profiling")
        .join("logos-blockchain-node")
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn running_in_ci() -> bool {
    env::var_os("CI").is_some() || env::var_os("GITHUB_ACTIONS").is_some()
}

fn configure_logging(base_dir: &Path, prefix: &str) -> logger::Layers {
    debug!(prefix, base_dir = %base_dir.display(), "configuring node logging");

    if let Some(log_dir) = tf_env::logos_blockchain_log_dir() {
        match fs::create_dir_all(&log_dir) {
            Ok(()) => {
                return logger::Layers {
                    file: Some(logger::FileConfig {
                        directory: log_dir,
                        prefix: Some(prefix.into()),
                        appender_type: AppenderType::Simple,
                    }),
                    loki: None,
                    gelf: None,
                    otlp: None,
                    format: logger::LogFormat::default(),
                    stdout: false,
                    stderr: false,
                };
            }

            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to create LOGOS_BLOCKCHAIN_LOG_DIR; falling back to node dir"
                );
            }
        }
    }

    logger::Layers {
        file: Some(logger::FileConfig {
            directory: base_dir.to_owned(),
            prefix: Some(prefix.into()),
            appender_type: AppenderType::Simple,
        }),
        loki: None,
        gelf: None,
        otlp: None,
        format: logger::LogFormat::default(),
        stdout: false,
        stderr: false,
    }
}

fn configured_node_log_level() -> Option<Level> {
    env::var(LOG_LEVEL)
        .ok()
        .map(|level| level.parse::<Level>().unwrap_or(Level::INFO))
}

fn build_dynamic_node_config(
    topology: &DeploymentPlan,
    index: usize,
    peer_ports_by_name: &HashMap<String, u16>,
    options: &StartNodeOptions<LbcEnv>,
    peer_ports: &[u16],
    template_config: Option<&LbcNodeConfig>,
) -> Result<PreparedNode<LbcNodeConfig>, DynError> {
    let node_binary_profile = topology.config().node_binary_profile().clone();
    let plan = plan_local_node_config(
        topology,
        index,
        peer_ports_by_name,
        options.common.peers.as_ref(),
        peer_ports,
    )?;
    let mut config = finalize_dynamic_run_config(
        &plan,
        options
            .config_override
            .as_ref()
            .map(LbcNodeConfig::run_config),
        template_config.map(LbcNodeConfig::run_config),
    );
    let mut network_port = config.user.network.backend.swarm.port;

    match plan.port_strategy {
        PortStrategy::PreservePlannedPorts => {}
        PortStrategy::AllocateEphemeralPorts => {
            network_port = allocate_udp_port("network")?;
            let blend_port = allocate_udp_port("blend")?;
            config.user.network.backend.swarm.port = network_port;
            config.user.blend.core.backend.listening_address =
                lb_libp2p::multiaddr(Ipv4Addr::LOCALHOST, blend_port);
        }
    }

    Ok(PreparedNode {
        name: format!("node-{index}"),
        config: LbcNodeConfig::new(config, node_binary_profile),
        network_port,
    })
}

fn plan_local_node_config(
    descriptors: &DeploymentPlan,
    index: usize,
    peer_ports_by_name: &HashMap<String, u16>,
    peer_selection: Option<&PeerSelection>,
    peer_ports: &[u16],
) -> Result<PlannedLocalNodeConfig, DynError> {
    let base_node = descriptors
        .nodes()
        .first()
        .ok_or_else(|| io::Error::other("generated topology must include at least one node"))?;

    let base_consensus = &base_node.general.consensus_config;
    let base_time = &base_node.general.time_config;

    if let Some(node) = descriptors.nodes().get(index) {
        let mut config = node.general.clone();
        let initial_peers = resolve_initial_peers(
            peer_ports_by_name,
            peer_selection,
            &config.network_config.backend.initial_peers,
            descriptors,
            peer_ports,
        )?;

        config.network_config.backend.initial_peers = initial_peers;

        let genesis_block = descriptors
            .config()
            .genesis_block
            .as_ref()
            .ok_or_else(|| io::Error::other("missing topology genesis tx"))?;

        return Ok(PlannedLocalNodeConfig {
            config,
            descriptor_override: descriptors.config().node_config_override(index).cloned(),
            deployment_settings: deployment_settings_for_topology(
                genesis_block,
                descriptors.config(),
            ),
            port_strategy: PortStrategy::PreservePlannedPorts,
        });
    }

    let id = {
        let mut id = [0u8; 32];
        rand::thread_rng().fill(&mut id);
        id
    };

    let network_port = base_node.general.network_config.backend.swarm.port;
    let blend_port = DEFAULT_BLEND_NETWORK_PORT;
    let initial_peers = resolve_initial_peers(
        peer_ports_by_name,
        peer_selection,
        &[],
        descriptors,
        peer_ports,
    )?;

    let config = {
        let mut config = create_node_config_for_node(
            id,
            network_port,
            initial_peers,
            blend_port,
            base_consensus,
            base_time,
            descriptors.config.test_context.as_deref(),
            descriptors.config.genesis_time(),
        )
        .map_err(|source| -> DynError { source.into() })?;

        let keys = &mut config.kms_config.backend.keys;
        for account in &descriptors.config().wallet_config.accounts {
            let key = account.secret_key.clone().into();
            let key_id = key_id_for_preload_backend(&key);
            keys.entry(key_id).or_insert(key);
        }

        config
    };

    let genesis_block = descriptors
        .config()
        .genesis_block
        .as_ref()
        .ok_or_else(|| io::Error::other("missing topology genesis tx"))?;

    Ok(PlannedLocalNodeConfig {
        config,
        descriptor_override: descriptors.config().node_config_override(index).cloned(),
        deployment_settings: deployment_settings_for_topology(genesis_block, descriptors.config()),
        port_strategy: PortStrategy::AllocateEphemeralPorts,
    })
}

pub fn build_node_run_config(
    topology: &DeploymentPlan,
    node: &NodePlan,
    descriptor_override: Option<&RunConfig>,
) -> Result<RunConfig, DynError> {
    if let Some(override_config) = descriptor_override {
        return Ok(override_config.clone());
    }

    let genesis_block = topology
        .config()
        .genesis_block
        .clone()
        .ok_or_else(|| io::Error::other("missing topology genesis tx"))?;
    let deployment_settings = deployment_settings_for_topology(&genesis_block, topology.config());
    Ok(build_run_config(node.general.clone(), &deployment_settings))
}

fn local_node_config(topology: &DeploymentPlan, config: RunConfig) -> LbcNodeConfig {
    LbcNodeConfig::new(config, topology.config().node_binary_profile().clone())
}

fn finalize_dynamic_run_config(
    plan: &PlannedLocalNodeConfig,
    runtime_override: Option<&RunConfig>,
    template_config: Option<&RunConfig>,
) -> RunConfig {
    if let Some(override_config) = runtime_override {
        return override_config.clone();
    }

    if let Some(template_config) = template_config {
        return template_config.clone();
    }

    if let Some(override_config) = &plan.descriptor_override {
        return override_config.clone();
    }

    build_run_config(plan.config.clone(), &plan.deployment_settings)
}

fn build_run_config(config: Config, deployment_settings: &DeploymentSettings) -> RunConfig {
    let mut tracing = config.tracing_config.tracing_settings;
    tracing.level = Level::INFO;

    let user_config = UserConfig {
        network: config.network_config,
        blend: config.blend_config.0,
        time: config.time_config,
        cryptarchia: build_cryptarchia_user_config(&config.consensus_config),
        tracing,
        api: api::serde::Config {
            backend: api::serde::AxumBackendSettings {
                listen_address: config.api_config.address,
                max_concurrent_requests: 1000,
                ..Default::default()
            },
        },
        storage: storage::serde::Config::default(),
        sdp: sdp::serde::Config {
            declaration: config.sdp_config.declaration.map(Into::into),
            wallet: sdp::serde::WalletConfig {
                max_tx_fee: mantle::Value::MAX.into(),
                funding_pk: config.consensus_config.funding_sk.as_public_key(),
            },
            active_message_tracker: ActiveMessageTrackerConfig {
                status_check_interval_in_tip_changes: NonZeroU64::new(3).unwrap(),
            },
        },
        wallet: {
            let known_keys: HashMap<_, _> = [
                (
                    key_id_for_preload_backend(&Key::Zk(config.consensus_config.known_key.clone())),
                    config.consensus_config.known_key.as_public_key(),
                ),
                (
                    key_id_for_preload_backend(&Key::Zk(
                        config.consensus_config.funding_sk.clone(),
                    )),
                    config.consensus_config.funding_sk.as_public_key(),
                ),
            ]
            .into_iter()
            .chain(config.consensus_config.other_keys.iter().map(|sk| {
                (
                    key_id_for_preload_backend(&sk.clone().into()),
                    sk.as_public_key(),
                )
            }))
            .chain(
                config
                    .kms_config
                    .backend
                    .keys
                    .values()
                    .filter_map(|key| match key {
                        Key::Zk(sk) => Some((
                            key_id_for_preload_backend(&Key::Zk(sk.clone())),
                            sk.as_public_key(),
                        )),
                        Key::Ed25519(_) => None,
                    }),
            )
            .collect();

            wallet::serde::Config {
                known_keys,
                ..wallet::serde::Config::with_required_values(wallet::serde::RequiredValues {
                    voucher_master_key_id: key_id_for_preload_backend(&Key::Zk(
                        config.consensus_config.known_key.clone(),
                    )),
                })
            }
        },
        mempool: mempool::serde::Config::default(),
        kms: config::kms::serde::Config {
            backend: config::kms::serde::PreloadKmsBackendSettings {
                keys: config.kms_config.backend.keys,
            },
            ..config::kms::serde::Config::default()
        },
        // Mining defaults, auto-claim off: provisioned nodes claim on demand,
        // naming the destination key on each claim request.
        pow: config::pow::serde::Config::default(),
        state: state::Config::default(),
    };

    RunConfig {
        deployment: deployment_settings.clone(),
        user: user_config,
    }
}

fn build_cryptarchia_user_config(
    consensus: &crate::config::node_configs::consensus::GeneralConsensusConfig,
) -> config::cryptarchia::serde::Config {
    use std::{collections::HashSet, num::NonZeroUsize, time::Duration};

    use config::cryptarchia::serde::{
        Config as CryptarchiaConfig, leader, leader::Config as LeaderConfig, network,
        network::Config as NetworkConfig, service, service::Config as ServiceConfig,
    };

    CryptarchiaConfig {
        network: NetworkConfig {
            bootstrap: network::BootstrapConfig {
                ibd: network::IbdConfig {
                    peers: HashSet::new(),
                    delay_before_new_download: Duration::from_secs(10),
                    tips_fetch_max_attempts: 3,
                    tips_fetch_min_delay: Duration::from_millis(250),
                    tips_fetch_max_delay: Duration::from_secs(1),
                    round_delay: Duration::from_secs(1),
                },
            },
            network: network::NetworkConfig {
                max_connected_peers_to_try_download: 16,
                max_discovered_peers_to_try_download: 16,
            },
            sync: network::SyncConfig {
                orphan: network::OrphanConfig {
                    max_orphan_cache_size: NonZeroUsize::new(1000)
                        .expect("max orphan cache size must be non-zero"),
                    max_rejected_cache_size: 1000,
                },
                tip_poll: network::TipPollConfig::default(),
            },
            seen_items_subscription_channel_size: NonZeroU64::new(64)
                .expect("received proposals buffer must be non-zero"),
        },
        service: ServiceConfig {
            bootstrap: service::BootstrapConfig {
                force_bootstrap: false,
                offline_grace_period: service::OfflineGracePeriodConfig {
                    grace_period: Duration::from_mins(20),
                    state_recording_interval: Duration::from_mins(1),
                },
                prolonged_bootstrap_period: consensus.prolonged_bootstrap_period,
            },
            sync: service::SyncConfig {
                block_provider: service::BlockProviderConfig {
                    batch_size: NonZeroUsize::new(1000)
                        .expect("block_provider batch_size must be non-zero"),
                },
            },
        },
        leader: LeaderConfig {
            wallet: leader::WalletConfig {
                max_tx_fee: mantle::Value::MAX.into(),
                funding_pk: consensus.funding_pk,
            },
        },
    }
}

fn resolve_initial_peers(
    peer_ports_by_name: &HashMap<String, u16>,
    peer_selection: Option<&PeerSelection>,
    default_peers: &[Multiaddr],
    descriptors: &DeploymentPlan,
    peer_ports: &[u16],
) -> Result<Vec<Multiaddr>, DynError> {
    match peer_selection {
        Some(PeerSelection::Named(names)) => {
            let mut peers = Vec::with_capacity(names.len());
            for name in names {
                let port = peer_ports_by_name
                    .get(name)
                    .ok_or_else(|| io::Error::other(format!("unknown peer name '{name}'")))?;
                peers.push(lb_libp2p::multiaddr(Ipv4Addr::LOCALHOST, *port));
            }

            Ok(peers)
        }
        None | Some(PeerSelection::DefaultLayout) => {
            if default_peers.is_empty() {
                let topology: &TopologyConfig = descriptors.config();
                Ok(initial_peers_for_dynamic_node(
                    topology.network_params.as_ref(),
                    peer_ports,
                ))
            } else {
                Ok(default_peers.to_vec())
            }
        }
        Some(PeerSelection::None) => Ok(Vec::new()),
    }
}

fn initial_peers_for_dynamic_node(
    network_params: &NetworkParams,
    peer_ports: &[u16],
) -> Vec<Multiaddr> {
    match network_params.libp2p_network_layout {
        Libp2pNetworkLayout::Star => peer_ports
            .first()
            .map(|port| vec![lb_libp2p::multiaddr(Ipv4Addr::LOCALHOST, *port)])
            .unwrap_or_default(),
        Libp2pNetworkLayout::Chain => peer_ports
            .last()
            .map(|port| vec![lb_libp2p::multiaddr(Ipv4Addr::LOCALHOST, *port)])
            .unwrap_or_default(),
        Libp2pNetworkLayout::Full => peer_ports
            .iter()
            .map(|port| lb_libp2p::multiaddr(Ipv4Addr::LOCALHOST, *port))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use std::{process::Command as StdCommand, sync::Mutex, time::Duration};

    use async_trait::async_trait;
    use flate2::{Compression, write::GzEncoder};
    use testing_framework_runner_local::BinaryProvider;

    use super::*;
    use crate::config::deployment::{DeploymentBuilder, NODE_BINARY_PROFILE};

    #[test]
    fn release_download_rejects_tokio_console_profile() {
        let error = validate_node_binary_selection(&NodeBinaryProfile::TokioConsole, true)
            .expect_err("downloaded releases do not include tokio-console support");

        assert_eq!(
            error.downcast_ref::<io::Error>().map(io::Error::kind),
            Some(io::ErrorKind::InvalidInput)
        );
        assert!(
            error
                .to_string()
                .contains(LOGOS_BLOCKCHAIN_NODE_DOWNLOAD_URL)
        );
    }

    #[test]
    fn tokio_console_build_flags_preserve_existing_flags_and_enable_unstable_apis() {
        assert_eq!(
            rustflags_with_tokio_unstable_from(Vec::new()),
            ["--cfg", "tokio_unstable"]
        );
        assert_eq!(
            rustflags_with_tokio_unstable_from(["-C".to_owned(), "opt-level=1".to_owned(),]),
            ["-C", "opt-level=1", "--cfg", "tokio_unstable"]
        );
        assert_eq!(
            rustflags_with_tokio_unstable_from([
                "--cfg".to_owned(),
                "tokio_unstable".to_owned(),
                "-C".to_owned(),
                "opt-level=1".to_owned(),
                "--cfg".to_owned(),
                "tokio_unstable".to_owned(),
            ]),
            ["--cfg", "tokio_unstable", "-C", "opt-level=1"]
        );
        assert_eq!(
            rustflags_with_tokio_unstable_from([
                "--cfg=tokio_unstable".to_owned(),
                "--cfg".to_owned(),
                "tokio_unstable".to_owned(),
            ]),
            ["--cfg=tokio_unstable"]
        );
        assert_eq!(
            cfg_flag_count(
                &rustflags_with_tokio_unstable_from([
                    "--cfg".to_owned(),
                    "tokio_unstable".to_owned(),
                    "--cfg=tokio_unstable".to_owned(),
                ]),
                "tokio_unstable"
            ),
            1
        );
        assert_eq!(
            effective_rustflags_from(Some("--cfg plain_user"), Some("--cfg\u{1f}encoded_user")),
            ["--cfg", "encoded_user"],
            "Cargo gives CARGO_ENCODED_RUSTFLAGS precedence over RUSTFLAGS"
        );
    }

    #[test]
    fn tokio_console_build_cache_identity_includes_effective_flags() {
        let first = BuildBinaryProvider {
            command: cargo_command_with_tokio_console_flags(
                ["build".to_owned()],
                vec!["--cfg".to_owned(), "first_profile_flag".to_owned()],
            ),
            output_path: PathBuf::from("target/release-profiling/logos-blockchain-node"),
            working_dir: Some(PathBuf::from("/workspace")),
            lock_dir: None,
        };
        let second = BuildBinaryProvider {
            command: cargo_command_with_tokio_console_flags(
                ["build".to_owned()],
                vec!["--cfg".to_owned(), "second_profile_flag".to_owned()],
            ),
            output_path: first.output_path.clone(),
            working_dir: first.working_dir.clone(),
            lock_dir: None,
        };

        assert_ne!(
            first.cache_key(),
            second.cache_key(),
            "effective Rust flags must distinguish cached build providers"
        );
    }

    #[tokio::test]
    async fn tokio_console_cargo_build_preserves_flag_precedence_and_encoded_boundaries() {
        let parent_profile = env::var_os(NODE_BINARY_PROFILE);
        let parent_rustflags = env::var_os("RUSTFLAGS");
        let parent_encoded_rustflags = env::var_os("CARGO_ENCODED_RUSTFLAGS");
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let fixture_dir = temp_dir.path().join("fixture");
        write_profile_build_fixture(&fixture_dir);
        let lock_dir = temp_dir.path().join("locks");
        let cases = [
            (
                "rustflags-only",
                Some("-C opt-level=1 --cfg profile_user_flag"),
                None,
                "true:true:false:false:false",
            ),
            (
                "encoded-only",
                None,
                Some("--cfg\u{1f}encoded_user\u{1f}--cfg=encoded_space=\"two words\""),
                "true:false:true:false:true",
            ),
            (
                "both-variables",
                Some("--cfg plain_user --cfg profile_user_flag"),
                Some("--cfg\u{1f}encoded_user"),
                "true:false:true:false:false",
            ),
            (
                "duplicate-tokio-cfg",
                None,
                Some(
                    "--cfg\u{1f}tokio_unstable\u{1f}--cfg=tokio_unstable\u{1f}--cfg\u{1f}encoded_user",
                ),
                "true:false:true:false:false",
            ),
        ];

        for (name, rustflags, encoded_rustflags, expected) in cases {
            let target_dir = temp_dir.path().join(format!("target-{name}"));
            let output_path = fixture_binary_path(&target_dir);
            let build_args = fixture_build_args(&fixture_dir, &target_dir);
            let provider = BuildBinaryProvider {
                command: fixture_build_command(build_args, rustflags, encoded_rustflags, true),
                output_path,
                working_dir: Some(fixture_dir.clone()),
                lock_dir: Some(lock_dir.clone()),
            };

            let binary = resolve_node_binary_with_provider(Arc::new(provider))
                .await
                .unwrap_or_else(|error| panic!("{name} Cargo build should succeed: {error}"));

            assert_eq!(
                run_fixture_binary(&binary),
                expected,
                "Cargo should compile the effective flags for {name}"
            );
        }

        assert_eq!(env::var_os(NODE_BINARY_PROFILE), parent_profile);
        assert_eq!(env::var_os("RUSTFLAGS"), parent_rustflags);
        assert_eq!(
            env::var_os("CARGO_ENCODED_RUSTFLAGS"),
            parent_encoded_rustflags
        );
    }

    #[tokio::test]
    async fn scenario_preflight_profile_reaches_local_startup_unchanged() {
        let parent_profile = env::var_os(NODE_BINARY_PROFILE);
        let parent_rustflags = env::var_os("RUSTFLAGS");
        let tokio_config = config_for_profile(NodeBinaryProfile::TokioConsole);
        assert_eq!(
            tokio_config.node_binary_profile(),
            &NodeBinaryProfile::TokioConsole
        );

        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let tokio_binary = temp_dir.path().join("tokio-console-node");
        let normal_binary = temp_dir.path().join("normal-node");
        fs::write(&tokio_binary, b"tokio node").expect("fixture binary should be written");
        fs::write(&normal_binary, b"normal node").expect("fixture binary should be written");
        let expected_tokio_binary = tokio_binary.clone();
        let selected_profile = Arc::new(Mutex::new(None));
        let observed_profile = Arc::clone(&selected_profile);
        let spec = build_node_launch_spec(
            temp_dir.path(),
            "user config".to_owned(),
            "deployment config".to_owned(),
            &tokio_config,
            move |profile| {
                *observed_profile
                    .lock()
                    .expect("profile test mutex should not be poisoned") = Some(profile.clone());
                let binary = match profile {
                    NodeBinaryProfile::Normal => normal_binary,
                    NodeBinaryProfile::TokioConsole => tokio_binary,
                };
                Ok(Arc::new(PathBinaryProvider::new(binary)))
            },
        )
        .await
        .expect("launch spec should resolve the selected node binary");

        assert_eq!(
            *selected_profile
                .lock()
                .expect("profile test mutex should not be poisoned"),
            Some(NodeBinaryProfile::TokioConsole)
        );
        assert_eq!(spec.binary, expected_tokio_binary);
        assert_eq!(env::var_os(NODE_BINARY_PROFILE), parent_profile);
        assert_eq!(env::var_os("RUSTFLAGS"), parent_rustflags);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_normal_and_tokio_console_builds_are_isolated() {
        let parent_profile = env::var_os(NODE_BINARY_PROFILE);
        let parent_rustflags = env::var_os("RUSTFLAGS");
        let parent_encoded_rustflags = env::var_os("CARGO_ENCODED_RUSTFLAGS");
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let normal_dir = temp_dir.path().join("normal");
        let tokio_dir = temp_dir.path().join("tokio-console");
        write_profile_build_fixture(&normal_dir);
        write_profile_build_fixture(&tokio_dir);
        let lock_dir = temp_dir.path().join("locks");
        let normal_target = normal_dir.join("target");
        let tokio_target = tokio_dir.join("target");
        let normal_output = fixture_binary_path(&normal_target);
        let tokio_output = fixture_binary_path(&tokio_target);
        let normal_provider = BuildBinaryProvider {
            command: fixture_build_command(
                fixture_build_args(&normal_dir, &normal_target),
                Some("-C opt-level=1 --cfg profile_user_flag"),
                None,
                false,
            ),
            output_path: normal_output.clone(),
            working_dir: Some(normal_dir.clone()),
            lock_dir: Some(lock_dir.clone()),
        };
        let tokio_provider = BuildBinaryProvider {
            command: fixture_build_command(
                fixture_build_args(&tokio_dir, &tokio_target),
                Some("-C opt-level=1 --cfg profile_user_flag"),
                None,
                true,
            ),
            output_path: tokio_output.clone(),
            working_dir: Some(tokio_dir),
            lock_dir: Some(lock_dir),
        };

        let normal_provider: BinaryProviderRef = Arc::new(normal_provider);
        let tokio_provider: BinaryProviderRef = Arc::new(tokio_provider);
        let (normal_result, tokio_result) = tokio::join!(
            resolve_node_binary_with_provider(normal_provider),
            resolve_node_binary_with_provider(tokio_provider),
        );
        let normal_binary = normal_result.expect("normal build should succeed");
        let tokio_binary = tokio_result.expect("Tokio-console build should succeed");

        assert_ne!(
            normal_binary, tokio_binary,
            "profiles must not share a cached binary"
        );
        assert_eq!(normal_binary, normal_output);
        assert_eq!(tokio_binary, tokio_output);
        let normal_cfg = run_fixture_binary(&normal_binary);
        let tokio_cfg = run_fixture_binary(&tokio_binary);
        assert_eq!(
            normal_cfg, "false:true:false:false:false",
            "normal Cargo must not inherit the Tokio-console cfg"
        );
        assert_eq!(tokio_cfg, "true:true:false:false:false");
        assert_eq!(env::var_os(NODE_BINARY_PROFILE), parent_profile);
        assert_eq!(env::var_os("RUSTFLAGS"), parent_rustflags);
        assert_eq!(
            env::var_os("CARGO_ENCODED_RUSTFLAGS"),
            parent_encoded_rustflags
        );
    }

    #[tokio::test]
    async fn failed_or_cancelled_resolution_does_not_mutate_parent_environment() {
        let parent_profile = env::var_os(NODE_BINARY_PROFILE);
        let parent_rustflags = env::var_os("RUSTFLAGS");
        let parent_encoded_rustflags = env::var_os("CARGO_ENCODED_RUSTFLAGS");
        let failing: BinaryProviderRef = Arc::new(FallbackBinaryProvider::new(Vec::new()));
        assert!(resolve_node_binary_with_provider(failing).await.is_err());

        let pending = tokio::spawn(resolve_node_binary_with_provider(Arc::new(PendingProvider)));
        tokio::task::yield_now().await;
        pending.abort();
        assert!(
            pending.await.is_err(),
            "pending binary resolution should cancel"
        );

        assert_eq!(env::var_os(NODE_BINARY_PROFILE), parent_profile);
        assert_eq!(env::var_os("RUSTFLAGS"), parent_rustflags);
        assert_eq!(
            env::var_os("CARGO_ENCODED_RUSTFLAGS"),
            parent_encoded_rustflags
        );
    }

    #[tokio::test]
    async fn failed_cargo_build_is_not_cached_and_can_be_retried() {
        let parent_rustflags = env::var_os("RUSTFLAGS");
        let parent_encoded_rustflags = env::var_os("CARGO_ENCODED_RUSTFLAGS");
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let fixture_dir = temp_dir.path().join("fixture");
        write_profile_build_fixture(&fixture_dir);
        let source_path = fixture_dir.join("src/main.rs");
        fs::write(
            &source_path,
            "fn main() { compile_error!(\"fixture failure\"); }\n",
        )
        .expect("failing fixture source should be written");
        let target_dir = fixture_dir.join("target");
        let output_path = fixture_binary_path(&target_dir);
        let provider: BinaryProviderRef = Arc::new(BuildBinaryProvider {
            command: fixture_build_command(
                fixture_build_args(&fixture_dir, &target_dir),
                None,
                None,
                false,
            ),
            output_path: output_path.clone(),
            working_dir: Some(fixture_dir.clone()),
            lock_dir: Some(temp_dir.path().join("locks")),
        });

        assert!(
            resolve_node_binary_with_provider(Arc::clone(&provider))
                .await
                .is_err()
        );
        assert!(
            !output_path.exists(),
            "a failed Cargo build must not leave a valid-looking cached output"
        );

        write_profile_build_fixture(&fixture_dir);
        let binary = resolve_node_binary_with_provider(provider)
            .await
            .expect("the same provider should retry after its failed build");
        assert_eq!(binary, output_path);
        assert_eq!(run_fixture_binary(&binary), "false:false:false:false:false");
        assert_eq!(env::var_os("RUSTFLAGS"), parent_rustflags);
        assert_eq!(
            env::var_os("CARGO_ENCODED_RUSTFLAGS"),
            parent_encoded_rustflags
        );
    }

    #[tokio::test]
    async fn cancelled_cargo_resolution_leaves_no_cached_binary_and_can_be_retried() {
        let parent_rustflags = env::var_os("RUSTFLAGS");
        let parent_encoded_rustflags = env::var_os("CARGO_ENCODED_RUSTFLAGS");
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let fixture_dir = temp_dir.path().join("fixture");
        write_profile_build_fixture(&fixture_dir);
        let manifest_path = fixture_dir.join("Cargo.toml");
        let manifest = fs::read_to_string(&manifest_path).expect("fixture manifest should exist");
        fs::write(
            manifest_path,
            manifest.replace(
                "edition = \"2024\"\n",
                "edition = \"2024\"\nbuild = \"build.rs\"\n",
            ),
        )
        .expect("fixture manifest should include its build script");
        fs::write(
            fixture_dir.join("build.rs"),
            "fn main() {\n    let marker = std::path::PathBuf::from(std::env::var_os(\"CARGO_MANIFEST_DIR\").unwrap()).join(\"build-started\");\n    if !marker.exists() {\n        std::fs::write(marker, b\"started\").unwrap();\n        std::thread::sleep(std::time::Duration::from_secs(2));\n    }\n}\n",
        )
        .expect("fixture build script should be written");

        let target_dir = fixture_dir.join("target");
        let output_path = fixture_binary_path(&target_dir);
        let provider: BinaryProviderRef = Arc::new(BuildBinaryProvider {
            command: fixture_build_command(
                fixture_build_args(&fixture_dir, &target_dir),
                None,
                None,
                false,
            ),
            output_path: output_path.clone(),
            working_dir: Some(fixture_dir.clone()),
            lock_dir: Some(temp_dir.path().join("locks")),
        });
        let marker_path = fixture_dir.join("build-started");
        let build = tokio::spawn(resolve_node_binary_with_provider(Arc::clone(&provider)));

        tokio::time::timeout(Duration::from_secs(30), async {
            while !marker_path.exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("Cargo should start the fixture build script");
        build.abort();
        assert!(
            build
                .await
                .expect_err("build task should be cancelled")
                .is_cancelled()
        );
        tokio::time::sleep(Duration::from_secs(3)).await;

        assert!(
            !output_path.exists(),
            "cancelled Cargo resolution must not leave a valid-looking binary"
        );
        let binary = resolve_node_binary_with_provider(provider)
            .await
            .expect("the same provider should build successfully after cancellation");
        assert_eq!(binary, output_path);
        assert_eq!(run_fixture_binary(&binary), "false:false:false:false:false");
        assert_eq!(env::var_os("RUSTFLAGS"), parent_rustflags);
        assert_eq!(
            env::var_os("CARGO_ENCODED_RUSTFLAGS"),
            parent_encoded_rustflags
        );
    }

    #[test]
    fn extracts_node_binary_from_release_archive() {
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let archive = temp_dir.path().join("release.tar.gz");
        let output = temp_dir.path().join("logos-blockchain-node");
        write_tar_gz(
            &archive,
            &[(
                "logos-blockchain-node/target/release/logos-blockchain-node",
                b"node binary",
            )],
        );

        extract_release_binary(&archive, &output).expect("node binary should be extracted");

        assert_eq!(
            fs::read(output).expect("extracted binary should be readable"),
            b"node binary"
        );
    }

    #[test]
    fn release_archive_without_node_binary_is_rejected() {
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let archive = temp_dir.path().join("release.tar.gz");
        let output = temp_dir.path().join("logos-blockchain-node");
        write_tar_gz(&archive, &[("release/README.md", b"missing binary")]);

        let error = extract_release_binary(&archive, &output)
            .expect_err("archive without the node binary should fail");

        assert_eq!(
            error.downcast_ref::<io::Error>().map(io::Error::kind),
            Some(io::ErrorKind::NotFound)
        );
        assert!(!output.exists());
    }

    fn write_tar_gz(path: &Path, entries: &[(&str, &[u8])]) {
        let file = fs::File::create(path).expect("archive file should be created");
        let encoder = GzEncoder::new(file, Compression::default());
        let mut archive = tar::Builder::new(encoder);

        for (entry_path, contents) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            archive
                .append_data(&mut header, entry_path, *contents)
                .expect("archive entry should be written");
        }

        let encoder = archive.into_inner().expect("archive should be finalized");
        encoder.finish().expect("gzip stream should be finalized");
    }

    fn config_for_profile(profile: NodeBinaryProfile) -> LbcNodeConfig {
        let topology = DeploymentBuilder::new(
            TopologyConfig::with_node_numbers(1).with_resolved_node_binary_profile(profile),
        )
        .build()
        .expect("profile deployment should build");
        let node = topology.nodes().first().expect("node should be planned");
        let run_config =
            build_node_run_config(&topology, node, None).expect("node run config should build");

        local_node_config(&topology, run_config)
    }

    fn write_profile_build_fixture(dir: &Path) {
        fs::create_dir_all(dir.join("src")).expect("fixture source directory should be created");
        fs::write(
            dir.join("Cargo.toml"),
            "[package]\nname = \"profile-build-fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
        )
        .expect("fixture manifest should be written");
        fs::write(
            dir.join("src/main.rs"),
            "#![allow(unexpected_cfgs)]\nfn main() { println!(\"{}:{}:{}:{}:{}\", cfg!(tokio_unstable), cfg!(profile_user_flag), cfg!(encoded_user), cfg!(plain_user), cfg!(encoded_space = \"two words\")); }\n",
        )
        .expect("fixture source should be written");
    }

    fn fixture_build_args(fixture_dir: &Path, target_dir: &Path) -> Vec<String> {
        vec![
            "build".to_owned(),
            "--quiet".to_owned(),
            "--manifest-path".to_owned(),
            fixture_dir.join("Cargo.toml").display().to_string(),
            "--target-dir".to_owned(),
            target_dir.display().to_string(),
        ]
    }

    fn fixture_build_command(
        build_args: impl IntoIterator<Item = String>,
        rustflags: Option<&str>,
        encoded_rustflags: Option<&str>,
        tokio_console: bool,
    ) -> BuildCommand {
        let mut environment_args = vec![
            "-u".to_owned(),
            "RUSTFLAGS".to_owned(),
            "-u".to_owned(),
            "CARGO_ENCODED_RUSTFLAGS".to_owned(),
        ];
        if let Some(rustflags) = rustflags {
            environment_args.push(format!("RUSTFLAGS={rustflags}"));
        }

        if tokio_console {
            let command = cargo_command_with_tokio_console_flags(
                build_args,
                effective_rustflags_from(rustflags, encoded_rustflags),
            );
            environment_args.extend(command.args);
        } else {
            if let Some(encoded_rustflags) = encoded_rustflags {
                environment_args.push(format!("CARGO_ENCODED_RUSTFLAGS={encoded_rustflags}"));
            }
            environment_args.push("cargo".to_owned());
            environment_args.extend(build_args);
        }

        BuildCommand::new("env").with_args(environment_args)
    }

    fn cfg_flag_count(flags: &[String], cfg: &str) -> usize {
        let pair_form_count = flags
            .windows(2)
            .filter(|pair| pair[0] == "--cfg" && pair[1] == cfg)
            .count();
        let assignment_form_count = flags
            .iter()
            .filter(|flag| flag.as_str() == format!("--cfg={cfg}"))
            .count();
        pair_form_count + assignment_form_count
    }

    fn fixture_binary_path(target_dir: &Path) -> PathBuf {
        target_dir
            .join("debug")
            .join(format!("profile-build-fixture{}", env::consts::EXE_SUFFIX))
    }

    fn run_fixture_binary(binary: &Path) -> String {
        let output = StdCommand::new(binary)
            .output()
            .expect("fixture binary should execute");
        assert!(
            output.status.success(),
            "fixture binary should exit successfully"
        );
        String::from_utf8(output.stdout)
            .expect("fixture output should be utf8")
            .trim()
            .to_owned()
    }

    struct PendingProvider;

    #[async_trait]
    impl BinaryProvider for PendingProvider {
        async fn try_resolve(
            &self,
        ) -> Result<Option<PathBuf>, testing_framework_runner_local::BinaryProviderError> {
            std::future::pending().await
        }

        fn display(&self) -> String {
            "pending test provider".to_owned()
        }

        fn cache_key(&self) -> String {
            "pending test provider".to_owned()
        }
    }

    #[test]
    fn local_cluster_lifecycle_unregisters_system_monitor_output() {
        let state_dir = tempfile::tempdir().expect("state directory should be created");
        let output = state_dir.path().join("system_stats.ndjson");
        let deployment = DeploymentBuilder::new(TopologyConfig::with_node_numbers(1))
            .scenario_base_dir(state_dir.path().to_owned())
            .build()
            .expect("deployment should build");

        LbcEnv::prepare_local_cluster(&deployment);
        assert!(output.exists());

        LbcEnv::cleanup_local_cluster(&deployment);
        fs::remove_file(&output).expect("monitor output should be removable after cleanup");
        record_system_monitor_event("after_cluster_cleanup", "test");

        assert!(!output.exists(), "cleaned-up output must not be recreated");
    }
}
