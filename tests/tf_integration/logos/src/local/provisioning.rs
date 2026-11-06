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
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
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

const TOKIO_CONSOLE_ORIGINAL_RUSTC_WRAPPER: &str =
    "LOGOS_BLOCKCHAIN_TOKIO_CONSOLE_ORIGINAL_RUSTC_WRAPPER";

#[derive(Default, Deserialize)]
#[serde(default)]
struct CargoConfigBuild {
    rustc: Option<String>,
    #[serde(rename = "rustc-wrapper")]
    rustc_wrapper: Option<String>,
    #[serde(rename = "rustc-workspace-wrapper")]
    rustc_workspace_wrapper: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct CargoConfig {
    build: Option<CargoConfigBuild>,
}

type CargoConfigSnapshot = Vec<(PathBuf, Option<Vec<u8>>)>;

fn tokio_console_wrapper_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("assets")
        .join("tokio_console_rustc_wrapper.sh")
}

fn cargo_config_directories() -> Vec<PathBuf> {
    let root = workspace_root();
    let mut directories = Vec::new();
    if let Some(cargo_home) = env::var_os("CARGO_HOME") {
        directories.push(PathBuf::from(cargo_home));
    } else if let Some(home) = env::var_os("HOME").or_else(|| env::var_os("USERPROFILE")) {
        directories.push(PathBuf::from(home).join(".cargo"));
    }

    let mut ancestors = root.ancestors().map(Path::to_path_buf).collect::<Vec<_>>();
    ancestors.reverse();
    directories.extend(ancestors.into_iter().map(|path| path.join(".cargo")));
    directories
}

fn cargo_config_snapshot() -> Result<CargoConfigSnapshot, DynError> {
    let mut snapshot = Vec::new();
    for directory in cargo_config_directories() {
        let config = directory.join("config");
        let config_toml = directory.join("config.toml");
        if config.is_file() {
            snapshot.push((config.clone(), Some(fs::read(&config)?)));
            snapshot.push((config_toml, None));
        } else if config_toml.is_file() {
            snapshot.push((config, None));
            snapshot.push((config_toml.clone(), Some(fs::read(config_toml)?)));
        } else {
            snapshot.push((config, None));
            snapshot.push((config_toml, None));
        }
    }
    Ok(snapshot)
}

fn configured_rustc_wrapper() -> Result<Option<PathBuf>, DynError> {
    let direct_wrapper = env::var_os("RUSTC_WRAPPER");
    let cargo_wrapper = env::var_os("CARGO_BUILD_RUSTC_WRAPPER");
    if let (Some(direct), Some(cargo)) = (&direct_wrapper, &cargo_wrapper)
        && direct != cargo
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "RUSTC_WRAPPER and CARGO_BUILD_RUSTC_WRAPPER disagree; refusing to choose a different compiler wrapper for the Tokio-console build",
        )
        .into());
    }
    if let Some(value) = direct_wrapper.or(cargo_wrapper) {
        return resolve_wrapper_value(&value, None, "rustc wrapper");
    }

    let mut selected = None;
    for (path, contents) in cargo_config_snapshot()? {
        let Some(contents) = contents else {
            continue;
        };
        let parsed: CargoConfig =
            toml::from_str(std::str::from_utf8(&contents).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Cargo config {} is not UTF-8: {error}", path.display()),
                )
            })?)
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Cannot parse Cargo config {}: {error}", path.display()),
                )
            })?;
        if let Some(value) = parsed.build.and_then(|build| build.rustc_wrapper) {
            let config_root = path.parent().and_then(Path::parent);
            selected = resolve_wrapper_value(
                std::ffi::OsStr::new(&value),
                config_root,
                "configured rustc wrapper",
            )?;
        }
    }
    Ok(selected)
}

fn resolve_wrapper_value(
    value: &std::ffi::OsStr,
    config_root: Option<&Path>,
    description: &str,
) -> Result<Option<PathBuf>, DynError> {
    if value.is_empty() {
        return Ok(None);
    }

    let path = PathBuf::from(value);
    if path.is_absolute() {
        return existing_wrapper_path(&path, description).map(Some);
    }
    if let Some(config_root) = config_root {
        let configured = config_root.join(&path);
        if configured.exists() {
            return existing_wrapper_path(&configured, description).map(Some);
        }
    } else if path.components().count() > 1 {
        let relative = env::current_dir()?.join(&path);
        if relative.exists() {
            return existing_wrapper_path(&relative, description).map(Some);
        }
    }

    if path.components().count() == 1
        && let Some(search_path) = env::var_os("PATH")
    {
        for directory in env::split_paths(&search_path) {
            let candidate = directory.join(&path);
            if candidate.is_file() {
                return existing_wrapper_path(&candidate, description).map(Some);
            }
        }
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("Cannot resolve {description} `{}`", path.display()),
    )
    .into())
}

fn existing_wrapper_path(path: &Path, description: &str) -> Result<PathBuf, DynError> {
    if !path.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{description} `{}` is not a file", path.display()),
        )
        .into());
    }
    Ok(path.canonicalize()?)
}

fn build_environment_names() -> Vec<std::ffi::OsString> {
    const FIXED: &[&str] = &[
        "RUSTFLAGS",
        "CARGO_ENCODED_RUSTFLAGS",
        "RUSTC",
        "CARGO_BUILD_RUSTC",
        "RUSTC_WRAPPER",
        "CARGO_BUILD_RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
        "CARGO_BUILD_TARGET",
        "CARGO_BUILD_TARGET_DIR",
        "CARGO_TARGET_DIR",
        "CARGO_BUILD_INCREMENTAL",
        "CARGO_INCREMENTAL",
        "CARGO_HOME",
        "RUSTUP_TOOLCHAIN",
        "RUSTC_BOOTSTRAP",
        "PATH",
        "CC",
        "CXX",
        "AR",
        "CFLAGS",
        "CXXFLAGS",
        "CPPFLAGS",
        "LDFLAGS",
        "PKG_CONFIG_PATH",
        "PKG_CONFIG_LIBDIR",
        "PKG_CONFIG_SYSROOT_DIR",
        "SDKROOT",
        "MACOSX_DEPLOYMENT_TARGET",
        "IPHONEOS_DEPLOYMENT_TARGET",
    ];
    let mut names = FIXED
        .iter()
        .map(std::ffi::OsString::from)
        .collect::<Vec<_>>();
    names.extend(env::vars_os().filter_map(|(name, _)| {
        let name = name.to_string_lossy();
        (name.starts_with("CARGO_TARGET_")
            || name.starts_with("CARGO_PROFILE_")
            || name.starts_with("CARGO_BUILD_"))
        .then(|| std::ffi::OsString::from(name.as_ref()))
    }));
    names.sort_unstable();
    names.dedup();
    names
}

fn hash_field(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(bytes);
}

fn compiler_program_values(
    config_snapshot: &CargoConfigSnapshot,
) -> Result<Vec<(std::ffi::OsString, Option<PathBuf>)>, DynError> {
    const ENVIRONMENT_NAMES: &[&str] = &[
        "RUSTC",
        "CARGO_BUILD_RUSTC",
        "RUSTC_WRAPPER",
        "CARGO_BUILD_RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
    ];
    let mut programs = Vec::new();
    for name in ENVIRONMENT_NAMES {
        if let Some(value) = env::var_os(name) {
            let path = resolve_wrapper_value(&value, None, name).ok().flatten();
            programs.push((value, path));
        }
    }
    for (config_path, contents) in config_snapshot {
        let Some(contents) = contents else {
            continue;
        };
        let parsed: CargoConfig =
            toml::from_str(std::str::from_utf8(contents).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Cargo config {} is not UTF-8: {error}",
                        config_path.display()
                    ),
                )
            })?)
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Cannot parse Cargo config {}: {error}",
                        config_path.display()
                    ),
                )
            })?;
        let Some(build) = parsed.build else {
            continue;
        };
        let config_root = config_path.parent().and_then(Path::parent);
        for value in [
            build.rustc,
            build.rustc_wrapper,
            build.rustc_workspace_wrapper,
        ]
        .into_iter()
        .flatten()
        {
            let path = resolve_wrapper_value(
                std::ffi::OsStr::new(&value),
                config_root,
                "Cargo compiler program",
            )
            .ok()
            .flatten();
            programs.push((value.into(), path));
        }
    }
    Ok(programs)
}

fn managed_build_target_dir(
    profile: &NodeBinaryProfile,
    build_args: &[String],
    tokio_console: bool,
) -> Result<PathBuf, DynError> {
    let environment = build_environment_names()
        .into_iter()
        .map(|name| {
            let value = env::var_os(&name);
            (name, value)
        })
        .collect::<Vec<_>>();
    let config_snapshot = cargo_config_snapshot()?;
    let compiler_programs = compiler_program_values(&config_snapshot)?;
    let toolchain_files = [
        workspace_root().join("rust-toolchain.toml"),
        workspace_root().join("rust-toolchain"),
    ]
    .into_iter()
    .map(|path| {
        let contents = path.is_file().then(|| fs::read(&path)).transpose()?;
        Ok((path, contents))
    })
    .collect::<Result<Vec<_>, io::Error>>()?;
    let mut extra_files = Vec::new();
    if tokio_console {
        let wrapper = tokio_console_wrapper_path();
        extra_files.push((wrapper.clone(), fs::read(wrapper)?));
        if let Some(original) = configured_rustc_wrapper()? {
            extra_files.push((original.clone(), fs::read(original)?));
        }
    }

    build_target_dir_for_configuration(
        &workspace_root(),
        profile,
        build_args,
        &environment,
        &config_snapshot,
        &compiler_programs,
        &toolchain_files,
        &extra_files,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "The identity hashes each distinct compiler and Cargo input explicitly"
)]
fn build_target_dir_for_configuration(
    target_root: &Path,
    profile: &NodeBinaryProfile,
    build_args: &[String],
    environment: &[(std::ffi::OsString, Option<std::ffi::OsString>)],
    config_snapshot: &CargoConfigSnapshot,
    compiler_programs: &[(std::ffi::OsString, Option<PathBuf>)],
    toolchain_files: &[(PathBuf, Option<Vec<u8>>)],
    extra_files: &[(PathBuf, Vec<u8>)],
) -> Result<PathBuf, DynError> {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, b"logos-managed-node-build-v2");
    hash_field(&mut hasher, profile.to_string().as_bytes());
    for argument in build_args {
        hash_field(&mut hasher, argument.as_bytes());
    }
    for (name, value) in environment {
        hash_field(&mut hasher, name.as_encoded_bytes());
        match value {
            Some(value) => hash_field(&mut hasher, value.as_encoded_bytes()),
            None => hash_field(&mut hasher, b"<unset>"),
        }
    }
    for (path, contents) in config_snapshot {
        hash_field(&mut hasher, path.as_os_str().as_encoded_bytes());
        if let Some(contents) = contents {
            hash_field(&mut hasher, contents);
        } else {
            hash_field(&mut hasher, b"<missing-or-shadowed>");
        }
    }
    for (program, path) in compiler_programs {
        hash_field(&mut hasher, program.as_encoded_bytes());
        if let Some(path) = path {
            hash_field(&mut hasher, path.as_os_str().as_encoded_bytes());
            hash_field(&mut hasher, &fs::read(path)?);
        } else {
            hash_field(&mut hasher, b"<unresolved-program>");
        }
    }
    for (file, contents) in toolchain_files {
        hash_field(&mut hasher, file.as_os_str().as_encoded_bytes());
        if let Some(contents) = contents {
            hash_field(&mut hasher, contents);
        } else {
            hash_field(&mut hasher, b"<missing>");
        }
    }
    for (path, contents) in extra_files {
        hash_field(&mut hasher, path.as_os_str().as_encoded_bytes());
        hash_field(&mut hasher, contents);
    }
    let digest = hasher.finalize();
    let identity = hex::encode(digest);
    Ok(target_root
        .join("target")
        .join(".tf-binaries")
        .join("builds")
        .join(identity))
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
        NodeBinaryProfile::Normal => default_node_binary_provider()?,
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

fn default_node_binary_provider() -> Result<BinaryProviderRef, DynError> {
    if running_in_ci() {
        Ok(Arc::new(
            PathBinaryProvider::new(release_node_binary_path()),
        ))
    } else {
        let build_args = [
            "build".to_owned(),
            "--locked".to_owned(),
            "--release".to_owned(),
            "-p".to_owned(),
            "logos-blockchain-node".to_owned(),
            "--features".to_owned(),
            "testing".to_owned(),
        ];
        let target_dir = managed_build_target_dir(&NodeBinaryProfile::Normal, &build_args, false)?;
        let command = BuildCommand::new("cargo").with_args(
            build_args
                .into_iter()
                .chain(["--target-dir".to_owned(), target_dir.display().to_string()]),
        );
        Ok(Arc::new(BuildBinaryProvider {
            command,
            output_path: target_dir.join("release").join("logos-blockchain-node"),
            working_dir: Some(workspace_root()),
            lock_dir: Some(workspace_root().join("target").join(".tf-binaries")),
        }))
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
        let build_args = [
            "build".to_owned(),
            "--locked".to_owned(),
            "--profile".to_owned(),
            "release-profiling".to_owned(),
            "-p".to_owned(),
            "logos-blockchain-node".to_owned(),
            "--features".to_owned(),
            "testing,tokio-console".to_owned(),
        ];
        let target_dir =
            managed_build_target_dir(&NodeBinaryProfile::TokioConsole, &build_args, true)?;
        let command = tokio_console_build_command(
            build_args
                .into_iter()
                .chain(["--target-dir".to_owned(), target_dir.display().to_string()]),
        )?;
        Ok(Arc::new(BuildBinaryProvider {
            command,
            output_path: target_dir
                .join("release-profiling")
                .join("logos-blockchain-node"),
            working_dir: Some(workspace_root()),
            lock_dir: Some(workspace_root().join("target").join(".tf-binaries")),
        }))
    }
}

fn tokio_console_build_command(
    build_args: impl IntoIterator<Item = String>,
) -> Result<BuildCommand, DynError> {
    let wrapper = tokio_console_wrapper_path();
    let mut env_args = vec![
        "-u".to_owned(),
        TOKIO_CONSOLE_ORIGINAL_RUSTC_WRAPPER.to_owned(),
        format!("RUSTC_WRAPPER={}", wrapper.display()),
        format!("CARGO_BUILD_RUSTC_WRAPPER={}", wrapper.display()),
    ];
    if let Some(original_wrapper) = configured_rustc_wrapper()? {
        env_args.push(format!(
            "{TOKIO_CONSOLE_ORIGINAL_RUSTC_WRAPPER}={}",
            original_wrapper.display()
        ));
    }
    env_args.push("cargo".to_owned());
    env_args.extend(build_args);
    Ok(BuildCommand::new("env").with_args(env_args))
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

    #[cfg(unix)]
    #[expect(
        clippy::too_many_lines,
        clippy::type_complexity,
        reason = "Exercises Cargo's distinct rustflags sources through real fixture builds"
    )]
    #[tokio::test]
    async fn tokio_console_compiler_wrapper_preserves_cargo_flag_sources() {
        let parent_environment = [
            "RUSTFLAGS",
            "CARGO_ENCODED_RUSTFLAGS",
            "RUSTC_WRAPPER",
            "RUSTC_WORKSPACE_WRAPPER",
            "CARGO_BUILD_RUSTC_WRAPPER",
            "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
        ]
        .map(|name| (name, env::var_os(name)));
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let lock_dir = temp_dir.path().join("locks");
        let cases: [(&str, &[(&str, &str)], Option<&str>, &str); 7] = [
            (
                "none",
                &[],
                None,
                "true:false:false:false:false:false:false:false:false",
            ),
            (
                "rustflags",
                &[("RUSTFLAGS", "-C opt-level=1 --cfg fixture_rustflags")],
                None,
                "true:true:false:false:false:false:false:false:false",
            ),
            (
                "encoded",
                &[(
                    "CARGO_ENCODED_RUSTFLAGS",
                    "--cfg\u{1f}fixture_encoded\u{1f}--cfg\u{1f}encoded_space=\"two words\"",
                )],
                None,
                "true:false:true:false:true:false:false:false:false",
            ),
            (
                "both",
                &[
                    ("RUSTFLAGS", "--cfg fixture_plain"),
                    ("CARGO_ENCODED_RUSTFLAGS", "--cfg\u{1f}fixture_encoded"),
                ],
                None,
                "true:false:true:false:false:false:false:false:false",
            ),
            (
                "target-config",
                &[],
                Some("[target.'cfg(unix)']\nrustflags = ['--cfg', 'fixture_target']\n"),
                "true:false:false:false:false:true:false:false:false",
            ),
            (
                "build-config",
                &[],
                Some("[build]\nrustflags = ['--cfg', 'fixture_build']\n"),
                "true:false:false:false:false:false:true:false:false",
            ),
            (
                "existing-tokio-flag",
                &[("RUSTFLAGS", "--cfg tokio_unstable --cfg fixture_rustflags")],
                None,
                "true:true:false:false:false:false:false:false:false",
            ),
        ];

        for (name, environment, cargo_config, expected) in cases {
            let fixture_dir = temp_dir.path().join(name);
            write_profile_build_fixture(&fixture_dir);
            if let Some(config) = cargo_config {
                write_fixture_cargo_config(&fixture_dir, config);
            }
            let target_dir = fixture_dir.join("target");
            let output_path = fixture_binary_path(&target_dir);
            let provider = BuildBinaryProvider {
                command: fixture_build_command(
                    fixture_build_args(&fixture_dir, &target_dir),
                    true,
                    environment,
                    None,
                ),
                output_path,
                working_dir: Some(fixture_dir.clone()),
                lock_dir: Some(lock_dir.clone()),
            };
            let binary = resolve_node_binary_with_provider(Arc::new(provider))
                .await
                .unwrap_or_else(|error| panic!("{name} Cargo build should succeed: {error}"));
            assert_eq!(run_fixture_binary(&binary), expected, "case {name}");
        }

        let assignment_dir = temp_dir.path().join("existing-tokio-assignment");
        write_profile_build_fixture(&assignment_dir);
        let existing_rustc_wrapper = temp_dir.path().join("existing-rustc-wrapper.sh");
        let existing_rustc_log = temp_dir.path().join("existing-rustc-wrapper.log");
        let existing_workspace_wrapper = temp_dir.path().join("existing-workspace-wrapper.sh");
        let workspace_log = temp_dir.path().join("workspace-wrapper.log");
        write_logging_wrapper(&existing_rustc_wrapper, "TOKIO_EXISTING_RUSTC_WRAPPER_LOG");
        write_logging_wrapper(&existing_workspace_wrapper, "TOKIO_WORKSPACE_WRAPPER_LOG");
        let build_environment = [
            (
                "CARGO_ENCODED_RUSTFLAGS",
                "--cfg=tokio_unstable\u{1f}--cfg\u{1f}encoded_space=\"two words\"",
            ),
            (
                "RUSTC_WORKSPACE_WRAPPER",
                existing_workspace_wrapper.to_str().unwrap(),
            ),
            (
                "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER",
                existing_workspace_wrapper.to_str().unwrap(),
            ),
            (
                "TOKIO_WORKSPACE_WRAPPER_LOG",
                workspace_log.to_str().unwrap(),
            ),
            (
                "TOKIO_EXISTING_RUSTC_WRAPPER_LOG",
                existing_rustc_log.to_str().unwrap(),
            ),
        ];
        let target_dir = assignment_dir.join("target");
        let output_path = fixture_binary_path(&target_dir);
        let provider = BuildBinaryProvider {
            command: fixture_build_command(
                fixture_build_args(&assignment_dir, &target_dir),
                true,
                &build_environment,
                Some(&existing_rustc_wrapper),
            ),
            output_path,
            working_dir: Some(assignment_dir),
            lock_dir: Some(lock_dir),
        };
        let binary = resolve_node_binary_with_provider(Arc::new(provider))
            .await
            .expect("existing Cargo wrappers should remain in the invocation chain");
        assert_eq!(
            run_fixture_binary(&binary),
            "true:false:false:false:true:false:false:false:false"
        );
        assert!(fs::metadata(&workspace_log).unwrap().len() > 0);
        assert!(fs::metadata(&existing_rustc_log).unwrap().len() > 0);
        assert!(
            fs::read(&workspace_log)
                .unwrap()
                .windows(b"encoded_space=\"two words\"\0".len())
                .any(|window| window == b"encoded_space=\"two words\"\0"),
            "Cargo must preserve encoded flag boundaries through both wrappers"
        );
        assert!(
            logged_tokio_cfgs(&workspace_log)
                .iter()
                .all(|count| *count <= 1),
            "the wrapper must not append a second tokio_unstable cfg"
        );

        for (name, expected) in parent_environment {
            assert_eq!(env::var_os(name), expected, "parent {name} changed");
        }
    }

    #[cfg(unix)]
    #[expect(
        clippy::too_many_lines,
        reason = "Covers A-B-A artifact isolation, reuse, and overlapping fixture builds"
    )]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn managed_binary_artifacts_are_isolated_for_a_b_a_and_concurrent_builds() {
        let parent_rustflags = env::var_os("RUSTFLAGS");
        let parent_encoded = env::var_os("CARGO_ENCODED_RUSTFLAGS");
        let temp_dir = tempfile::tempdir().expect("temporary directory should be created");
        let fixture_dir = temp_dir.path().join("fixture");
        write_profile_build_fixture(&fixture_dir);
        let build_count = temp_dir.path().join("build-count");
        write_counting_build_script(&fixture_dir);
        let logical_args = vec![
            "build".to_owned(),
            "--quiet".to_owned(),
            "--manifest-path".to_owned(),
            fixture_dir.join("Cargo.toml").display().to_string(),
        ];
        let config_snapshot = fixture_config_snapshot(&fixture_dir);
        let wrapper_path = tokio_console_wrapper_path();
        let wrapper_file = (wrapper_path.clone(), fs::read(&wrapper_path).unwrap());
        let env_a = fixture_environment_snapshot("--cfg fixture_a");
        let env_b = fixture_environment_snapshot("--cfg fixture_b");
        let extra_files = [wrapper_file];
        let target_a = build_target_dir_for_configuration(
            &fixture_dir,
            &NodeBinaryProfile::TokioConsole,
            &logical_args,
            &env_a,
            &config_snapshot,
            &[],
            &[],
            &extra_files,
        )
        .expect("configuration A should have a stable target directory");
        let target_b = build_target_dir_for_configuration(
            &fixture_dir,
            &NodeBinaryProfile::TokioConsole,
            &logical_args,
            &env_b,
            &config_snapshot,
            &[],
            &[],
            &extra_files,
        )
        .expect("configuration B should have a stable target directory");
        assert_ne!(
            target_a, target_b,
            "different flags need separate artifacts"
        );

        let env_a_command = [
            ("RUSTFLAGS", "--cfg fixture_a"),
            ("FIXTURE_BUILD_COUNT_FILE", build_count.to_str().unwrap()),
        ];
        let env_b_command = [
            ("RUSTFLAGS", "--cfg fixture_b"),
            ("FIXTURE_BUILD_COUNT_FILE", build_count.to_str().unwrap()),
        ];
        let lock_dir = temp_dir.path().join("locks");
        let provider_a: BinaryProviderRef = Arc::new(BuildBinaryProvider {
            command: fixture_build_command(
                fixture_build_args(&fixture_dir, &target_a),
                true,
                &env_a_command,
                None,
            ),
            output_path: fixture_binary_path(&target_a),
            working_dir: Some(fixture_dir.clone()),
            lock_dir: Some(lock_dir.clone()),
        });
        let provider_b: BinaryProviderRef = Arc::new(BuildBinaryProvider {
            command: fixture_build_command(
                fixture_build_args(&fixture_dir, &target_b),
                true,
                &env_b_command,
                None,
            ),
            output_path: fixture_binary_path(&target_b),
            working_dir: Some(fixture_dir.clone()),
            lock_dir: Some(lock_dir),
        });

        let binary_a = resolve_node_binary_with_provider(Arc::clone(&provider_a))
            .await
            .expect("configuration A should build");
        assert_eq!(
            run_fixture_binary(&binary_a),
            "true:false:false:false:false:false:false:true:false"
        );
        assert_eq!(fs::read_to_string(&build_count).unwrap().trim(), "1");

        let binary_b = resolve_node_binary_with_provider(Arc::clone(&provider_b))
            .await
            .expect("configuration B should build");
        assert_ne!(binary_a, binary_b);
        assert_eq!(
            run_fixture_binary(&binary_b),
            "true:false:false:false:false:false:false:false:true"
        );
        assert_eq!(
            run_fixture_binary(&binary_a),
            "true:false:false:false:false:false:false:true:false"
        );

        let binary_a_again = resolve_node_binary_with_provider(provider_a)
            .await
            .expect("configuration A should resolve after B");
        assert_eq!(binary_a_again, binary_a);
        assert_eq!(
            run_fixture_binary(&binary_a_again),
            "true:false:false:false:false:false:false:true:false"
        );
        assert_eq!(fs::read_to_string(&build_count).unwrap().trim(), "2");

        let concurrent_dir = temp_dir.path().join("concurrent");
        write_profile_build_fixture(&concurrent_dir);
        let concurrent_args = vec![
            "build".to_owned(),
            "--quiet".to_owned(),
            "--manifest-path".to_owned(),
            concurrent_dir.join("Cargo.toml").display().to_string(),
        ];
        let concurrent_config = fixture_config_snapshot(&concurrent_dir);
        let target_normal = build_target_dir_for_configuration(
            &concurrent_dir,
            &NodeBinaryProfile::Normal,
            &concurrent_args,
            &env_a,
            &concurrent_config,
            &[],
            &[],
            &[],
        )
        .expect("normal profile should have an isolated target directory");
        let target_tokio = build_target_dir_for_configuration(
            &concurrent_dir,
            &NodeBinaryProfile::TokioConsole,
            &concurrent_args,
            &env_b,
            &concurrent_config,
            &[],
            &[],
            &extra_files,
        )
        .expect("Tokio-console profile should have an isolated target directory");
        assert_ne!(target_normal, target_tokio);
        let normal_environment = [("RUSTFLAGS", "--cfg fixture_a")];
        let tokio_environment = [("RUSTFLAGS", "--cfg fixture_b")];
        let normal_provider = Arc::new(BuildBinaryProvider {
            command: fixture_build_command(
                fixture_build_args(&concurrent_dir, &target_normal),
                false,
                &normal_environment,
                None,
            ),
            output_path: fixture_binary_path(&target_normal),
            working_dir: Some(concurrent_dir.clone()),
            lock_dir: Some(temp_dir.path().join("locks")),
        });
        let tokio_provider = Arc::new(BuildBinaryProvider {
            command: fixture_build_command(
                fixture_build_args(&concurrent_dir, &target_tokio),
                true,
                &tokio_environment,
                None,
            ),
            output_path: fixture_binary_path(&target_tokio),
            working_dir: Some(concurrent_dir),
            lock_dir: Some(temp_dir.path().join("locks")),
        });
        let (normal, tokio_console) = tokio::join!(
            resolve_node_binary_with_provider(normal_provider),
            resolve_node_binary_with_provider(tokio_provider),
        );
        let normal = normal.expect("normal concurrent build should succeed");
        let tokio_console = tokio_console.expect("Tokio-console concurrent build should succeed");
        assert_ne!(normal, tokio_console);
        assert_eq!(
            run_fixture_binary(&normal),
            "false:false:false:false:false:false:false:true:false"
        );
        assert_eq!(
            run_fixture_binary(&tokio_console),
            "true:false:false:false:false:false:false:false:true"
        );
        assert_eq!(env::var_os("RUSTFLAGS"), parent_rustflags);
        assert_eq!(env::var_os("CARGO_ENCODED_RUSTFLAGS"), parent_encoded);
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

    #[cfg(unix)]
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
                true,
                &[],
                None,
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
        assert_eq!(
            run_fixture_binary(&binary),
            "true:false:false:false:false:false:false:false:false"
        );
        assert_eq!(env::var_os("RUSTFLAGS"), parent_rustflags);
        assert_eq!(
            env::var_os("CARGO_ENCODED_RUSTFLAGS"),
            parent_encoded_rustflags
        );
    }

    #[cfg(unix)]
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
                true,
                &[],
                None,
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
        assert_eq!(
            run_fixture_binary(&binary),
            "true:false:false:false:false:false:false:false:false"
        );
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
            "#![allow(unexpected_cfgs)]\nfn main() { println!(\"{}:{}:{}:{}:{}:{}:{}:{}:{}\", cfg!(tokio_unstable), cfg!(fixture_rustflags), cfg!(fixture_encoded), cfg!(fixture_plain), cfg!(encoded_space = \"two words\"), cfg!(fixture_target), cfg!(fixture_build), cfg!(fixture_a), cfg!(fixture_b)); }\n",
        )
        .expect("fixture source should be written");
    }

    fn write_fixture_cargo_config(dir: &Path, contents: &str) {
        fs::create_dir_all(dir.join(".cargo"))
            .expect("fixture cargo config directory should exist");
        fs::write(dir.join(".cargo/config.toml"), contents)
            .expect("fixture cargo config should be written");
    }

    fn fixture_config_snapshot(dir: &Path) -> CargoConfigSnapshot {
        let config_dir = dir.join(".cargo");
        let config = config_dir.join("config");
        let config_toml = config_dir.join("config.toml");
        vec![
            (config, None),
            (
                config_toml.clone(),
                config_toml.exists().then(|| fs::read(config_toml).unwrap()),
            ),
        ]
    }

    fn fixture_environment_snapshot(
        rustflags: &str,
    ) -> Vec<(std::ffi::OsString, Option<std::ffi::OsString>)> {
        [
            ("RUSTFLAGS", Some(rustflags)),
            ("CARGO_ENCODED_RUSTFLAGS", None),
            ("RUSTC_WRAPPER", None),
            ("CARGO_BUILD_RUSTC_WRAPPER", None),
            ("RUSTC_WORKSPACE_WRAPPER", None),
            ("CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER", None),
            ("CARGO_BUILD_TARGET", None),
            ("RUSTUP_TOOLCHAIN", None),
        ]
        .into_iter()
        .map(|(name, value)| (name.into(), value.map(std::ffi::OsString::from)))
        .collect()
    }

    fn write_counting_build_script(dir: &Path) {
        let manifest_path = dir.join("Cargo.toml");
        let manifest = fs::read_to_string(&manifest_path).expect("fixture manifest should exist");
        fs::write(
            manifest_path,
            manifest.replace(
                "edition = \"2024\"\n",
                "edition = \"2024\"\nbuild = \"build.rs\"\n",
            ),
        )
        .expect("fixture manifest should enable build script");
        fs::write(
            dir.join("build.rs"),
            "fn main() { println!(\"cargo:rerun-if-env-changed=FIXTURE_BUILD_COUNT_FILE\"); let path = std::path::PathBuf::from(std::env::var_os(\"FIXTURE_BUILD_COUNT_FILE\").unwrap()); let count = std::fs::read_to_string(&path).ok().and_then(|value| value.trim().parse::<u32>().ok()).unwrap_or_default(); std::fs::write(path, (count + 1).to_string()).unwrap(); }\n",
        )
        .expect("fixture build script should be written");
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

    #[cfg(unix)]
    fn fixture_build_command(
        build_args: impl IntoIterator<Item = String>,
        tokio_console: bool,
        child_environment: &[(&str, &str)],
        existing_workspace_wrapper: Option<&Path>,
    ) -> BuildCommand {
        let build_args = build_args.into_iter().collect::<Vec<_>>();
        let fixture_dir = build_args
            .windows(2)
            .find(|pair| pair[0] == "--manifest-path")
            .map(|pair| PathBuf::from(&pair[1]).parent().unwrap().to_owned())
            .expect("fixture build arguments should include a manifest path");
        let mut environment_args = vec![
            "-u".to_owned(),
            "RUSTFLAGS".to_owned(),
            "-u".to_owned(),
            "CARGO_ENCODED_RUSTFLAGS".to_owned(),
            "-u".to_owned(),
            "RUSTC_WRAPPER".to_owned(),
            "-u".to_owned(),
            "CARGO_BUILD_RUSTC_WRAPPER".to_owned(),
            "-u".to_owned(),
            "RUSTC_WORKSPACE_WRAPPER".to_owned(),
            "-u".to_owned(),
            "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER".to_owned(),
            "-u".to_owned(),
            TOKIO_CONSOLE_ORIGINAL_RUSTC_WRAPPER.to_owned(),
            format!("CARGO_HOME={}", fixture_dir.join("cargo-home").display()),
        ];
        environment_args.extend(
            child_environment
                .iter()
                .map(|(name, value)| format!("{name}={value}")),
        );
        if tokio_console {
            if let Some(original) = existing_workspace_wrapper {
                environment_args.push(format!(
                    "{TOKIO_CONSOLE_ORIGINAL_RUSTC_WRAPPER}={}",
                    original.display()
                ));
            }
            let wrapper = tokio_console_wrapper_path();
            environment_args.push(format!("RUSTC_WRAPPER={}", wrapper.display()));
            environment_args.push(format!("CARGO_BUILD_RUSTC_WRAPPER={}", wrapper.display()));
        }
        environment_args.push("cargo".to_owned());
        environment_args.extend(build_args);

        BuildCommand::new("env").with_args(environment_args)
    }

    #[cfg(unix)]
    fn write_logging_wrapper(path: &Path, log_environment_variable: &str) {
        use std::os::unix::fs::PermissionsExt as _;

        let script = format!(
            "#!/bin/sh\nset -eu\nprintf 'CALL\\000' >> \"${log_environment_variable}\"\nfor argument do printf '%s\\000' \"$argument\" >> \"${log_environment_variable}\"; done\nprintf '\\n' >> \"${log_environment_variable}\"\nexec \"$@\"\n"
        );
        fs::write(path, script).expect("compiler wrapper script should be written");
        let mut permissions = fs::metadata(path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("compiler wrapper should be executable");
    }

    #[cfg(unix)]
    fn logged_tokio_cfgs(log_path: &Path) -> Vec<usize> {
        let bytes = fs::read(log_path).expect("compiler wrapper should produce a log");
        bytes
            .split(|byte| *byte == b'\n')
            .filter_map(|call| {
                let args = call
                    .split(|byte| *byte == 0)
                    .filter(|argument| !argument.is_empty())
                    .collect::<Vec<_>>();
                let pair_count = args
                    .windows(2)
                    .filter(|pair| pair[0] == b"--cfg" && pair[1] == b"tokio_unstable")
                    .count();
                let assignment_count = args
                    .iter()
                    .filter(|argument| **argument == b"--cfg=tokio_unstable")
                    .count();
                (pair_count + assignment_count > 0).then_some(pair_count + assignment_count)
            })
            .collect()
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
