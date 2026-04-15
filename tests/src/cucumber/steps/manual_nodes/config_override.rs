use lb_node::config::RunConfig;
use serde::{Serialize, de::DeserializeOwned};
use serde_yaml::{Mapping, Value as YamlValue};
use time::{Duration as TimeDuration, OffsetDateTime};

use crate::cucumber::{
    error::{StepError, StepResult},
    world::{ConfigOverride, CucumberWorld},
};

/// Publishes a user config override to the world, to be applied to nodes when
/// they are created.
pub fn set_user_config_override(
    world: &mut CucumberWorld,
    step: &str,
    raw_path: &str,
    raw_value: &str,
) -> StepResult {
    let path = normalize_custom_config_path(step, raw_path)?;
    let value = parse_custom_config_step_value(step, raw_value)?;

    if let Some(existing) = world
        .user_config_overrides
        .iter_mut()
        .find(|override_item| override_item.path == path)
    {
        existing.value = value;
        return Ok(());
    }

    world
        .user_config_overrides
        .push(ConfigOverride { path, value });
    Ok(())
}

/// Publishes a deployment config override to the world, to be applied to nodes
/// when they are created.
pub fn set_deployment_config_override(
    world: &mut CucumberWorld,
    step: &str,
    raw_path: &str,
    raw_value: &str,
) -> StepResult {
    let path = normalize_custom_config_path(step, raw_path)?;
    let value = parse_custom_config_step_value(step, raw_value)?;

    if let Some(existing) = world
        .deployment_config_overrides
        .iter_mut()
        .find(|override_item| override_item.path == path)
    {
        existing.value = value;
        return Ok(());
    }

    world
        .deployment_config_overrides
        .push(ConfigOverride { path, value });
    Ok(())
}

/// Applies a user config override to the runtime config.
pub fn apply_user_config_overrides(
    config: &mut RunConfig,
    config_overrides: &[ConfigOverride],
) -> Result<(), StepError> {
    apply_overrides_to(&mut config.user, config_overrides, "user")
}

/// Applies a deployment config override to the runtime config.
pub fn apply_deployment_config_overrides(
    config: &mut RunConfig,
    config_overrides: &[ConfigOverride],
) -> Result<(), StepError> {
    apply_overrides_to(&mut config.deployment, config_overrides, "deployment")
}

fn apply_overrides_to<T>(
    target: &mut T,
    config_overrides: &[ConfigOverride],
    logical_name: &str,
) -> Result<(), StepError>
where
    T: Serialize + DeserializeOwned,
{
    if config_overrides.is_empty() {
        return Ok(());
    }

    let mut yaml = serde_yaml::to_value(&*target).map_err(|source| StepError::LogicalError {
        message: format!("Failed to serialize node {logical_name} config for patching: {source}"),
    })?;

    for override_item in config_overrides {
        let path = override_item.path.split('.').collect::<Vec<_>>();
        let value = coerce_override_value_for_existing_path(
            &yaml,
            &path,
            override_item.value.clone(),
            &override_item.path,
        )?;
        set_yaml_value_at_path(&mut yaml, &path, value, &override_item.path)?;
    }

    *target = serde_yaml::from_value(yaml).map_err(|source| StepError::InvalidArgument {
        message: format!(
            "Invalid {logical_name} config override combination. Resulting config could not be deserialized: {source}"
        ),
    })?;

    Ok(())
}

// Normalizes a custom config path by trimming whitespace and removing empty
// segments.
fn normalize_custom_config_path(step: &str, raw_path: &str) -> Result<String, StepError> {
    let path = raw_path.trim();
    if path.is_empty() {
        return Err(StepError::InvalidArgument {
            message: format!("Step `{step}` error: user config path cannot be empty"),
        });
    }

    let mut normalized = String::with_capacity(path.len());
    for segment in path.split('.').map(str::trim) {
        if segment.is_empty() {
            return Err(StepError::InvalidArgument {
                message: format!(
                    "Step `{step}` error: user config path '{path}' has an empty segment"
                ),
            });
        }

        if !normalized.is_empty() {
            normalized.push('.');
        }
        normalized.push_str(segment);
    }

    Ok(normalized)
}

// Parses a custom config step value from a string, handling special cases like
// relative timestamps.
fn parse_custom_config_step_value(step: &str, raw_value: &str) -> Result<YamlValue, StepError> {
    let value = raw_value.trim();

    if let Some(relative_seconds) = parse_now_plus_seconds(value) {
        let timestamp = OffsetDateTime::now_utc() + TimeDuration::seconds(relative_seconds);
        return serde_yaml::to_value(timestamp).map_err(|source| StepError::InvalidArgument {
            message: format!(
                "Step `{step}` error: config value '{value}' could not be converted to YAML: {source}",
            ),
        });
    }

    // Values like `1,2` are not YAML scalars but are useful shorthand in steps.
    serde_yaml::from_str::<YamlValue>(value)
        .map_or_else(|_| Ok(YamlValue::String(value.to_owned())), Ok)
}

// Parses a string like "now+10s" and returns the number of seconds as an
// integer.
fn parse_now_plus_seconds(value: &str) -> Option<i64> {
    let suffix = value.strip_prefix("now+")?.strip_suffix('s')?;
    suffix.parse::<i64>().ok()
}

fn coerce_override_value_for_existing_path(
    root: &YamlValue,
    path: &[&str],
    override_value: YamlValue,
    full_path: &str,
) -> Result<YamlValue, StepError> {
    let Some(existing_leaf) = yaml_value_at_path(root, path) else {
        return Ok(override_value);
    };

    if is_duration_like_yaml(existing_leaf) {
        let Some((seconds, nanos)) = parse_duration_parts_from_override(&override_value) else {
            return Ok(override_value);
        };

        if seconds < 0 {
            return Err(StepError::InvalidArgument {
                message: format!(
                    "Invalid config override path '{full_path}': negative duration '{seconds}' is not supported"
                ),
            });
        }

        return Ok(duration_parts_to_yaml_like_existing(
            existing_leaf,
            seconds,
            nanos,
        ));
    }

    if is_offset_datetime_like_yaml(existing_leaf) {
        let Some(value) = override_value.as_str() else {
            return Ok(override_value);
        };

        let Some(normalized) = normalize_offset_datetime_string(value) else {
            return Ok(override_value);
        };

        let parsed = serde_yaml::from_value::<OffsetDateTime>(YamlValue::String(normalized))
            .map_err(|source| StepError::InvalidArgument {
                message: format!(
                    "Invalid config override path '{full_path}': invalid datetime '{value}': {source}"
                ),
            })?;

        return serde_yaml::to_value(parsed).map_err(|source| StepError::InvalidArgument {
            message: format!(
                "Invalid config override path '{full_path}': failed to serialize datetime '{value}': {source}"
            ),
        });
    }

    if is_byte_sequence_like_yaml(existing_leaf) {
        let Some(value) = override_value.as_str() else {
            return Ok(override_value);
        };
        let hex = value.trim_start_matches("0x").trim_start_matches("0X");
        if !is_hex_string(hex) {
            return Ok(override_value);
        }

        let bytes = hex::decode(hex).map_err(|source| StepError::InvalidArgument {
            message: format!(
                "Invalid config override path '{full_path}': invalid hex bytes '{value}': {source}"
            ),
        })?;

        return Ok(YamlValue::Sequence(
            bytes
                .into_iter()
                .map(|byte| YamlValue::from(i64::from(byte)))
                .collect(),
        ));
    }

    Ok(override_value)
}

fn parse_duration_parts_from_override(value: &YamlValue) -> Option<(i64, u32)> {
    if let Some(seconds) = yaml_integer_to_i64(value) {
        return Some((seconds, 0));
    }

    if let Some(float_seconds) = value.as_f64() {
        if !float_seconds.is_finite() || float_seconds.is_sign_negative() {
            return None;
        }
        let seconds = float_seconds.trunc() as i64;
        let nanos =
            ((float_seconds.fract() * 1_000_000_000.0).round() as u64).min(999_999_999) as u32;
        return Some((seconds, nanos));
    }

    let raw = value.as_str()?.trim();
    let normalized = raw.replace(',', ".");
    if normalized.is_empty() {
        return None;
    }

    if let Some((seconds, fraction)) = normalized.split_once('.') {
        if seconds.is_empty() || !seconds.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        if fraction.is_empty() || !fraction.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }

        let seconds = seconds.parse::<i64>().ok()?;
        let nanos_text = if fraction.len() >= 9 {
            fraction.chars().take(9).collect::<String>()
        } else {
            format!("{fraction:0<9}")
        };
        let nanos = nanos_text.parse::<u32>().ok()?;
        return Some((seconds, nanos));
    }

    if raw.bytes().all(|b| b.is_ascii_digit()) {
        return Some((raw.parse::<i64>().ok()?, 0));
    }

    None
}

fn duration_parts_to_yaml_like_existing(
    existing_leaf: &YamlValue,
    seconds: i64,
    nanos: u32,
) -> YamlValue {
    match existing_leaf {
        YamlValue::Sequence(_) => YamlValue::Sequence(vec![
            YamlValue::from(seconds),
            YamlValue::from(i64::from(nanos)),
        ]),
        YamlValue::Mapping(_) => {
            let mut map = Mapping::new();
            map.insert(
                YamlValue::String("secs".to_owned()),
                YamlValue::from(seconds),
            );
            map.insert(
                YamlValue::String("nanos".to_owned()),
                YamlValue::from(i64::from(nanos)),
            );
            YamlValue::Mapping(map)
        }
        _ => YamlValue::String(format!("{seconds}.{nanos:09}")),
    }
}

fn is_offset_datetime_like_yaml(value: &YamlValue) -> bool {
    let Some(raw) = value.as_str() else {
        return false;
    };
    serde_yaml::from_value::<OffsetDateTime>(YamlValue::String(raw.to_owned())).is_ok()
}

fn normalize_offset_datetime_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let replaced_utc = trimmed
        .strip_suffix(" UTC")
        .map_or_else(|| trimmed.to_owned(), |base| format!("{base} +00:00:00"));

    let (date_time, offset) = replaced_utc.rsplit_once(' ')?;
    if !offset.starts_with('+') && !offset.starts_with('-') {
        return None;
    }

    if let Some((base, fraction)) = date_time.split_once('.') {
        if !fraction.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let nanos = if fraction.len() >= 9 {
            fraction.chars().take(9).collect::<String>()
        } else {
            format!("{fraction:0<9}")
        };
        return Some(format!("{base}.{nanos} {offset}"));
    }

    Some(format!("{date_time}.000000000 {offset}"))
}

fn is_byte_sequence_like_yaml(value: &YamlValue) -> bool {
    let Some(values) = value.as_sequence() else {
        return false;
    };
    !values.is_empty()
        && values
            .iter()
            .all(|v| v.as_i64().is_some_and(|n| (0..=255).contains(&n)))
}

fn is_hex_string(value: &str) -> bool {
    !value.is_empty()
        && value.len().is_multiple_of(2)
        && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn yaml_value_at_path<'a>(current: &'a YamlValue, path: &[&str]) -> Option<&'a YamlValue> {
    if path.is_empty() {
        return Some(current);
    }

    let segment = path[0];
    if let Ok(index) = segment.parse::<usize>() {
        let sequence = current.as_sequence()?;
        return yaml_value_at_path(sequence.get(index)?, &path[1..]);
    }

    let mapping = current.as_mapping()?;
    let key = YamlValue::String(segment.to_owned());
    yaml_value_at_path(mapping.get(&key)?, &path[1..])
}

fn yaml_integer_to_i64(value: &YamlValue) -> Option<i64> {
    value.as_i64().or_else(|| {
        let u = value.as_u64()?;
        i64::try_from(u).ok()
    })
}

fn is_duration_like_yaml(value: &YamlValue) -> bool {
    match value {
        YamlValue::String(v) => is_duration_like_string(v),
        YamlValue::Sequence(v) => {
            v.len() == 2
                && yaml_integer_to_i64(&v[0]).is_some()
                && yaml_integer_to_i64(&v[1]).is_some()
        }
        YamlValue::Mapping(v) => {
            let secs = v
                .get(YamlValue::String("secs".to_owned()))
                .and_then(yaml_integer_to_i64);
            let nanos = v
                .get(YamlValue::String("nanos".to_owned()))
                .and_then(yaml_integer_to_i64);
            secs.is_some() && nanos.is_some()
        }
        _ => false,
    }
}

fn is_duration_like_string(value: &str) -> bool {
    let Some((secs, nanos)) = value.split_once('.') else {
        return false;
    };
    !secs.is_empty()
        && secs.as_bytes().iter().all(u8::is_ascii_digit)
        && nanos.len() == 9
        && nanos.as_bytes().iter().all(u8::is_ascii_digit)
}

fn set_yaml_value_at_path(
    current: &mut YamlValue,
    path: &[&str],
    value: YamlValue,
    full_path: &str,
) -> Result<(), StepError> {
    if path.is_empty() {
        *current = value;
        return Ok(());
    }

    let segment = path[0];
    let is_last = path.len() == 1;

    if let Ok(index) = segment.parse::<usize>() {
        if current.is_null() {
            *current = YamlValue::Sequence(Vec::new());
        }

        let sequence =
            current
                .as_sequence_mut()
                .ok_or_else(|| StepError::InvalidArgument {
                    message: format!(
                        "Invalid user config override path '{full_path}': segment '{segment}' expects a YAML sequence"
                    ),
                })?;

        if sequence.len() <= index {
            sequence.resize(index + 1, YamlValue::Null);
        }

        if is_last {
            sequence[index] = value;
            return Ok(());
        }

        return set_yaml_value_at_path(&mut sequence[index], &path[1..], value, full_path);
    }

    if current.is_null() {
        *current = YamlValue::Mapping(Mapping::new());
    }

    let mapping = current
        .as_mapping_mut()
        .ok_or_else(|| StepError::InvalidArgument {
            message: format!(
                "Invalid user config override path '{full_path}': segment '{segment}' expects a YAML mapping"
            ),
        })?;

    let key = YamlValue::String(segment.to_owned());
    if is_last {
        mapping.insert(key, value);
        return Ok(());
    }

    let next_is_sequence = path[1].parse::<usize>().is_ok();
    let next_default = if next_is_sequence {
        YamlValue::Sequence(Vec::new())
    } else {
        YamlValue::Mapping(Mapping::new())
    };
    let child = mapping.entry(key).or_insert(next_default);
    if child.is_null() {
        *child = if next_is_sequence {
            YamlValue::Sequence(Vec::new())
        } else {
            YamlValue::Mapping(Mapping::new())
        };
    }

    set_yaml_value_at_path(child, &path[1..], value, full_path)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use lb_libp2p::Multiaddr;
    use time::Duration as TimeDuration;

    use super::*;
    use crate::{
        add_strings,
        nodes::create_validator_config,
        topology::configs::{
            create_general_configs, deployment::e2e_deployment_settings_with_genesis_tx,
        },
    };

    #[test]
    fn normalize_user_config_path_rejects_empty_segments() {
        let path = "network..backend";
        assert!(normalize_custom_config_path("test-step", path).is_err());
    }

    #[test]
    fn set_yaml_value_at_path_supports_nested_mapping() {
        let mut root = YamlValue::Mapping(Mapping::new());
        set_yaml_value_at_path(
            &mut root,
            &["network", "backend", "swarm", "gossipsub", "retain_scores"],
            serde_yaml::from_str::<YamlValue>("20").expect("yaml value"),
            "network.backend.swarm.gossipsub.retain_scores",
        )
        .expect("path should be writable");

        let got = root["network"]["backend"]["swarm"]["gossipsub"]["retain_scores"].as_i64();
        assert_eq!(got, Some(20));
    }

    #[test]
    fn set_yaml_value_at_path_supports_sequence_indices() {
        let mut root = YamlValue::Mapping(Mapping::new());
        set_yaml_value_at_path(
            &mut root,
            &["network", "backend", "initial_peers", "1"],
            serde_yaml::from_str::<YamlValue>("/ip4/127.0.0.1/udp/3000/quic-v1")
                .expect("yaml value"),
            "network.backend.initial_peers.1",
        )
        .expect("path should be writable");

        let sequence = root["network"]["backend"]["initial_peers"]
            .as_sequence()
            .expect("sequence");
        assert_eq!(sequence.len(), 2);
        assert_eq!(
            sequence[1].as_str(),
            Some("/ip4/127.0.0.1/udp/3000/quic-v1")
        );
    }

    #[test]
    fn test_apply_custom_config_overrides() {
        let (configs, genesis_tx) = create_general_configs(1, Some("test_set_config_overrides"));
        let deployment_settings = e2e_deployment_settings_with_genesis_tx(genesis_tx);
        let mut config = create_validator_config(configs[0].clone(), deployment_settings);

        // User configs
        let retain_scores = config.user.network.backend.swarm.gossipsub.retain_scores;
        let override_1 = ConfigOverride {
            path: "network.backend.swarm.gossipsub.retain_scores".to_owned(),
            value: (retain_scores + 10).into(),
        };
        let read_only = config.user.storage.backend.read_only;
        let override_2 = ConfigOverride {
            path: "storage.backend.read_only".to_owned(),
            value: serde_yaml::to_value(!read_only).expect("yaml value"),
        };
        let override_3 = ConfigOverride {
            path: "cryptarchia.service.bootstrap.prolonged_bootstrap_period".to_owned(),
            value: 0.into(),
        };
        assert!(
            apply_user_config_overrides(&mut config, &[override_1, override_2, override_3]).is_ok()
        );
        assert_eq!(
            config.user.network.backend.swarm.gossipsub.retain_scores,
            retain_scores + 10
        );
        assert_eq!(config.user.storage.backend.read_only, !read_only);
        assert_eq!(
            config
                .user
                .cryptarchia
                .service
                .bootstrap
                .prolonged_bootstrap_period,
            Duration::ZERO
        );

        // Deployment configs
        let slot_duration = config.deployment.time.slot_duration;
        let slot_duration_override =
            TimeDuration::try_from(slot_duration + Duration::from_secs(1)).unwrap();
        let override_4 = ConfigOverride {
            path: "time.slot_duration".to_owned(),
            value: serde_yaml::to_value(slot_duration_override).expect("yaml value"),
        };
        let pubsub_topic = config.deployment.mempool.pubsub_topic.clone();
        let override_5 = ConfigOverride {
            path: "mempool.pubsub_topic".to_owned(),
            value: serde_yaml::to_value(add_strings!(&[&pubsub_topic, "_test_1234"]))
                .expect("yaml value"),
        };
        assert!(apply_deployment_config_overrides(&mut config, &[override_4, override_5]).is_ok());
        assert_eq!(
            config.deployment.time.slot_duration,
            slot_duration + Duration::from_secs(1)
        );
        assert_eq!(
            config.deployment.mempool.pubsub_topic,
            add_strings!(&[&pubsub_topic, "_test_1234"])
        );
    }

    #[test]
    fn test_world_override_duration_user_friendly_formats() {
        let (configs, genesis_tx) = create_general_configs(1, Some("test_override_friendly"));
        let deployment_settings = e2e_deployment_settings_with_genesis_tx(genesis_tx);
        let mut config = create_validator_config(configs[0].clone(), deployment_settings);
        let mut world = CucumberWorld::default();

        set_user_config_override(
            &mut world,
            "test-step",
            "cryptarchia.service.bootstrap.prolonged_bootstrap_period",
            "1,2",
        )
        .expect("user duration comma override");
        set_user_config_override(
            &mut world,
            "test-step",
            "cryptarchia.service.bootstrap.offline_grace_period.state_recording_interval",
            "1.200000000",
        )
        .expect("user duration dot override");
        set_user_config_override(
            &mut world,
            "test-step",
            "network.backend.swarm.gossipsub.heartbeat_interval",
            "1",
        )
        .expect("user duration int override");

        set_deployment_config_override(&mut world, "test-step", "time.slot_duration", "1")
            .expect("deployment duration int override");
        set_deployment_config_override(
            &mut world,
            "test-step",
            "time.chain_start_time",
            "2026-04-13 06:45:24 +00:00:00",
        )
        .expect("deployment chain_start_time without nanos");
        set_deployment_config_override(
            &mut world,
            "test-step",
            "time.chain_start_time",
            "2026-04-13 06:45:24 UTC",
        )
        .expect("deployment chain_start_time UTC");

        apply_user_config_overrides(&mut config, &world.user_config_overrides)
            .expect("apply user overrides");
        apply_deployment_config_overrides(&mut config, &world.deployment_config_overrides)
            .expect("apply deployment overrides");

        assert_eq!(
            config
                .user
                .cryptarchia
                .service
                .bootstrap
                .prolonged_bootstrap_period,
            Duration::from_millis(1200)
        );
        assert_eq!(
            config
                .user
                .cryptarchia
                .service
                .bootstrap
                .offline_grace_period
                .state_recording_interval,
            Duration::from_millis(1200)
        );
        assert_eq!(
            config
                .user
                .network
                .backend
                .swarm
                .gossipsub
                .heartbeat_interval,
            Duration::from_secs(1)
        );
        assert_eq!(config.deployment.time.slot_duration, Duration::from_secs(1));

        let expected_chain_start = serde_yaml::from_value::<OffsetDateTime>(YamlValue::String(
            "2026-04-13 06:45:24.000000000 +00:00:00".to_owned(),
        ))
        .expect("expected timestamp");
        assert_eq!(
            config.deployment.time.chain_start_time,
            expected_chain_start
        );
    }

    // Tests that all scalar field types found in the user/deployment configs can be
    // overridden via `set_*_config_override` and survive a full apply round-trip.
    //
    // Types covered: bool, usize, f64, String, Multiaddr, hex-string ZkPublicKey /
    // SecretKey.
    #[test]
    fn test_world_override_scalar_types() {
        let (configs, genesis_tx) = create_general_configs(1, Some("test_override_scalar_types"));
        let deployment_settings = e2e_deployment_settings_with_genesis_tx(genesis_tx);
        let mut config = create_validator_config(configs[0].clone(), deployment_settings);
        let mut world = CucumberWorld::default();
        // bool: validate_messages
        set_user_config_override(
            &mut world,
            "test-step",
            "network.backend.swarm.gossipsub.validate_messages",
            "true",
        )
        .expect("bool override");
        // bool: force_bootstrap
        set_user_config_override(
            &mut world,
            "test-step",
            "cryptarchia.service.bootstrap.force_bootstrap",
            "true",
        )
        .expect("bool force_bootstrap override");
        // integer (usize): history_length
        set_user_config_override(
            &mut world,
            "test-step",
            "network.backend.swarm.gossipsub.history_length",
            "5",
        )
        .expect("usize override");
        // f64: gossip_factor
        set_user_config_override(
            &mut world,
            "test-step",
            "network.backend.swarm.gossipsub.gossip_factor",
            "0.5",
        )
        .expect("f64 override");
        // String: pubsub_topic
        set_deployment_config_override(
            &mut world,
            "test-step",
            "mempool.pubsub_topic",
            "my-custom-topic",
        )
        .expect("string override");
        // Multiaddr: listening_address
        set_user_config_override(
            &mut world,
            "test-step",
            "blend.core.backend.listening_address",
            "/ip4/127.0.0.1/udp/20128/quic-v1",
        )
        .expect("multiaddr override");
        // hex string passthrough: ZkPublicKey (funding_pk)
        // ZkPublicKey::zero() is Fr::ZERO = 32 zero bytes = 64 hex zeros
        let funding_pk_hex = "0".repeat(64);
        set_user_config_override(
            &mut world,
            "test-step",
            "cryptarchia.leader.wallet.funding_pk",
            &funding_pk_hex,
        )
        .expect("zkpk hex string override");
        // hex string passthrough: SecretKey (node_key) — any 32 bytes is a valid
        // Ed25519 seed
        let node_key_hex = "01".repeat(32);
        set_user_config_override(
            &mut world,
            "test-step",
            "network.backend.swarm.node_key",
            &node_key_hex,
        )
        .expect("node_key hex string override");
        apply_user_config_overrides(&mut config, &world.user_config_overrides)
            .expect("apply user overrides");
        apply_deployment_config_overrides(&mut config, &world.deployment_config_overrides)
            .expect("apply deployment overrides");
        assert!(
            config
                .user
                .network
                .backend
                .swarm
                .gossipsub
                .validate_messages
        );
        assert!(config.user.cryptarchia.service.bootstrap.force_bootstrap);
        assert_eq!(
            config.user.network.backend.swarm.gossipsub.history_length,
            5
        );
        assert!((config.user.network.backend.swarm.gossipsub.gossip_factor - 0.5f64).abs() < 1e-9);
        assert_eq!(config.deployment.mempool.pubsub_topic, "my-custom-topic");
        assert_eq!(
            config.user.blend.core.backend.listening_address,
            "/ip4/127.0.0.1/udp/20128/quic-v1"
                .parse::<Multiaddr>()
                .expect("multiaddr"),
        );
        assert_eq!(
            config.user.cryptarchia.leader.wallet.funding_pk,
            lb_key_management_system_service::keys::ZkPublicKey::zero(),
        );
    }

    // Tests that `coerce_override_value_for_existing_path` converts a hex string to
    // a byte-sequence YAML value when the existing leaf is a sequence of small
    // integers.
    #[test]
    fn test_coerce_hex_to_byte_sequence() {
        // Use 4 elements so it is not mistaken for Duration's [secs, nanos] 2-element
        // shape.
        let existing_bytes: Vec<YamlValue> = vec![
            YamlValue::from(0i64),
            YamlValue::from(1i64),
            YamlValue::from(2i64),
            YamlValue::from(3i64),
        ];
        let root = YamlValue::Mapping({
            let mut m = Mapping::new();
            m.insert(
                YamlValue::String("data".to_owned()),
                YamlValue::Sequence(existing_bytes),
            );
            m
        });
        for hex_input in &["deadbeef", "0xdeadbeef"] {
            let result = coerce_override_value_for_existing_path(
                &root,
                &["data"],
                YamlValue::String((*hex_input).to_owned()),
                "data",
            )
            .expect("coerce should succeed");
            let seq = result.as_sequence().expect("result should be a sequence");
            assert_eq!(seq.len(), 4, "input: {hex_input}");
            assert_eq!(seq[0].as_i64(), Some(0xde), "input: {hex_input}");
            assert_eq!(seq[1].as_i64(), Some(0xad), "input: {hex_input}");
            assert_eq!(seq[2].as_i64(), Some(0xbe), "input: {hex_input}");
            assert_eq!(seq[3].as_i64(), Some(0xef), "input: {hex_input}");
        }
    }
}
