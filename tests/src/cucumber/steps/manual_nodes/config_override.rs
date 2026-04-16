use lb_node::config::RunConfig;
use serde::{Serialize, de::DeserializeOwned};
use serde_yaml::{Mapping, Value as YamlValue};
use time::{Duration as TimeDuration, OffsetDateTime};

use crate::cucumber::{
    error::{StepError, StepResult},
    world::{ConfigOverride, CucumberWorld},
};

// ============================================================
// Public API
// ============================================================

pub fn set_user_config_override(
    world: &mut CucumberWorld,
    step: &str,
    raw_path: &str,
    raw_value: &str,
) -> StepResult {
    set_override(&mut world.user_config_overrides, step, raw_path, raw_value)
}

pub fn set_deployment_config_override(
    world: &mut CucumberWorld,
    step: &str,
    raw_path: &str,
    raw_value: &str,
) -> StepResult {
    set_override(
        &mut world.deployment_config_overrides,
        step,
        raw_path,
        raw_value,
    )
}

pub fn apply_user_config_overrides(
    config: &mut RunConfig,
    overrides: &[ConfigOverride],
) -> Result<(), StepError> {
    apply_overrides(&mut config.user, overrides, "user")
}

pub fn apply_deployment_config_overrides(
    config: &mut RunConfig,
    overrides: &[ConfigOverride],
) -> Result<(), StepError> {
    apply_overrides(&mut config.deployment, overrides, "deployment")
}

// ============================================================
// Override collection
// ============================================================

fn set_override(
    overrides: &mut Vec<ConfigOverride>,
    step: &str,
    raw_path: &str,
    raw_value: &str,
) -> StepResult {
    let path = normalize_path(step, raw_path)?;
    let value = parse_value(step, raw_value)?;

    if let Some(existing) = overrides.iter_mut().find(|item| item.path == path) {
        existing.value = value;
    } else {
        overrides.push(ConfigOverride { path, value });
    }

    Ok(())
}

fn normalize_path(step: &str, raw_path: &str) -> Result<String, StepError> {
    let trimmed = raw_path.trim();
    if trimmed.is_empty() {
        return Err(step_error(step, "config path must not be empty"));
    }

    let segments = trimmed
        .split('.')
        .map(str::trim)
        .map(|segment| {
            if segment.is_empty() {
                Err(step_error(
                    step,
                    &format!("config path '{trimmed}' must not contain empty segments"),
                ))
            } else {
                Ok(segment)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(segments.join("."))
}

fn parse_value(step: &str, raw_value: &str) -> Result<YamlValue, StepError> {
    let value = raw_value.trim();

    if let Some(relative_seconds) = parse_now_plus_seconds(value) {
        let timestamp = OffsetDateTime::now_utc() + TimeDuration::seconds(relative_seconds);
        return serde_yaml::to_value(timestamp).map_err(|source| {
            step_error(
                step,
                &format!("failed to convert config value '{value}' to YAML: {source}"),
            )
        });
    }

    serde_yaml::from_str::<YamlValue>(value)
        .map_or_else(|_| Ok(YamlValue::String(value.to_owned())), Ok)
}

fn parse_now_plus_seconds(value: &str) -> Option<i64> {
    value
        .strip_prefix("now+")?
        .strip_suffix('s')?
        .parse::<i64>()
        .ok()
}

// ============================================================
// Override application
// ============================================================

fn apply_overrides<T>(
    target: &mut T,
    overrides: &[ConfigOverride],
    scope: &str,
) -> Result<(), StepError>
where
    T: Serialize + DeserializeOwned,
{
    if overrides.is_empty() {
        return Ok(());
    }

    let mut yaml = to_yaml(target, scope)?;

    for ov in overrides {
        apply_override(&mut yaml, ov)?;
    }

    from_yaml(yaml, scope, target)
}

fn to_yaml<T>(target: &T, scope: &str) -> Result<YamlValue, StepError>
where
    T: Serialize,
{
    serde_yaml::to_value(target).map_err(|source| StepError::LogicalError {
        message: format!("failed to serialize {scope} config for patching: {source}"),
    })
}

fn from_yaml<T>(yaml: YamlValue, scope: &str, target: &mut T) -> Result<(), StepError>
where
    T: DeserializeOwned,
{
    *target = serde_yaml::from_value(yaml).map_err(|source| StepError::InvalidArgument {
        message: format!(
            "invalid {scope} config override: resulting config could not be deserialized: {source}"
        ),
    })?;

    Ok(())
}

fn apply_override(root: &mut YamlValue, ov: &ConfigOverride) -> Result<(), StepError> {
    let path = split_path(&ov.path);
    let value = coerce_at_path(root, &path, ov.value.clone(), &ov.path)?;
    set_at_path(root, &path, value, &ov.path)
}

fn split_path(path: &str) -> Vec<&str> {
    path.split('.').collect()
}

// ============================================================
// Type-aware coercion
// ============================================================

fn coerce_at_path(
    root: &YamlValue,
    path: &[&str],
    value: YamlValue,
    full_path: &str,
) -> Result<YamlValue, StepError> {
    let Some(existing) = get_at_path(root, path) else {
        return Ok(value);
    };

    if is_duration_like_yaml(existing) {
        return coerce_duration(existing, value, full_path);
    }

    if is_offset_datetime_like_yaml(existing) {
        return coerce_datetime(value, full_path);
    }

    if is_byte_sequence_like_yaml(existing) {
        return coerce_bytes(value, full_path);
    }

    Ok(value)
}

fn coerce_duration(
    existing: &YamlValue,
    value: YamlValue,
    full_path: &str,
) -> Result<YamlValue, StepError> {
    let Some((seconds, nanos)) = parse_duration_parts(&value) else {
        return Ok(value);
    };

    if seconds < 0 {
        return Err(invalid_path(
            full_path,
            &format!("negative duration '{seconds}' is not supported"),
        ));
    }

    Ok(duration_yaml(existing, seconds, nanos))
}

fn coerce_datetime(value: YamlValue, full_path: &str) -> Result<YamlValue, StepError> {
    let Some(raw) = value.as_str() else {
        return Ok(value);
    };

    let Some(normalized) = normalize_datetime(raw) else {
        return Ok(value);
    };

    let parsed = serde_yaml::from_value::<OffsetDateTime>(YamlValue::String(normalized)).map_err(
        |source| invalid_path(full_path, &format!("invalid datetime '{raw}': {source}")),
    )?;

    serde_yaml::to_value(parsed).map_err(|source| {
        invalid_path(
            full_path,
            &format!("failed to serialize datetime '{raw}': {source}"),
        )
    })
}

fn coerce_bytes(value: YamlValue, full_path: &str) -> Result<YamlValue, StepError> {
    let Some(raw) = value.as_str() else {
        return Ok(value);
    };

    let hex = raw.trim_start_matches("0x").trim_start_matches("0X");
    if !is_hex_string(hex) {
        return Ok(value);
    }

    let bytes = hex::decode(hex).map_err(|source| {
        invalid_path(full_path, &format!("invalid hex bytes '{raw}': {source}"))
    })?;

    Ok(YamlValue::Sequence(
        bytes
            .into_iter()
            .map(|byte| YamlValue::from(i64::from(byte)))
            .collect(),
    ))
}

// ============================================================
// Duration helpers
// ============================================================

fn parse_duration_parts(value: &YamlValue) -> Option<(i64, u32)> {
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

    parse_duration_string(value.as_str()?.trim())
}

fn parse_duration_string(raw: &str) -> Option<(i64, u32)> {
    let normalized = raw.replace(',', ".");
    if normalized.is_empty() {
        return None;
    }

    if let Some((seconds, fraction)) = normalized.split_once('.') {
        if !is_ascii_digits(seconds) || !is_ascii_digits(fraction) {
            return None;
        }

        let seconds = seconds.parse::<i64>().ok()?;
        let nanos = normalize_nanos(fraction).parse::<u32>().ok()?;
        return Some((seconds, nanos));
    }

    if is_ascii_digits(raw) {
        return Some((raw.parse::<i64>().ok()?, 0));
    }

    None
}

fn duration_yaml(existing: &YamlValue, seconds: i64, nanos: u32) -> YamlValue {
    match existing {
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

fn is_duration_like_yaml(value: &YamlValue) -> bool {
    match value {
        YamlValue::String(v) => is_duration_string(v),
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

fn is_duration_string(value: &str) -> bool {
    let Some((secs, nanos)) = value.split_once('.') else {
        return false;
    };

    is_ascii_digits(secs) && nanos.len() == 9 && is_ascii_digits(nanos)
}

// ============================================================
// Datetime helpers
// ============================================================

fn is_offset_datetime_like_yaml(value: &YamlValue) -> bool {
    value.as_str().is_some_and(|raw| {
        serde_yaml::from_value::<OffsetDateTime>(YamlValue::String(raw.to_owned())).is_ok()
    })
}

fn normalize_datetime(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let value = trimmed
        .strip_suffix(" UTC")
        .map_or_else(|| trimmed.to_owned(), |base| format!("{base} +00:00:00"));

    let (date_time, offset) = value.rsplit_once(' ')?;
    if !offset.starts_with('+') && !offset.starts_with('-') {
        return None;
    }

    if let Some((base, fraction)) = date_time.split_once('.') {
        if !is_ascii_digits(fraction) {
            return None;
        }
        return Some(format!("{base}.{} {offset}", normalize_nanos(fraction)));
    }

    Some(format!("{date_time}.000000000 {offset}"))
}

// ============================================================
// Byte helpers
// ============================================================

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

// ============================================================
// YAML traversal
// ============================================================

fn get_at_path<'a>(current: &'a YamlValue, path: &[&str]) -> Option<&'a YamlValue> {
    let mut current = current;

    for segment in path {
        current = if let Ok(index) = segment.parse::<usize>() {
            current.as_sequence()?.get(index)?
        } else {
            current
                .as_mapping()?
                .get(YamlValue::String((*segment).to_owned()))?
        };
    }

    Some(current)
}

fn set_at_path(
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
    let rest = &path[1..];
    let is_last = rest.is_empty();

    if let Ok(index) = segment.parse::<usize>() {
        return set_seq(current, segment, index, rest, value, full_path, is_last);
    }

    set_map(current, segment, rest, value, full_path, is_last)
}

fn set_seq(
    current: &mut YamlValue,
    segment: &str,
    index: usize,
    rest: &[&str],
    value: YamlValue,
    full_path: &str,
    is_last: bool,
) -> Result<(), StepError> {
    if current.is_null() {
        *current = YamlValue::Sequence(Vec::new());
    }

    let sequence = current.as_sequence_mut().ok_or_else(|| {
        invalid_path(
            full_path,
            &format!("segment '{segment}' expects a YAML sequence"),
        )
    })?;

    if sequence.len() <= index {
        sequence.resize(index + 1, YamlValue::Null);
    }

    if is_last {
        sequence[index] = value;
        return Ok(());
    }

    set_at_path(&mut sequence[index], rest, value, full_path)
}

fn set_map(
    current: &mut YamlValue,
    segment: &str,
    rest: &[&str],
    value: YamlValue,
    full_path: &str,
    is_last: bool,
) -> Result<(), StepError> {
    if current.is_null() {
        *current = YamlValue::Mapping(Mapping::new());
    }

    let mapping = current.as_mapping_mut().ok_or_else(|| {
        invalid_path(
            full_path,
            &format!("segment '{segment}' expects a YAML mapping"),
        )
    })?;

    let key = YamlValue::String(segment.to_owned());
    if is_last {
        mapping.insert(key, value);
        return Ok(());
    }

    let child = mapping.entry(key).or_insert_with(|| default_child(rest));
    if child.is_null() {
        *child = default_child(rest);
    }

    set_at_path(child, rest, value, full_path)
}

fn default_child(rest: &[&str]) -> YamlValue {
    if rest
        .first()
        .is_some_and(|segment| segment.parse::<usize>().is_ok())
    {
        YamlValue::Sequence(Vec::new())
    } else {
        YamlValue::Mapping(Mapping::new())
    }
}

// ============================================================
// Small utilities
// ============================================================

fn yaml_integer_to_i64(value: &YamlValue) -> Option<i64> {
    value.as_i64().or_else(|| {
        let u = value.as_u64()?;
        i64::try_from(u).ok()
    })
}

fn normalize_nanos(fraction: &str) -> String {
    if fraction.len() >= 9 {
        fraction.chars().take(9).collect()
    } else {
        format!("{fraction:0<9}")
    }
}

fn is_ascii_digits(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit())
}

fn step_error(step: &str, detail: &str) -> StepError {
    StepError::InvalidArgument {
        message: format!("step `{step}`: {detail}"),
    }
}

fn invalid_path(path: &str, detail: &str) -> StepError {
    StepError::InvalidArgument {
        message: format!("invalid config override at '{path}': {detail}"),
    }
}

// ============================================================
// Tests
// ============================================================

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
    fn normalize_path_rejects_empty_segments() {
        let path = "network..backend";
        assert!(normalize_path("test-step", path).is_err());
    }

    #[test]
    fn set_at_path_supports_nested_mapping() {
        let mut root = YamlValue::Mapping(Mapping::new());
        set_at_path(
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
    fn set_at_path_supports_sequence_indices() {
        let mut root = YamlValue::Mapping(Mapping::new());
        set_at_path(
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
    fn apply_overrides_updates_user_and_deployment_config() {
        let (configs, genesis_tx) = create_general_configs(1, Some("test_set_config_overrides"));
        let deployment_settings = e2e_deployment_settings_with_genesis_tx(genesis_tx);
        let mut config = create_validator_config(configs[0].clone(), deployment_settings);

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
    fn world_overrides_accept_friendly_duration_formats() {
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

    #[test]
    fn world_overrides_round_trip_scalar_types() {
        let (configs, genesis_tx) = create_general_configs(1, Some("test_override_scalar_types"));
        let deployment_settings = e2e_deployment_settings_with_genesis_tx(genesis_tx);
        let mut config = create_validator_config(configs[0].clone(), deployment_settings);
        let mut world = CucumberWorld::default();

        set_user_config_override(
            &mut world,
            "test-step",
            "network.backend.swarm.gossipsub.validate_messages",
            "true",
        )
        .expect("bool override");
        set_user_config_override(
            &mut world,
            "test-step",
            "cryptarchia.service.bootstrap.force_bootstrap",
            "true",
        )
        .expect("bool force_bootstrap override");
        set_user_config_override(
            &mut world,
            "test-step",
            "network.backend.swarm.gossipsub.history_length",
            "5",
        )
        .expect("usize override");
        set_user_config_override(
            &mut world,
            "test-step",
            "network.backend.swarm.gossipsub.gossip_factor",
            "0.5",
        )
        .expect("f64 override");
        set_deployment_config_override(
            &mut world,
            "test-step",
            "mempool.pubsub_topic",
            "my-custom-topic",
        )
        .expect("string override");
        set_user_config_override(
            &mut world,
            "test-step",
            "blend.core.backend.listening_address",
            "/ip4/127.0.0.1/udp/20128/quic-v1",
        )
        .expect("multiaddr override");

        let funding_pk_hex = "0".repeat(64);
        set_user_config_override(
            &mut world,
            "test-step",
            "cryptarchia.leader.wallet.funding_pk",
            &funding_pk_hex,
        )
        .expect("zkpk hex string override");

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

    #[test]
    fn coerce_bytes_converts_hex_string_to_byte_sequence() {
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
            let result = coerce_at_path(
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
