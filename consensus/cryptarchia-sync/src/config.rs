use std::{num::NonZeroUsize, time::Duration};

use serde::{Deserialize, Serialize};
use serde_with::{DurationMilliSeconds, serde_as};

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// The maximum duration to wait for a peer to respond
    /// with a message.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    pub peer_response_timeout: Duration,
    /// The maximum number of inbound block download requests that can be
    /// served concurrently. The same limit separately applies to inbound
    /// requests whose message is still being read. Tip requests are not
    /// counted against the download limit.
    pub max_inbound_requests: NonZeroUsize,
    /// The maximum duration to serve one inbound block download stream, from
    /// receiving the request until the stream is closed. Unlike
    /// `peer_response_timeout`, which bounds each message, this bounds the
    /// whole download, so a peer that keeps reading slowly cannot hold a
    /// download slot indefinitely.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    pub inbound_download_stream_timeout: Duration,
    /// Experimental controls for system-level era-transition regressions.
    /// Absent by default, including in builds with the feature enabled.
    #[cfg(feature = "testing-era-boundaries")]
    #[serde(default)]
    pub testing_era_boundaries: Option<EraBoundaryTestingConfig>,
}

/// Per-node controls for the experimental era-transition tests.
#[cfg(feature = "testing-era-boundaries")]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct EraBoundaryTestingConfig {
    /// Hold the provider after this many successfully sent blocks, but only if
    /// another valid block response is ready to send.
    pub hold_provider_after_blocks: Option<NonZeroUsize>,
}
