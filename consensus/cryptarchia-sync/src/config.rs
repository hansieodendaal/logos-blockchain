use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_with::{DurationMilliSeconds, serde_as};

const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_INCOMING_REQUEST_QUEUE_TIMEOUT: Duration = Duration::from_secs(2);

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    /// The maximum duration to wait for a peer to respond
    /// with a message.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(default = "default_response_timeout")]
    pub peer_response_timeout: Duration,
    /// The maximum duration to keep an inbound request waiting
    /// in the local queue before dropping it.
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    #[serde(default = "default_incoming_request_queue_timeout")]
    pub incoming_request_queue_timeout: Duration,
}

const fn default_response_timeout() -> Duration {
    DEFAULT_RESPONSE_TIMEOUT
}

const fn default_incoming_request_queue_timeout() -> Duration {
    DEFAULT_INCOMING_REQUEST_QUEUE_TIMEOUT
}
