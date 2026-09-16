use std::collections::HashMap;

use blake2::{Blake2b, Digest as _, digest::consts::U32};
use lb_utils::net::MAX_WIRE_MESSAGE_SIZE;
use libp2p::{PeerId, gossipsub};
use thiserror::Error;

pub mod swarm_ext;

/// An application payload maximum for one Gossipsub topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipsubTopicSizeLimit {
    pub topic: String,
    pub max_payload_size: usize,
}

impl GossipsubTopicSizeLimit {
    #[must_use]
    pub fn new(topic: impl Into<String>, max_payload_size: usize) -> Self {
        Self {
            topic: topic.into(),
            max_payload_size,
        }
    }
}

#[derive(Debug, Error)]
pub enum GossipsubTopicSizeLimitError {
    #[error(
        "Gossipsub topic `{topic}` payload maximum {max_payload_size} cannot fit below the global wire-message maximum {global_maximum}"
    )]
    PayloadExceedsGlobal {
        topic: String,
        max_payload_size: usize,
        global_maximum: usize,
    },
    #[error(
        "Gossipsub topic `{topic}` transmit maximum {transmit_size} exceeds the global wire-message maximum {global_maximum}"
    )]
    TransmitSizeExceedsGlobal {
        topic: String,
        transmit_size: usize,
        global_maximum: usize,
    },
    #[error("invalid Gossipsub configuration: {0}")]
    InvalidConfig(#[from] gossipsub::ConfigBuilderError),
}

/// Adds application payload limits to a Gossipsub config.
///
/// Gossipsub applies its per-topic limit differently on the send and receive
/// paths: outbound `publish` compares transformed data directly with the
/// configured limit, while inbound decoding compares the encoded protobuf
/// `Message` size. The current behaviour publishes with
/// `MessageAuthenticity::Author`, so the configured limit is derived from the
/// actual unsigned `RawMessage` representation containing the author, sequence
/// number, data, and raw topic. Application guards remain responsible for
/// enforcing the exact payload maximum on both paths.
/// The node currently derives that author from its configured Ed25519 identity
/// and publishes unsigned messages without signature or key fields. Changes to
/// the authentication mode, signing, identity representation, public-key
/// inclusion, or data transform require revisiting this envelope calculation.
pub fn configure_topic_size_limits(
    config: gossipsub::Config,
    author: PeerId,
    limits: impl IntoIterator<Item = GossipsubTopicSizeLimit>,
) -> Result<gossipsub::Config, GossipsubTopicSizeLimitError> {
    let mut topic_limits = HashMap::<gossipsub::TopicHash, usize>::new();

    for GossipsubTopicSizeLimit {
        topic,
        max_payload_size,
    } in limits
    {
        if max_payload_size >= MAX_WIRE_MESSAGE_SIZE {
            return Err(GossipsubTopicSizeLimitError::PayloadExceedsGlobal {
                topic,
                max_payload_size,
                global_maximum: MAX_WIRE_MESSAGE_SIZE,
            });
        }

        let topic_hash = gossipsub::IdentTopic::new(&topic).hash();
        let transmit_size = gossipsub_message_size(&topic_hash, &author, max_payload_size);
        if transmit_size > MAX_WIRE_MESSAGE_SIZE {
            return Err(GossipsubTopicSizeLimitError::TransmitSizeExceedsGlobal {
                topic,
                transmit_size,
                global_maximum: MAX_WIRE_MESSAGE_SIZE,
            });
        }

        topic_limits
            .entry(topic_hash)
            .and_modify(|maximum| *maximum = (*maximum).max(transmit_size))
            .or_insert(transmit_size);
    }

    let mut builder = gossipsub::ConfigBuilder::from(config);
    for (topic, transmit_size) in topic_limits {
        builder.max_transmit_size_for_topic(transmit_size, topic);
    }

    Ok(builder.build()?)
}

fn gossipsub_message_size(
    topic: &gossipsub::TopicHash,
    author: &PeerId,
    payload_size: usize,
) -> usize {
    gossipsub::RawMessage {
        source: Some(*author),
        data: vec![0; payload_size],
        sequence_number: Some(u64::MAX),
        topic: topic.clone(),
        signature: None,
        key: None,
        validated: true,
    }
    .raw_protobuf_len()
}

#[must_use]
pub fn compute_message_id(message: &gossipsub::Message) -> gossipsub::MessageId {
    let mut hasher = Blake2b::<U32>::new();
    hasher.update(&message.data);
    gossipsub::MessageId::from(hasher.finalize().to_vec())
}

#[cfg(test)]
mod tests {
    use libp2p::gossipsub::{ConfigBuilder, MessageAuthenticity, PublishError, ValidationMode};

    use super::*;

    fn author() -> PeerId {
        PeerId::random()
    }

    #[test]
    fn duplicate_topics_get_the_largest_derived_limit_in_any_order() {
        let author = author();
        let small = GossipsubTopicSizeLimit::new("shared", 512);
        let large = GossipsubTopicSizeLimit::new("shared", 1024);
        let topic_hash = gossipsub::IdentTopic::new("shared").hash();
        let expected = gossipsub_message_size(&topic_hash, &author, large.max_payload_size);

        for limits in [[small.clone(), large.clone()], [large, small]] {
            let config =
                configure_topic_size_limits(gossipsub::Config::default(), author, limits).unwrap();

            assert_eq!(config.max_transmit_size_for_topic(&topic_hash), expected);
        }
    }

    #[test]
    fn rejects_a_topic_limit_that_cannot_fit_under_the_global_ceiling() {
        let error = configure_topic_size_limits(
            gossipsub::Config::default(),
            author(),
            [GossipsubTopicSizeLimit::new(
                "too-large",
                MAX_WIRE_MESSAGE_SIZE,
            )],
        )
        .unwrap_err();

        assert!(matches!(
            error,
            GossipsubTopicSizeLimitError::PayloadExceedsGlobal { .. }
        ));
    }

    #[test]
    fn gossipsub_rejects_payloads_over_the_topic_limit() {
        let author = author();
        let topic = "transactions";
        let topic_hash = gossipsub::IdentTopic::new(topic).hash();
        let config = configure_topic_size_limits(
            gossipsub::Config::default(),
            author,
            [GossipsubTopicSizeLimit::new(topic, 512)],
        )
        .unwrap();
        let topic_limit = config.max_transmit_size_for_topic(&topic_hash);
        assert!(topic_limit < MAX_WIRE_MESSAGE_SIZE);
        let config = ConfigBuilder::from(config)
            .validation_mode(ValidationMode::None)
            .build()
            .unwrap();
        let mut behaviour = gossipsub::Behaviour::<
            gossipsub::IdentityTransform,
            gossipsub::AllowAllSubscriptionFilter,
        >::new(MessageAuthenticity::Author(author), config)
        .unwrap();

        assert!(matches!(
            behaviour.publish(gossipsub::IdentTopic::new(topic), vec![0; topic_limit + 1]),
            Err(PublishError::MessageTooLarge)
        ));
    }
}
