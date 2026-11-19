use std::fmt::{Debug, Formatter};

use lb_core::mantle::{
    Note,
    notes::{NoteCommitment, NoteNullifier, derive_note_nullifier},
};
use lb_key_management_system_keys::keys::{
    LeadershipKey, ZkKey,
    errors::KeyError,
    secured_key::{SecureKeyOperator, SecuredKey as _},
};
use lb_log_targets::kms;
use tokio::sync::oneshot;
use tracing::debug;

const LOG_TARGET: &str = kms::operators::ZK_NULLIFIER;

pub struct DeriveLeadershipNullifier {
    note: Note,
    result_channel: oneshot::Sender<NoteNullifier>,
}

impl Debug for DeriveLeadershipNullifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("DeriveLeadershipNullifier")
    }
}

impl DeriveLeadershipNullifier {
    #[must_use]
    pub const fn new(note: Note, result_channel: oneshot::Sender<NoteNullifier>) -> Self {
        Self {
            note,
            result_channel,
        }
    }
}

#[async_trait::async_trait]
impl SecureKeyOperator for DeriveLeadershipNullifier {
    type Key = LeadershipKey;
    type Error = KeyError;

    async fn execute(self: Box<Self>, key: &Self::Key) -> Result<(), Self::Error> {
        let public = key.as_public_key();
        if self.note.public_key != public.nullifier_public_key
            || self.note.stake_public_key != public.staking_public_key
            || self.note.stake_public_key
                == lb_key_management_system_keys::keys::ZkPublicKey::zero()
        {
            return Err(KeyError::UnsupportedKey(
                "leadership credentials do not match note".into(),
            ));
        }
        let _ = self.result_channel.send(derive_note_nullifier(
            &self.note.commitment(),
            &key.nullifier_key(),
        ));
        Ok(())
    }
}

pub struct DeriveNullifier {
    commitment: NoteCommitment,
    result_channel: oneshot::Sender<NoteNullifier>,
}

impl Debug for DeriveNullifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeriveNullifier")
    }
}

impl DeriveNullifier {
    #[must_use]
    pub const fn new(
        commitment: NoteCommitment,
        result_channel: oneshot::Sender<NoteNullifier>,
    ) -> Self {
        Self {
            commitment,
            result_channel,
        }
    }
}

#[async_trait::async_trait]
impl SecureKeyOperator for DeriveNullifier {
    type Key = ZkKey;
    type Error = KeyError;

    async fn execute(self: Box<Self>, key: &Self::Key) -> Result<(), Self::Error> {
        let Self {
            commitment,
            result_channel,
        } = *self;
        if result_channel
            .send(derive_note_nullifier(&commitment, &key.nullifier_key()))
            .is_err()
        {
            debug!(target: LOG_TARGET, "Failed to send result via channel");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use lb_groth16::Fr;

    use super::*;

    #[tokio::test]
    async fn derive_nullifier_hashes_the_commitment_with_the_nullifier_key() {
        let key = ZkKey::new(Fr::from(7u64));
        let commitment = NoteCommitment(Fr::from(3u64));
        let (sender, receiver) = oneshot::channel();
        key.execute(DeriveNullifier::new(commitment, sender))
            .await
            .unwrap();
        assert_eq!(
            receiver.await.unwrap(),
            derive_note_nullifier(&commitment, &key.nullifier_key())
        );
    }
}
