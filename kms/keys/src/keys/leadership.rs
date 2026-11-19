use std::fmt::{Debug, Formatter};

use lb_groth16::Fr;
use serde::Deserialize;
use zeroize::ZeroizeOnDrop;

use super::{ZkPublicKey, errors::KeyError, kdf_public_key, secured_key::SecuredKey};

/// Credentials for leadership only. Neither secret is a spending key.
#[derive(Clone, Deserialize, PartialEq, Eq, ZeroizeOnDrop)]
#[cfg_attr(feature = "unsafe", derive(serde::Serialize))]
pub struct LeadershipKey {
    #[serde(with = "lb_groth16::serde::serde_fr")]
    nullifier_key: Fr,
    #[serde(with = "lb_groth16::serde::serde_fr")]
    staking_secret_key: Fr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, Deserialize)]
pub struct LeadershipPublicKeys {
    pub nullifier_public_key: ZkPublicKey,
    pub staking_public_key: ZkPublicKey,
}

impl LeadershipKey {
    #[must_use]
    pub const fn new(nullifier_key: Fr, staking_secret_key: Fr) -> Self {
        Self {
            nullifier_key,
            staking_secret_key,
        }
    }

    #[must_use]
    pub const fn nullifier_key(&self) -> Fr {
        self.nullifier_key
    }

    #[must_use]
    pub const fn staking_secret_key(&self) -> Fr {
        self.staking_secret_key
    }
}

impl Debug for LeadershipKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("LeadershipKey(<redacted>)")
    }
}

#[async_trait::async_trait]
impl SecuredKey for LeadershipKey {
    type Payload = ();
    type Signature = ();
    type PublicKey = LeadershipPublicKeys;
    type Error = KeyError;

    fn sign(&self, (): &Self::Payload) -> Result<Self::Signature, Self::Error> {
        Err(KeyError::UnsupportedKey(
            "leadership credentials cannot sign".into(),
        ))
    }

    fn sign_multiple(_: &[&Self], (): &Self::Payload) -> Result<Self::Signature, Self::Error> {
        Err(KeyError::UnsupportedKey(
            "leadership credentials cannot sign".into(),
        ))
    }

    fn as_public_key(&self) -> Self::PublicKey {
        LeadershipPublicKeys {
            nullifier_public_key: kdf_public_key(&self.nullifier_key),
            staking_public_key: kdf_public_key(&self.staking_secret_key),
        }
    }
}
