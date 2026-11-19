use std::collections::HashMap;

use lb_key_management_system_service::{
    backend::preload::KeyId,
    keys::{LeadershipPublicKeys, ZkPublicKey},
};
use lb_wallet_service::{default_fee_margin_percent, default_pending_note_expiry_blocks};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    /// Operator-requested reconstruction before the wallet becomes ready.
    #[serde(default)]
    pub historical_recovery: Vec<lb_wallet_service::RecoveryOpening>,
    /// Explicitly withdraw these pending authority request IDs at startup.
    #[serde(default)]
    pub historical_recovery_withdrawals: Vec<u64>,
    #[serde(default)]
    pub known_keys: HashMap<KeyId, ZkPublicKey>,
    #[serde(default)]
    pub leadership_keys: HashMap<KeyId, LeadershipPublicKeys>,
    /// The master key the fresh key of each mining run is derived from. The
    /// name is kept from the leader vouchers.
    pub voucher_master_key_id: KeyId,
    #[serde(default = "default_pending_note_expiry_blocks")]
    pub pending_note_expiry_blocks: u64,
    /// The priority fee percentage of the final mandatory fee the node funds
    /// its own transactions with.
    #[serde(default = "default_fee_margin_percent")]
    pub fee_margin_percent: u64,
}

pub struct RequiredValues {
    pub voucher_master_key_id: KeyId,
}

impl Config {
    #[must_use]
    pub fn with_required_values(
        RequiredValues {
            voucher_master_key_id,
        }: RequiredValues,
    ) -> Self {
        Self {
            historical_recovery: Vec::new(),
            historical_recovery_withdrawals: Vec::new(),
            known_keys: HashMap::new(),
            leadership_keys: HashMap::new(),
            voucher_master_key_id,
            pending_note_expiry_blocks: default_pending_note_expiry_blocks(),
            fee_margin_percent: default_fee_margin_percent(),
        }
    }
}
