use serde::{Deserialize, Serialize};

/// Runtime controls for security-audit-only leader behavior.
///
/// Audit behavior is disabled when this setting is omitted or set to zero.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct SecurityAuditSettings {
    /// Additional valid same-parent, same-slot blocks to produce after a
    /// genuine leadership win.
    #[serde(default)]
    pub sibling_blocks_per_leadership: usize,
}

impl SecurityAuditSettings {
    #[must_use]
    pub const fn is_disabled(&self) -> bool {
        self.sibling_blocks_per_leadership == 0
    }
}
