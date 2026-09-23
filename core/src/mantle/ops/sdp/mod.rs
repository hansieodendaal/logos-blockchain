pub mod active;
pub mod declare;
pub mod withdraw;

pub use active::{SDPActiveExecutionContext, SDPActiveValidationContext};
pub use declare::{SDPDeclareExecutionContext, SDPDeclareVerificationContext};
use lb_cryptarchia_engine::Epoch;
use thiserror::Error;
pub use withdraw::{SDPWithdrawExecutionContext, SDPWithdrawValidationContext};

use crate::{
    mantle::NoteId,
    sdp::{DeclarationId, ProviderId, ServiceType},
};

pub type SDPDeclareOp = crate::sdp::DeclarationMessage;
pub type SDPWithdrawOp = crate::sdp::WithdrawMessage;
pub type SDPActiveOp = crate::sdp::ActiveMessage;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SdpError {
    #[error("Note: {0:?} isn't in the ledger")]
    InexistingNote(NoteId),
    #[error("Note {0:?} is a channel note and cannot be used as service collateral")]
    ChannelNote(NoteId),
    #[error("Invalid SDP declare ZkSignature")]
    InvalidZkSignature,
    #[error("Invalid SDP declare EDDSA signature")]
    InvalidEddsaSignature,
    #[error("Duplicate sdp declaration id: {0:?}")]
    DuplicateDeclaration(DeclarationId),
    #[error("Duplicate provider_id within service {service_type:?}: {provider_id:?}")]
    DuplicateProviderId {
        service_type: ServiceType,
        provider_id: Box<ProviderId>,
    },
    #[error("Note {note_id:?} insufficient value: {value}")]
    NoteInsufficientValue { note_id: NoteId, value: u64 },
    #[error("Note {note_id:?} already used for service {service_type:?}")]
    NoteAlreadyUsedForService {
        note_id: NoteId,
        service_type: ServiceType,
    },
    #[error(
        "An unexpected error occurred during sdp declare execution, please validate the op before executing"
    )]
    UnexpectedError,
    #[error("Sdp declaration id could not be found: {0:?}")]
    DeclarationNotFound(DeclarationId),
    #[error("Service type could not be found: {0:?}")]
    ServiceNotFound(ServiceType),
    #[error(
        "Sdp declaration has been already scheduled to be withdrawn: {declaration_id:?} at epoch {withdraw_at:?}"
    )]
    DeclarationWithdrawn {
        declaration_id: DeclarationId,
        withdraw_at: Epoch,
    },
    #[error(
        "Invalid SDP nonce lifecycle: message_epoch={message_epoch:?}, declaration_created={declaration_created:?}"
    )]
    InvalidNonceLifecycle {
        message_epoch: Epoch,
        declaration_created: Epoch,
    },
    #[error(
        "Invalid SDP nonce sequence: message_sequence={message_sequence}, declaration_sequence={declaration_sequence}"
    )]
    InvalidNonceSequence {
        message_sequence: u32,
        declaration_sequence: u32,
    },
    #[error("Note is not a service note: {0:?}")]
    NotAServiceNote(NoteId),
    #[error("Note {note_id:?} not used for {service_type:?}")]
    NoteNotUsedForService {
        note_id: NoteId,
        service_type: ServiceType,
    },
}
