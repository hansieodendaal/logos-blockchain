use lb_binary_codec::canonical::BinaryEncode as _;
use lb_cryptarchia_engine::Epoch;
use lb_key_management_system_keys::keys::{ZkSignature, public_inputs_from_pks};
use lb_log_targets::mantle;
use tracing::debug;

use super::{SDPWithdrawOp, SdpError};
use crate::{
    events::TxEvent,
    mantle::{
        batch::DeferredZkpVerification,
        gas::{Gas, MainnetGasProfile, OpGasCalculator, OperationGas},
        ledger::{
            Declarations, ExecutableOperation, PreverifiableOperation, ProvableOperation,
            VerifiableOperation,
            verification_mode::{StandardMode, VerificationMode},
        },
        ops::{OpId, SignedOperation},
        transactions::{
            hash::TxHashView,
            states::{Preverified, Unverified, Verified},
        },
    },
    sdp::{self, service_notes::ServiceNotes},
};

const LOG_TARGET: &str = mantle::sdp::message::WITHDRAW;

pub struct SDPWithdrawValidationContext<'a> {
    pub declarations: &'a Declarations,
    pub epoch: Epoch,
    pub service_notes: &'a ServiceNotes,
    pub tx_hash_view: &'a TxHashView,
}

pub struct SDPWithdrawExecutionContext {
    pub declarations: Declarations,
    pub service_notes: ServiceNotes,
    pub epoch: Epoch,
}

impl ProvableOperation for SDPWithdrawOp {
    type Proof = ZkSignature;
    const CODE: u8 = 0x21;
}

impl OpId for SDPWithdrawOp {
    fn op_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }
}

impl OperationGas<MainnetGasProfile> for SDPWithdrawOp {
    const GAS_COST: Gas = Gas::new(590);
}

impl OpGasCalculator<MainnetGasProfile> for SDPWithdrawOp {}

impl PreverifiableOperation<StandardMode>
    for SignedOperation<SDPWithdrawOp, Unverified, StandardMode>
{
    type Context<'a> = ();
    type Error = SdpError;

    fn preverify(&self, _context: &Self::Context<'_>) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl VerifiableOperation<StandardMode>
    for SignedOperation<SDPWithdrawOp, Preverified, StandardMode>
{
    type Context<'a> = SDPWithdrawValidationContext<'a>;
    type Error = SdpError;

    fn verify(
        &self,
        context: &Self::Context<'_>,
    ) -> Result<Option<DeferredZkpVerification>, Self::Error> {
        let operation = self.operation();

        // Check that the declaration exists
        let Some(declaration) = context.declarations.get(&operation.declaration_id) else {
            return Err(SdpError::DeclarationNotFound(operation.declaration_id));
        };

        // Check that the declaration hasn't been already scheduled to be withdrawn.
        if let Some(withdraw_at) = declaration.withdraw_at {
            return Err(SdpError::DeclarationWithdrawn {
                declaration_id: operation.declaration_id,
                withdraw_at,
            });
        }

        let service_note_id = declaration.service_note_id;

        // The note is obtained from the stored declaration and must still be
        // bound to this exact declaration in the service-note index.
        let Some(note) = context.service_notes.get(&service_note_id) else {
            return Err(SdpError::NotAServiceNote(service_note_id));
        };
        if !context.service_notes.is_used_by_declaration(
            &service_note_id,
            &declaration.service_type,
            &operation.declaration_id,
        ) {
            return Err(SdpError::NoteNotUsedForService {
                note_id: service_note_id,
                service_type: declaration.service_type,
            });
        }

        // The signed nonce commits to the declaration lifecycle. Compare that
        // lifecycle explicitly before checking the sequence within it.
        if operation.nonce.lifecycle_epoch() != declaration.created {
            return Err(SdpError::InvalidNonceLifecycle {
                message_epoch: operation.nonce.lifecycle_epoch(),
                declaration_created: declaration.created,
            });
        }
        if operation.nonce.sequence() <= declaration.nonce.sequence() {
            return Err(SdpError::InvalidNonceSequence {
                message_sequence: operation.nonce.sequence(),
                declaration_sequence: declaration.nonce.sequence(),
            });
        }

        // Defer the proof verification so that the caller can batch it.
        // Ensure service note pk and zk_id attached to this declaration authorized this
        // Operation.
        let inputs = public_inputs_from_pks(
            (*context.tx_hash_view.as_fr()).into(),
            &[note.pk, declaration.zk_id],
        )
        .map_err(|_| SdpError::InvalidZkSignature)?;
        Ok(Some(DeferredZkpVerification::ZkSig(
            *self.proof().as_proof(),
            inputs,
        )))
    }
}

impl<Mode: VerificationMode> ExecutableOperation
    for SignedOperation<SDPWithdrawOp, Verified, Mode>
{
    type Context<'a> = SDPWithdrawExecutionContext;
    type Error = SdpError;

    fn execute<'a>(
        &self,
        mut context: Self::Context<'a>,
    ) -> Result<(Self::Context<'a>, Vec<TxEvent>), Self::Error> {
        let operation = self.operation();

        let declaration = context
            .declarations
            .get_mut(&operation.declaration_id)
            .expect("The operation should have been validated");

        // Delay the withdrawal by `SNAPSHOT_FINALIZATION_DELAY` epochs
        // to prevent "stake-less service provision".
        // Otherwise, providers can continue providing the service even after
        // withdrawal because SDP uses the snapshot from `SNAPSHOT_FINALIZATION_DELAY`
        // epochs ago.
        // The note will be unlocked once the withdrawn epoch set here is reached.
        declaration.withdraw_at = Some(context.epoch.strict_add(sdp::SNAPSHOT_FINALIZATION_DELAY));
        declaration.nonce = operation.nonce;

        debug!(
            target: LOG_TARGET,
            provider_id = ?declaration.provider_id,
            withdraw_at = ?declaration.withdraw_at,
            nonce = ?declaration.nonce,
            "updated declaration with withdraw message"
        );

        Ok((context, Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use lb_key_management_system_keys::keys::ZkKey;
    use num_bigint::BigUint;

    use super::*;
    use crate::{
        mantle::{
            Note, NoteId, TxHash,
            batch::{Error as BatchError, test_utils::batch_verify},
            gas::test_utils::FixedThresholds,
            ops::op_proof::samples::SampleProof as _,
        },
        sdp::{Declaration, DeclarationId, DeclarationMessage, MinStake, Nonce, ServiceType},
    };

    fn note_key() -> ZkKey {
        ZkKey::from(BigUint::from(1u8))
    }

    fn declaration_key() -> ZkKey {
        ZkKey::from(BigUint::from(2u8))
    }

    fn locked_notes(service_note_id: &NoteId, declaration_id: DeclarationId) -> ServiceNotes {
        ServiceNotes::new()
            .lock(
                &MinStake {
                    threshold: 0,
                    timestamp: 0,
                },
                ServiceType::BlendNetwork,
                declaration_id,
                Note::new(10_000, note_key().to_public_key()),
                service_note_id,
            )
            .expect("the note covers the minimum stake")
    }

    fn declaration(service_note_id: NoteId) -> Declaration {
        Declaration::new(
            Epoch::from(0),
            &DeclarationMessage {
                zk_id: declaration_key().to_public_key(),
                service_note_id,
                ..DeclarationMessage::sample()
            },
        )
    }

    fn declarations(operation: &SDPWithdrawOp, declaration: Declaration) -> Declarations {
        Declarations::new_sync().insert(operation.declaration_id, declaration)
    }

    fn preverified(
        operation: SDPWithdrawOp,
        tx_hash_view: &TxHashView,
    ) -> SignedOperation<SDPWithdrawOp, Preverified, StandardMode> {
        let proof = ZkKey::multi_sign(&[note_key(), declaration_key()], tx_hash_view.as_fr())
            .expect("signing should succeed");

        SignedOperation::<_, Unverified, StandardMode>::new(operation, proof)
            .into_preverified(&())
            .expect("preverify accepts every withdraw message")
    }

    #[test]
    fn preverify_accepts_every_withdraw_message() {
        let signed_operation = SignedOperation::<_, Unverified, StandardMode>::new(
            SDPWithdrawOp::sample(),
            <SDPWithdrawOp as ProvableOperation>::Proof::sample(),
        );

        assert_eq!(signed_operation.preverify(&()), Ok(()));
    }

    #[test]
    fn verify_rejects_an_unknown_declaration() {
        let operation = SDPWithdrawOp::sample();
        let declaration_id = operation.declaration_id;
        let service_note_id = DeclarationMessage::sample().service_note_id;
        let service_notes = locked_notes(&service_note_id, declaration_id);
        let declarations = Declarations::new_sync();

        let signed_view = TxHashView::from(TxHash::from([9u8; 32]));
        let signed_operation = preverified(operation, &signed_view);

        assert_eq!(
            signed_operation
                .verify(&SDPWithdrawValidationContext {
                    declarations: &declarations,
                    epoch: Epoch::from(0),
                    service_notes: &service_notes,
                    tx_hash_view: &signed_view,
                })
                .unwrap_err(),
            SdpError::DeclarationNotFound(declaration_id)
        );
    }

    #[test]
    fn verify_rejects_a_declaration_already_scheduled_for_withdrawal() {
        let operation = SDPWithdrawOp::sample();
        let declaration_id = operation.declaration_id;
        let service_note_id = DeclarationMessage::sample().service_note_id;
        let service_notes = locked_notes(&service_note_id, declaration_id);

        let withdraw_at = Epoch::from(7);
        let declaration = Declaration {
            withdraw_at: Some(withdraw_at),
            ..declaration(service_note_id)
        };
        let declarations = declarations(&operation, declaration);

        let signed_view = TxHashView::from(TxHash::from([9u8; 32]));
        let signed_operation = preverified(operation, &signed_view);

        assert_eq!(
            signed_operation
                .verify(&SDPWithdrawValidationContext {
                    declarations: &declarations,
                    epoch: Epoch::from(0),
                    service_notes: &service_notes,
                    tx_hash_view: &signed_view,
                })
                .unwrap_err(),
            SdpError::DeclarationWithdrawn {
                declaration_id,
                withdraw_at,
            }
        );
    }

    #[test]
    fn verify_rejects_a_note_missing_from_service_note_state() {
        let operation = SDPWithdrawOp::sample();
        let note_id = DeclarationMessage::sample().service_note_id;
        let service_notes = ServiceNotes::new();
        let declarations = declarations(&operation, declaration(note_id));

        let signed_view = TxHashView::from(TxHash::from([9u8; 32]));
        let signed_operation = preverified(operation, &signed_view);

        assert_eq!(
            signed_operation
                .verify(&SDPWithdrawValidationContext {
                    declarations: &declarations,
                    epoch: Epoch::from(0),
                    service_notes: &service_notes,
                    tx_hash_view: &signed_view,
                })
                .unwrap_err(),
            SdpError::NotAServiceNote(note_id)
        );
    }

    #[test]
    fn verify_rejects_a_service_note_bound_to_another_declaration() {
        let operation = SDPWithdrawOp::sample();
        let note_id = DeclarationMessage::sample().service_note_id;
        let service_notes = locked_notes(&note_id, DeclarationId([99; 32]));
        let declarations = declarations(&operation, declaration(note_id));

        let signed_view = TxHashView::from(TxHash::from([9u8; 32]));
        let signed_operation = preverified(operation, &signed_view);

        assert_eq!(
            signed_operation
                .verify(&SDPWithdrawValidationContext {
                    declarations: &declarations,
                    epoch: Epoch::from(0),
                    service_notes: &service_notes,
                    tx_hash_view: &signed_view,
                })
                .unwrap_err(),
            SdpError::NoteNotUsedForService {
                note_id,
                service_type: ServiceType::BlendNetwork,
            }
        );
    }

    fn deferred_zkp_signed_by(signers: &[ZkKey]) -> Option<DeferredZkpVerification> {
        let operation = SDPWithdrawOp::sample();
        let service_note_id = DeclarationMessage::sample().service_note_id;
        let service_notes = locked_notes(&service_note_id, operation.declaration_id);
        let declarations = declarations(&operation, declaration(service_note_id));
        let tx_hash_view = TxHashView::from(TxHash::from([9u8; 32]));
        let proof =
            ZkKey::multi_sign(signers, tx_hash_view.as_fr()).expect("signing should succeed");

        SignedOperation::<_, Unverified, StandardMode>::new(operation, proof)
            .into_preverified(&())
            .expect("preverify accepts every withdraw message")
            .verify(&SDPWithdrawValidationContext {
                declarations: &declarations,
                epoch: Epoch::from(0),
                service_notes: &service_notes,
                tx_hash_view: &tx_hash_view,
            })
            .expect("verify leaves the proof to the batch")
    }

    #[test]
    fn deferred_zkp_is_accepted() {
        assert!(batch_verify(deferred_zkp_signed_by(&[note_key(), declaration_key()])).is_ok());
    }

    #[test]
    fn wrong_deferred_zkp_is_rejected() {
        assert!(matches!(
            batch_verify(deferred_zkp_signed_by(&[declaration_key()])),
            Err(BatchError::InvalidZkSignatures)
        ));
    }

    #[test]
    fn verify_rejects_a_nonce_that_does_not_increase() {
        let operation = SDPWithdrawOp {
            nonce: Nonce::new(Epoch::new(0), 0),
            ..SDPWithdrawOp::sample()
        };
        let service_note_id = DeclarationMessage::sample().service_note_id;
        let service_notes = locked_notes(&service_note_id, operation.declaration_id);
        let declarations = declarations(&operation, declaration(service_note_id));

        let signed_view = TxHashView::from(TxHash::from([9u8; 32]));
        let signed_operation = preverified(operation, &signed_view);

        assert_eq!(
            signed_operation
                .verify(&SDPWithdrawValidationContext {
                    declarations: &declarations,
                    epoch: Epoch::from(0),
                    service_notes: &service_notes,
                    tx_hash_view: &signed_view,
                })
                .unwrap_err(),
            SdpError::InvalidNonceSequence {
                message_sequence: 0,
                declaration_sequence: 0,
            }
        );
    }

    #[test]
    fn verify_rejects_a_lower_sequence_in_the_same_lifecycle() {
        let operation = SDPWithdrawOp {
            nonce: Nonce::new(Epoch::new(0), 3),
            ..SDPWithdrawOp::sample()
        };
        let service_note_id = DeclarationMessage::sample().service_note_id;
        let service_notes = locked_notes(&service_note_id, operation.declaration_id);
        let declaration = Declaration {
            nonce: Nonce::new(Epoch::new(0), 4),
            ..declaration(service_note_id)
        };
        let declarations = declarations(&operation, declaration);
        let signed_view = TxHashView::from(TxHash::from([9u8; 32]));

        assert_eq!(
            preverified(operation, &signed_view)
                .verify(&SDPWithdrawValidationContext {
                    declarations: &declarations,
                    epoch: Epoch::from(0),
                    service_notes: &service_notes,
                    tx_hash_view: &signed_view,
                })
                .unwrap_err(),
            SdpError::InvalidNonceSequence {
                message_sequence: 3,
                declaration_sequence: 4,
            }
        );
    }

    #[test]
    fn verify_rejects_a_nonce_from_another_declaration_lifecycle() {
        let operation = SDPWithdrawOp {
            nonce: Nonce::new(Epoch::new(1), 100),
            ..SDPWithdrawOp::sample()
        };
        let service_note_id = DeclarationMessage::sample().service_note_id;
        let service_notes = locked_notes(&service_note_id, operation.declaration_id);
        let declaration = declaration(service_note_id);
        let declarations = declarations(&operation, declaration.clone());
        let signed_view = TxHashView::from(TxHash::from([9u8; 32]));

        assert_eq!(
            preverified(operation, &signed_view)
                .verify(&SDPWithdrawValidationContext {
                    declarations: &declarations,
                    epoch: declaration.created,
                    service_notes: &service_notes,
                    tx_hash_view: &signed_view,
                })
                .unwrap_err(),
            SdpError::InvalidNonceLifecycle {
                message_epoch: Epoch::new(1),
                declaration_created: declaration.created,
            }
        );
    }

    #[test]
    fn verify_accepts_a_sequence_jump_within_the_same_lifecycle() {
        let operation = SDPWithdrawOp {
            nonce: Nonce::new(Epoch::new(0), 100),
            ..SDPWithdrawOp::sample()
        };
        let service_note_id = DeclarationMessage::sample().service_note_id;
        let service_notes = locked_notes(&service_note_id, operation.declaration_id);
        let declaration = declaration(service_note_id);
        let declarations = declarations(&operation, declaration.clone());
        let signed_view = TxHashView::from(TxHash::from([9u8; 32]));

        assert!(
            preverified(operation, &signed_view)
                .verify(&SDPWithdrawValidationContext {
                    declarations: &declarations,
                    epoch: declaration.created,
                    service_notes: &service_notes,
                    tx_hash_view: &signed_view,
                })
                .unwrap()
                .is_some()
        );
    }

    fn verified(
        operation: SDPWithdrawOp,
    ) -> SignedOperation<SDPWithdrawOp, Verified, StandardMode> {
        SignedOperation::<_, Unverified, StandardMode>::new(
            operation,
            <SDPWithdrawOp as ProvableOperation>::Proof::sample(),
        )
        .into_state_trusted()
    }

    #[test]
    fn execute_schedules_the_withdrawal_after_the_snapshot_delay() {
        let operation = SDPWithdrawOp::sample();
        let declaration_id = operation.declaration_id;
        let locked_note_id = DeclarationMessage::sample().service_note_id;
        let nonce = operation.nonce;
        let service_notes = locked_notes(&locked_note_id, operation.declaration_id);
        let declarations = declarations(&operation, declaration(locked_note_id));
        let epoch = Epoch::from(4);

        let (context, events) = verified(operation)
            .execute(SDPWithdrawExecutionContext {
                declarations,
                service_notes,
                epoch,
            })
            .expect("the declaration is registered");

        let updated = context
            .declarations
            .get(&declaration_id)
            .expect("the declaration stays registered");
        assert_eq!(
            updated.withdraw_at,
            Some(epoch.strict_add(sdp::SNAPSHOT_FINALIZATION_DELAY))
        );
        assert_eq!(updated.nonce, nonce);
        assert!(
            context
                .service_notes
                .is_used_for_service(&locked_note_id, &ServiceType::BlendNetwork)
        );
        assert!(events.is_empty());
    }

    #[test]
    #[should_panic(expected = "The operation should have been validated")]
    fn execute_panics_on_a_declaration_the_ledger_does_not_hold() {
        let operation = SDPWithdrawOp::sample();
        let service_note_id = DeclarationMessage::sample().service_note_id;
        let service_notes = locked_notes(&service_note_id, operation.declaration_id);

        drop(verified(operation).execute(SDPWithdrawExecutionContext {
            declarations: Declarations::new_sync(),
            service_notes,
            epoch: Epoch::from(4),
        }));
    }

    #[test]
    fn sdp_withdraw_op_execution_gas_does_not_scale_with_the_threshold() {
        for threshold in [0, 1, 3] {
            assert_eq!(
                SDPWithdrawOp::sample().execution_gas(&FixedThresholds(threshold)),
                Ok(Gas::new(590))
            );
        }
    }
}
