@security_audit @security_audit_v1
Feature: Security audit

  @security_audit @siblings @consensus
  Scenario: Honest peers validate valid same-slot sibling blocks
    # A genuine NODE_01 win must yield four distinct siblings. At least one must
    # reach an honest node through ordered Blend, reconstruction and acceptance.
    Given I have a cluster with capacity of 4 nodes
    And the first 4 nodes are declared as blend providers
    And I have user config override "cryptarchia.leader.security_audit.sibling_blocks_per_leadership" as "0"
    When I start node "NODE_02"
    And I start peer node "NODE_03" connected to node "NODE_02"
    And I start peer node "NODE_04" connected to node "NODE_02"
    And I have user config override "cryptarchia.leader.security_audit.sibling_blocks_per_leadership" as "4"
    And I start peer node "NODE_01" connected to node "NODE_02"
    Given the security audit participants are:
      | node    | role     | canonical_reference |
      | NODE_01 | producer | no                  |
      | NODE_02 | honest   | yes                 |
      | NODE_03 | honest   | no                  |
      | NODE_04 | honest   | no                  |
    Given the audit producer is configured to produce 4 siblings
    Then all declared audit nodes establish Blend readiness within 60 seconds
    When the audit producer completes a genuine leadership opportunity with at least 4 valid siblings within 300 seconds
    Then at least 1 sibling proposals are accepted by declared honest nodes through ordered Blend-to-consensus paths within 30 seconds
    And I stop all nodes

  @security_audit @siblings @transactions
  Scenario: Honest peers validate nonempty same-slot sibling blocks
    # Fund real transactions, compare retrievable ordinary and sibling bodies,
    # and observe an accepted sibling on an honest peer.
    Given the genesis block has the following wallet resources:
      | account_index | token_count | token_amount |
      | 1             | 16          | 1000         |
      | 2             | 0           | 0            |
      | 3             | 0           | 0            |
      | 4             | 0           | 0            |
    And we have a sponsored genesis fee account with 32 tokens of 20000 value each
    And I have a cluster with capacity of 4 nodes
    And the first 4 nodes are declared as blend providers
    And I have user config override "cryptarchia.leader.security_audit.sibling_blocks_per_leadership" as "0"
    When I start nodes with wallet resources:
      | node_name | account_index | wallet_name             | connected_to |
      | NODE_02   | 2             | SECURITY_AUDIT_RECEIVER |              |
      | NODE_03   | 3             | SECURITY_AUDIT_PEER_3   | NODE_02      |
      | NODE_04   | 4             | SECURITY_AUDIT_PEER_4   | NODE_02      |
    And I have user config override "cryptarchia.leader.security_audit.sibling_blocks_per_leadership" as "4"
    And I start nodes with wallet resources:
      | node_name | account_index | wallet_name           | connected_to |
      | NODE_01   | 1             | SECURITY_AUDIT_SENDER | NODE_02      |
    Given the security audit participants are:
      | node    | role     | canonical_reference |
      | NODE_01 | producer | no                  |
      | NODE_02 | honest   | yes                 |
      | NODE_03 | honest   | no                  |
      | NODE_04 | honest   | no                  |
    Given the audit producer is configured to produce 4 siblings
    And the audit workload sends 10 from "SECURITY_AUDIT_SENDER" to "SECURITY_AUDIT_RECEIVER"
    And the nonempty selection submits at most 16 transactions within 180 seconds and samples each pending transaction for 6 seconds
    Then all declared audit nodes establish Blend readiness within 60 seconds
    When the audit producer completes a genuine nonempty opportunity with at least 4 valid siblings
    Then the selected ordinary block contains a submitted transaction and its siblings preserve its identity
    And at least 1 sibling proposals are accepted by declared honest nodes through ordered Blend-to-consensus paths within 30 seconds
    And accepted siblings have the ordinary block's body root and ordered transactions
    And I stop all nodes

  @security_audit @siblings @finality
  Scenario: Honest nodes converge and finalize after nonempty sibling equivocation
    # Observe a qualifying opportunity, then stop the producer. Honest nodes must make
    # progress, align within one block and maintain the same LIB beyond the
    # attack slot, with verified ancestry and no finalized-history conflict or
    # LIB continuity regression. Stop the producer promptly and record any
    # further opportunities; finality must pass the last observed attack slot.
    Given the genesis block has the following wallet resources:
      | account_index | token_count | token_amount |
      | 1             | 16          | 1000         |
      | 2             | 0           | 0            |
      | 3             | 0           | 0            |
      | 4             | 0           | 0            |
    And we have a sponsored genesis fee account with 32 tokens of 20000 value each
    And I have a cluster with capacity of 4 nodes
    And the cluster uses diagnostic parameter set "testnet_representative"
    And the first 4 nodes are declared as blend providers
    And I have user config override "cryptarchia.leader.security_audit.sibling_blocks_per_leadership" as "0"
    When I start nodes with wallet resources:
      | node_name | account_index | wallet_name             | connected_to |
      | NODE_02   | 2             | SECURITY_AUDIT_RECEIVER |              |
      | NODE_03   | 3             | SECURITY_AUDIT_PEER_3   | NODE_02      |
      | NODE_04   | 4             | SECURITY_AUDIT_PEER_4   | NODE_02      |
    And I have user config override "cryptarchia.leader.security_audit.sibling_blocks_per_leadership" as "4"
    And I start nodes with wallet resources:
      | node_name | account_index | wallet_name           | connected_to |
      | NODE_01   | 1             | SECURITY_AUDIT_SENDER | NODE_02      |
    Given the security audit participants are:
      | node    | role     | canonical_reference |
      | NODE_01 | producer | no                  |
      | NODE_02 | honest   | yes                 |
      | NODE_03 | honest   | no                  |
      | NODE_04 | honest   | no                  |
    Given the audit producer is configured to produce 4 siblings
    And the audit workload sends 10 from "SECURITY_AUDIT_SENDER" to "SECURITY_AUDIT_RECEIVER"
    And the nonempty selection submits at most 16 transactions within 180 seconds and samples each pending transaction for 6 seconds
    Then all declared audit nodes establish Blend readiness within 60 seconds
    When the audit producer completes a genuine nonempty opportunity with at least 4 valid siblings
    Then the selected ordinary block contains a submitted transaction and its siblings preserve its identity
    # Stop on reception of a selected sibling; complete acceptance is checked afterwards.
    When the audit producer is stopped upon the first selected sibling's Blend reception by a declared honest node within 30 seconds
    Then at least 1 sibling proposals are accepted by declared honest nodes through ordered Blend-to-consensus paths within 30 seconds
    And accepted siblings have the ordinary block's body root and ordered transactions
    Then the honest nodes converge with tip height skew at most 1 and finalize beyond the nonempty equivocation for 10 seconds within 600 seconds, with producer stopped within 30 seconds
    And I stop all nodes

  @security_audit @siblings @sustained
  Scenario: Honest peers recover after sustained nonempty sibling equivocation
    # NODE_01 continues participating while honest nodes sample canonical tips,
    # finalized history and reorganizations. Enqueue is not peer acceptance.
    Given the genesis block has the following wallet resources:
      | account_index | token_count | token_amount |
      | 1             | 80          | 1000         |
      | 2             | 0           | 0            |
      | 3             | 0           | 0            |
      | 4             | 0           | 0            |
    And we have a sponsored genesis fee account with 160 tokens of 20000 value each
    And I have a cluster with capacity of 4 nodes
    And the cluster uses diagnostic parameter set "testnet_representative"
    And the first 4 nodes are declared as blend providers
    And I have user config override "cryptarchia.leader.security_audit.sibling_blocks_per_leadership" as "0"
    When I start nodes with wallet resources:
      | node_name | account_index | wallet_name             | connected_to |
      | NODE_02   | 2             | SECURITY_AUDIT_RECEIVER |              |
      | NODE_03   | 3             | SECURITY_AUDIT_PEER_3   | NODE_02      |
      | NODE_04   | 4             | SECURITY_AUDIT_PEER_4   | NODE_02      |
    And I have user config override "cryptarchia.leader.security_audit.sibling_blocks_per_leadership" as "4"
    And I start nodes with wallet resources:
      | node_name | account_index | wallet_name           | connected_to |
      | NODE_01   | 1             | SECURITY_AUDIT_SENDER | NODE_02      |
    Given the security audit participants are:
      | node    | role     | canonical_reference |
      | NODE_01 | producer | no                  |
      | NODE_02 | honest   | yes                 |
      | NODE_03 | honest   | no                  |
      | NODE_04 | honest   | no                  |
    Given the audit producer is configured to produce 4 siblings
    And the audit workload sends 10 from "SECURITY_AUDIT_SENDER" to "SECURITY_AUDIT_RECEIVER"
    Then the sustained equivocation meets these exposure and cooldown criteria
      | criterion                        | value |
      | complete_opportunities           | 6     |
      | nonempty_submitted_opportunities | 3     |
      | delivered_opportunities          | 2     |
      | cooldown_canonical_blocks        | 10    |
      | minimum_active_seconds           | 300   |
      | active_timeout_seconds           | 1800  |
      | transaction_budget               | 80    |
      | initial_pending_transactions     | 8     |
      | submission_interval_seconds      | 25    |
      | readiness_seconds                | 60    |
      | maximum_tip_height_skew          | 1     |
    # The cooldown counts NODE_02 descendants of its start tip; a reorganization
    # away from that tip restarts it. NODE_01 stays online during the cooldown.
    When I stop node "NODE_01"
    # All three nodes must respond and progress, align within one block, and
    # prove tip/LIB ancestry and LIB continuity without a finalized conflict.
    Then the honest nodes maintain common finality beyond "last attack slot" for 10 seconds within 600 seconds
      | criterion                   | requirement      |
      | honest_nodes_responsive     | all              |
      | honest_nodes_make_progress  | all              |
      | maximum_tip_height_skew     | 1                |
      | common_lib                  | same ID and slot |
      | tip_and_finalized_ancestry  | verified         |
      | lib_continuity              | verified         |
      | finalized_history_conflicts | none             |
      | producer_stopped_and_quiet  | yes              |
    And I stop all nodes

  @security_audit @sustained_control
  Scenario: Honest peers under equivalent workload without sibling equivocation
    # Use an independent network with zero siblings and the same workload.
    Given the genesis block has the following wallet resources:
      | account_index | token_count | token_amount |
      | 1             | 80          | 1000         |
      | 2             | 0           | 0            |
      | 3             | 0           | 0            |
      | 4             | 0           | 0            |
    And we have a sponsored genesis fee account with 160 tokens of 20000 value each
    And I have a cluster with capacity of 4 nodes
    And the cluster uses diagnostic parameter set "testnet_representative"
    And the first 4 nodes are declared as blend providers
    And I have user config override "cryptarchia.leader.security_audit.sibling_blocks_per_leadership" as "0"
    When I start nodes with wallet resources:
      | node_name | account_index | wallet_name             | connected_to |
      | NODE_02   | 2             | SECURITY_AUDIT_RECEIVER |              |
      | NODE_03   | 3             | SECURITY_AUDIT_PEER_3   | NODE_02      |
      | NODE_04   | 4             | SECURITY_AUDIT_PEER_4   | NODE_02      |
    And I start nodes with wallet resources:
      | node_name | account_index | wallet_name           | connected_to |
      | NODE_01   | 1             | SECURITY_AUDIT_SENDER | NODE_02      |
    Given the security audit participants are:
      | node    | role     | canonical_reference |
      | NODE_01 | producer | no                  |
      | NODE_02 | honest   | yes                 |
      | NODE_03 | honest   | no                  |
      | NODE_04 | honest   | no                  |
    Given the audit producer is configured to produce 0 siblings
    And the audit workload sends 10 from "SECURITY_AUDIT_SENDER" to "SECURITY_AUDIT_RECEIVER"
    # NODE_02 must add at least as many canonical descendants as in the attack;
    # the attack report supplies that count. Missing ancestry is unverified.
    Then the control matches the attack's canonical block count with this workload
      | criterion                    | value |
      | transaction_budget           | 80    |
      | initial_pending_transactions | 8     |
      | submission_interval_seconds  | 25    |
      | active_timeout_seconds       | 1800  |
      | readiness_seconds            | 60    |
      | maximum_tip_height_skew      | 1     |
    When I stop node "NODE_01"
    # The control baseline is the highest honest LIB slot at the active end.
    # The same progress, alignment, ancestry and finality checks apply.
    Then the honest nodes maintain common finality beyond "control baseline" for 10 seconds within 600 seconds
      | criterion                   | requirement      |
      | honest_nodes_responsive     | all              |
      | honest_nodes_make_progress  | all              |
      | maximum_tip_height_skew     | 1                |
      | common_lib                  | same ID and slot |
      | tip_and_finalized_ancestry  | verified         |
      | lib_continuity              | verified         |
      | finalized_history_conflicts | none             |
      | producer_stopped_and_quiet  | yes              |
    And I stop all nodes
