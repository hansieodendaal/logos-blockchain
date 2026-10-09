Feature: Security audit

  @security_audit @siblings @consensus
  Scenario: Honest peers validate valid same-slot sibling blocks
    Given I have a cluster with capacity of 4 nodes
    And the first 4 nodes are declared as blend providers
    And I have user config override "cryptarchia.leader.security_audit.sibling_blocks_per_leadership" as "0"
    When I start node "NODE_2"
    And I start peer node "NODE_3" connected to node "NODE_2"
    And I start peer node "NODE_4" connected to node "NODE_2"
    And I have user config override "cryptarchia.leader.security_audit.sibling_blocks_per_leadership" as "4"
    And I start peer node "NODE_1" connected to node "NODE_2"
    Then Blend core peers are ready and NODE_1 produces four valid siblings accepted by an honest node
    And I stop all nodes

  @security_audit @siblings @transactions
  Scenario: Honest peers validate nonempty same-slot sibling blocks
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
      | NODE_2    | 2             | SECURITY_AUDIT_RECEIVER |              |
      | NODE_3    | 3             | SECURITY_AUDIT_PEER_3   | NODE_2       |
      | NODE_4    | 4             | SECURITY_AUDIT_PEER_4   | NODE_2       |
    And I have user config override "cryptarchia.leader.security_audit.sibling_blocks_per_leadership" as "4"
    And I start nodes with wallet resources:
      | node_name | account_index | wallet_name           | connected_to |
      | NODE_1    | 1             | SECURITY_AUDIT_SENDER | NODE_2       |
    Then NODE_1 produces four valid nonempty siblings containing submitted transactions
    And I stop all nodes
