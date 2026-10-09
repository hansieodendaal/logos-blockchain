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
