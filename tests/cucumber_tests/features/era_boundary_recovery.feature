Feature: Era boundary recovery

  @era_boundary_recovery
  Scenario: IBD recovers after an in-flight old-era block request expires
    Given era-boundary recovery evidence is active
    And the era-transition participants are:
      | node   | role      | canonical_reference |
      | NODE_1 | provider  | yes                 |
      | NODE_2 | requester | no                  |
    And I configure initial proposal publication disabled for all era-transition participants
    And the provider participant holds its response stream after 1 blocks
    And the cluster uses parameter set "cucumber_fixture"
    And no nodes are declared as blend providers
    And the cluster uses era schedule from era 0 to era 1 starting at epoch 1
    And I have a cluster with capacity of 2 nodes
    And we use IBD peers
    And the cluster uses prolonged bootstrap period of 0 seconds
    And I have user config override "network.backend.swarm.chain_sync.peer_response_timeout" as "seconds(300)"
    And I have user config override "network.backend.swarm.chain_sync.inbound_download_stream_timeout" as "seconds(300)"
    And I start the provider participant with its configured era-transition response hold
    Then the running nodes use the same effective deployment
    And the provider participant produces at least 24 blocks before era activation within 120 seconds
    When I capture a non-genesis finalized block before the era boundary on the canonical reference participant
    And I start the requester participant connected to the provider participant with tip polling disabled
    Then the running nodes use the same effective deployment
    And all era-transition participants report proposal publishing disabled
    And the requester receives and applies its first F0 block while the provider holds the next response within 120 seconds
    When all nodes enter the target era within 260 seconds
    Then all nodes report the target-era fork digest within 30 seconds
    And transition expiry cancels the held F0 request and the requester detects the interruption within 30 seconds
    And the requester automatically replaces the interrupted request under F1 and the provider accepts it within 90 seconds
    And the requester retains the first downloaded block and completes IBD within 180 seconds
    And all declared era-transition participants have at least 12 blocks and canonical histories converge to within 1 blocks within 180 seconds
    And all nodes preserve the captured finalized block on their LIB ancestry after the era boundary within 120 seconds
    And all era-transition participants remain healthy after IBD recovery
    And all era-transition participants report proposal publishing disabled
    When I enable proposal publishing on all era-transition participants
    Then all era-transition participants report proposal publishing enabled
