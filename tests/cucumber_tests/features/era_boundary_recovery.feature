Feature: Era boundary recovery

  # In each case, F0 is the pre-activation protocol. Rotation activates F1;
  # accepted F0 work may continue during transition grace and is cancelled only
  # at expiry if unfinished. Typed journals establish local event order and
  # protocol/request correlation; process clocks do not establish causality.

  # WP1: NODE_1 serves NODE_2's IBD. One response is applied before the F0
  # stream is held through grace and cancelled at expiry. The requester must
  # autonomously replace it under F1. Evidence covers partial-progress
  # retention, IBD completion, canonical convergence, finality and health; it
  # does not establish successful F0 completion during grace.
  @era_boundary_recovery
  Scenario: IBD recovers after an in-flight old-era block request expires
    # Bind the fault to the provider/requester pair and prevent unsolicited proposals.
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
    # Build a real IBD backlog and retain a finalized anchor before applying the hold.
    And the provider participant produces at least 24 blocks before era activation within 120 seconds
    When I capture a non-genesis finalized block before the era boundary on the canonical reference participant
    And I start the requester participant connected to the provider participant with tip polling disabled
    Then the running nodes use the same effective deployment
    And all era-transition participants report proposal publishing disabled
    And the requester receives and applies its first F0 block while the provider holds the next response within 120 seconds
    # Activation leaves the established stream alive during grace; expiry cancels it.
    When all nodes enter the target era within 260 seconds
    Then all nodes report the target-era fork digest within 30 seconds
    And transition expiry cancels the held F0 request and the requester detects the interruption within 30 seconds
    # Recovery must originate in the node and preserve already applied progress.
    And the requester automatically replaces the interrupted request under F1 and the provider accepts it within 90 seconds
    And the requester retains the first downloaded block and completes IBD within 180 seconds
    # Convergence, finalized ancestry, and health cover distinct outcomes.
    And all declared era-transition participants have at least 12 blocks and canonical histories converge to within 1 blocks within 180 seconds
    And all nodes preserve the captured finalized block on their LIB ancestry after the era boundary within 120 seconds
    And all era-transition participants remain healthy after IBD recovery
    And all era-transition participants report proposal publishing disabled
    When I enable proposal publishing on all era-transition participants
    Then all era-transition participants report proposal publishing enabled

  # WP2A: NODE_1 serves selected requester NODE_2 and unselected requester
  # NODE_3. Their distinguishable F0 lifetimes overlap; only NODE_2's response
  # is held after partial progress. NODE_3 must complete normally before the
  # selected request expires, after which NODE_2 autonomously recovers under
  # F1. This proves the declared two-request outcome, not every concurrency
  # or same-context collision pattern.
  @era_boundary_recovery @era_boundary_concurrent_requests
  Scenario: An unselected F0 request completes while a selected concurrent request expires
    # Select only one requester for the hold and keep proposal publication controlled.
    Given era-boundary recovery evidence is active
    And the era-transition participants are:
      | node   | role                 | canonical_reference |
      | NODE_1 | provider             | yes                 |
      | NODE_2 | requester            | no                  |
      | NODE_3 | unselected_requester | no                  |
    And I configure initial proposal publication disabled for all era-transition participants
    And the provider participant holds its response stream after 1 blocks
    And the cluster uses parameter set "cucumber_fixture"
    And no nodes are declared as blend providers
    And the cluster uses era schedule from era 0 to era 1 starting at epoch 1
    And I have a cluster with capacity of 3 nodes
    And the provider response hold selects the requester participant by peer identity
    And we use IBD peers
    And the cluster uses prolonged bootstrap period of 0 seconds
    And I have user config override "network.backend.swarm.chain_sync.peer_response_timeout" as "seconds(300)"
    And I have user config override "network.backend.swarm.chain_sync.inbound_download_stream_timeout" as "seconds(300)"
    And I have user config override "network.backend.swarm.kademlia.kbucket_inserts" as "manual"
    And I start the provider participant with its configured era-transition response hold
    Then the running nodes use the same effective deployment
    # Create overlapping F0 downloads with real progress on the selected request.
    And the provider participant produces at least 24 blocks before era activation within 120 seconds
    When I capture a non-genesis finalized block before the era boundary on the canonical reference participant
    And I start the requester participant connected to the provider participant with tip polling disabled
    Then the requester receives and applies its first F0 block while the provider holds the next response within 120 seconds
    When I start the unselected requester participant connected to the provider participant with tip polling enabled
    Then the selected F0 request overlaps with an unselected F0 request that completes normally within 120 seconds
    And the running nodes use the same effective deployment
    And all era-transition participants report proposal publishing disabled
    # Rotation preserves established work through grace; only expiry cancels the hold.
    When all nodes enter the target era within 260 seconds
    Then all nodes report the target-era fork digest within 30 seconds
    And transition expiry cancels the held F0 request and the requester detects the interruption within 30 seconds
    # The selected requester must recover on its own; the control already completed.
    And the requester automatically replaces the interrupted request under F1 and the provider accepts it within 90 seconds
    And the requester retains the first downloaded block and completes IBD within 180 seconds
    When I enable proposal publishing on participant "NODE_1"
    Then participant "NODE_1" reports proposal publishing enabled
    # Check shared history, finality, and health independently across participants.
    And all declared era-transition participants have at least 12 blocks and canonical histories converge to within 1 blocks within 180 seconds
    And all nodes preserve the captured finalized block on their LIB ancestry after the era boundary within 120 seconds
    And all era-transition participants remain healthy after IBD recovery
    When I enable proposal publishing on all era-transition participants
    Then all era-transition participants report proposal publishing enabled

  # A2-A4: NODE_1/NODE_2 form the explicitly selected provider/requester
  # interaction; NODE_3/NODE_4 form an independent control interaction. The
  # selected F0 stream remains held through grace and expires, while control
  # traffic completes without the fault. Evidence checks interaction-specific
  # correlation and automatic F1 recovery, as well as convergence, finality
  # and health for all four nodes. It establishes this topology, not all
  # possible peer/request collisions.
  @era_boundary_recovery @era_boundary_flexible_participants
  Scenario: Explicit interactions isolate the selected request across multiple providers and requesters
    # Bind separate selected and control interactions so the fault cannot apply globally.
    Given era-boundary recovery evidence is active
    And the era-transition participants are:
      | node   | role      | canonical_reference |
      | NODE_1 | provider  | yes                 |
      | NODE_2 | requester | no                  |
      | NODE_3 | provider  | no                  |
      | NODE_4 | requester | no                  |
    And the era-transition interactions are:
      | interaction | provider | requester | selected |
      | selected    | NODE_1   | NODE_2    | yes      |
      | control     | NODE_3   | NODE_4    | no       |
    And I configure initial proposal publication disabled for all era-transition participants
    And the provider participant holds its response stream after 1 blocks
    And the cluster uses parameter set "cucumber_fixture"
    And no nodes are declared as blend providers
    And the cluster uses era schedule from era 0 to era 1 starting at epoch 1
    And I have a cluster with capacity of 4 nodes
    And the provider response hold selects the requester participant by peer identity
    And we use IBD peers
    And the cluster uses prolonged bootstrap period of 0 seconds
    And I have user config override "network.backend.swarm.chain_sync.peer_response_timeout" as "seconds(300)"
    And I have user config override "network.backend.swarm.chain_sync.inbound_download_stream_timeout" as "seconds(300)"
    And I have user config override "network.backend.swarm.kademlia.kbucket_inserts" as "manual"
    And I start the provider participant with its configured era-transition response hold
    Then the running nodes use the same effective deployment
    # Establish selected F0 progress and an independent control download.
    And the provider participant produces at least 12 blocks before era activation within 120 seconds
    When I capture a non-genesis finalized block before the era boundary on the canonical reference participant
    And I start the requester participant connected to the provider participant with tip polling disabled
    Then the requester receives and applies its first F0 block while the provider holds the next response within 120 seconds
    When I start participant "NODE_3" as a provider connected to participant "NODE_1"
    And I start participant "NODE_4" as a requester connected to provider participant "NODE_3" with tip polling enabled
    Then the selected F0 request overlaps with an unselected F0 request that completes normally within 120 seconds
    And the running nodes use the same effective deployment
    And all era-transition participants report proposal publishing disabled
    # F1 activation starts grace; the established selected stream expires later.
    When all nodes enter the target era within 260 seconds
    Then all nodes report the target-era fork digest within 30 seconds
    And transition expiry cancels the held F0 request and the requester detects the interruption within 30 seconds
    # Require autonomous replacement without disturbing the completed control interaction.
    And the requester automatically replaces the interrupted request under F1 and the provider accepts it within 90 seconds
    And the requester retains the first downloaded block and completes IBD within 180 seconds
    When I enable proposal publishing on participant "NODE_1"
    Then participant "NODE_1" reports proposal publishing enabled
    When I enable proposal publishing on all era-transition participants
    Then all era-transition participants report proposal publishing enabled
    # All participants must converge, retain the finalized anchor, and stay healthy.
    And all declared era-transition participants have at least 12 blocks and canonical histories converge to within 1 blocks within 180 seconds
    And all nodes preserve the captured finalized block on their LIB ancestry after the era boundary within 120 seconds
    And all era-transition participants remain healthy after IBD recovery

  # WP2B: NODE_2 first completes IBD, then receives a genuine descendant whose
  # ancestry is missing. NODE_1's response to the resulting normal orphan
  # downloader request is held after five blocks through F0 rotation and grace,
  # then cancelled at expiry. The orphan downloader must continue under F1 and
  # resolve the original orphan without a test-side re-enqueue, republish,
  # restart, tip-poll recovery, or alternate download. Evidence distinguishes
  # the orphan path from IBD and checks retained progress and chain continuity.
  @era_boundary_recovery @era_boundary_orphan_download
  Scenario: A runtime orphan download recovers after its F0 request expires
    # Bind the hold to this interaction while excluding initial IBD and unsolicited proposals.
    Given era-boundary orphan-download recovery evidence is active
    And the era-transition participants are:
      | node   | role      | canonical_reference |
      | NODE_1 | provider  | yes                 |
      | NODE_2 | requester | no                  |
    And I configure initial proposal publication disabled for all era-transition participants
    And the provider participant holds its response stream after 5 blocks
    And the cluster uses parameter set "cucumber_fixture"
    And no nodes are declared as blend providers
    And the cluster uses era schedule from era 0 to era 1 starting at epoch 1
    And I have a cluster with capacity of 1 nodes
    And the provider response hold excludes the requester's genesis-tip IBD request
    And we use IBD peers
    And the cluster uses prolonged bootstrap period of 0 seconds
    And I have user config override "network.backend.swarm.kademlia.kbucket_inserts" as "manual"
    And I have user config override "network.backend.swarm.chain_sync.peer_response_timeout" as "seconds(300)"
    And I have user config override "network.backend.swarm.chain_sync.inbound_download_stream_timeout" as "seconds(300)"
    And I start the provider participant with its configured era-transition response hold
    Then the running nodes use the same effective deployment
    # Complete IBD first, then create missing ancestry for the normal runtime downloader.
    And the provider participant produces at least 18 blocks before era activation within 120 seconds
    When I capture a non-genesis finalized block before the era boundary on the canonical reference participant
    And I start the requester participant connected to the provider participant with tip polling disabled
    Then the requester completes initial IBD from the provider participant within 120 seconds
    When I record node "NODE_2" height as "orphan_requester_height"
    Then node "NODE_1" reaches 6 blocks beyond recorded height "orphan_requester_height" in 120 seconds
    When the provider publishes one descendant that starts a held F0 orphan request within 90 seconds
    Then participant "NODE_1" reports proposal publishing disabled
    And all era-transition participants report proposal publishing disabled
    # Activation preserves the established F0 stream through grace until expiry.
    When all nodes enter the target era within 260 seconds
    Then all nodes report the target-era fork digest within 30 seconds
    And expiry cancels the held F0 orphan request and schedules autonomous recovery within 60 seconds
    # The orphan downloader must continue without a test-side recovery trigger.
    And the requester completes its exact F1 orphan replacement within 90 seconds
    And the requester retains partial orphan progress and resolves the original ancestry within 90 seconds
    When I enable proposal publishing on participant "NODE_1"
    Then participant "NODE_1" reports proposal publishing enabled
    # Separately verify canonical convergence, finalized ancestry, and node health.
    And all declared era-transition participants have at least 12 blocks and canonical histories converge to within 1 blocks within 180 seconds
    And all nodes preserve the captured finalized block on their LIB ancestry after the era boundary within 120 seconds
    And all era-transition participants remain healthy after orphan recovery
    When I enable proposal publishing on all era-transition participants
    Then all era-transition participants report proposal publishing enabled
