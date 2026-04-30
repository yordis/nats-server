
# Subject Sequence PR Action Plan

Goal: reshape `yordis/subject-seq` into a PR that directly answers maintainer feedback around scope, storage-time mutation, namespace semantics, replication, and ADR-60 consumer reset.

Default path: keep the current write-path stream feature for now, but make it generic, explicit, replicated-safe, and honest about what ADR-60 consumer reset already solves. Pivot to consumption-time decoration only if maintainers reject stored server-owned metadata headers.

## Working Order

- [ ] First, make the PR easy to review without changing behavior:
  - [x] remove or relocate personal research artifacts
  - [ ] tighten public naming and docs
  - [x] write the PR description around maintainer concerns
- [x] Second, add the missing proof tests:
  - [x] exact-subject namespace
  - [x] grouped namespace
  - [x] concurrent replicated publishes
  - [x] failover/catchup continuity
- [x] Third, add a small cost/scale measurement.
- [x] Fourth, run focused tests.
- [ ] Fifth, get a clean full `./server` package run.
- [ ] Last, request maintainer review on the smallest defensible scope.

## File Map

- Public API/config: `server/stream.go`
- PubAck and header constants: `server/stream.go`
- Generated API errors: `server/errors.json`, `server/jetstream_errors_generated.go`
- Non-clustered publish path: `server/stream.go`
- Clustered publish path: `server/jetstream_cluster.go`
- Fast and atomic batch handling: `server/jetstream_batching.go`, `server/stream.go`
- In-memory state: `server/memstore.go`
- File-backed state and checkpointing: `server/filestore.go`
- Publish/store tests: `server/subject_versioning_publish_test.go`, `server/subject_versioning_store_test.go`
- Cluster/failover tests: `server/subject_versioning_cluster_test.go`
- Batch tests: `server/subject_versioning_batch_test.go`
- Shared test helpers: `server/subject_versioning_test_helpers_test.go`

## Decision Gates

- [ ] Gate A: Do maintainers accept stored server-owned metadata headers?
  - [ ] If yes, keep the current storage-path design and make the justification crisp.
  - [ ] If no, stop write-path work and redesign around consumer/group delivery decoration.
- [ ] Gate B: Is grouped namespace sequencing required for v1?
  - [ ] If yes, keep `subject_versioning.subject_transform`.
  - [ ] If no, consider exact-subject-only v1 to reduce scope.
- [ ] Gate C: Is this a standalone stream feature or should it wait for consumer groups?
  - [ ] If standalone, keep this PR narrow and opt-in.
  - [ ] If consumer groups, convert this branch into evidence and start a design issue/ADR instead.

## Phase 1: Reframe the Proposal

- [x] Rename the feature framing away from "event sourcing subject versioning".
- [x] Pick a neutral public concept name such as "subject namespace sequence" or "stream-assigned subject sequence".
- [x] Treat event sourcing as one motivating use case, not the feature identity.
- [x] Rewrite the PR summary around a generic JetStream capability:
  - [x] opt-in behavior
  - [x] explicit namespace derivation
  - [x] committed-message ordering
  - [x] replicated-stream correctness
- [x] State clearly that this PR does not replace normal JetStream delivery, acks, dedupe, or ADR-60 consumer reset.
- [ ] Audit user-facing names for event-sourcing-specific language:
  - [ ] config field names
  - [ ] error names
  - [ ] test names
  - [ ] ADR/docs language
  - [ ] PR title/body

## Phase 2: Answer the Namespace Concern

- [x] Keep explicit namespace derivation via `subject_versioning.subject_transform`.
- [x] Add or tighten docs for the exact-subject mode:
  - [x] `orders.123` has its own sequence
  - [x] `orders.456` has its own sequence
- [x] Add or tighten docs for the grouped-namespace mode:
  - [x] `events.order.123.created` maps to `events.order.123`
  - [x] `events.order.123.cancelled` maps to `events.order.123`
  - [x] both share one sequence line
- [x] Add tests showing exact-subject and grouped-namespace behavior side by side.
- [x] Explicitly document that wildcard consumers do not define the namespace.
- [x] Add one table to the PR description:
  - [x] publish subject
  - [x] stored subject
  - [x] namespace key
  - [x] assigned namespace sequence

## Phase 3: Reduce the Storage-Mutation Objection

- [x] Decide whether canonical headers remain stored metadata or become consumption-time decoration.
- [x] If stored headers remain:
  - [x] Explain why payloads are never mutated.
  - [x] Explain why server-owned metadata headers are acceptable.
  - [x] Keep rejecting client-supplied `Nats-Subject-Version`.
  - [x] Keep rejecting client-supplied `Nats-Subject-Version-Key`.
  - [x] Explain why stored metadata is needed for replay, direct get, and duplicate PubAck correctness.
  - [x] Verify expected-version command headers are not stored as canonical event metadata.
  - [x] Verify sourced/mirrored/scheduled messages remain rejected or explicitly out of scope.
- Fallback plan if maintainers reject stored headers:
  - Pivot to consumption-time decoration.
  - Remove header injection from the store path.
  - Move version assignment into consumer/group delivery behavior.
  - Rework tests around decorated delivery metadata instead of stored message metadata.

## Phase 4: Separate Publish Sequencing From Consumer Recovery

- [x] Add PR text explaining that ADR-60 consumer reset handles walking a consumer backward by stream sequence.
- [x] Add PR text explaining that this PR only provides a stable sequence namespace for committed messages.
- [x] Document the intended consumer recovery flow:
  - [x] durable consumer
  - [x] ordered processing with `MaxAckPending=1` where needed
  - [x] client tracks last stream sequence seen
  - [x] client detects delivery gaps or redeliveries
  - [x] client uses consumer reset to recover from a known stream sequence
- [x] Remove any claim that this feature alone gives "every message is precious" behavior.

## Phase 5: Prove Replicated Stream Correctness

- [x] Add a concurrent publish test for multiple publishers writing to the same namespace on `Replicas: 3`.
- [x] Add a concurrent publish test for multiple namespaces on `Replicas: 3`.
- [x] Add or strengthen a leader stepdown/failover test while writes are in flight.
- [x] Add or strengthen a follower snapshot/catchup test where the recovered follower later becomes leader.
- [x] Verify rejected writes do not consume namespace sequence numbers.
- [x] Verify failed atomic batches do not consume namespace sequence numbers.
- [x] Verify duplicate `Nats-Msg-Id` responses return the original namespace sequence metadata.
- [x] Verify same-namespace racing publishers produce contiguous assigned sequences with no duplicates.
- [x] Verify different namespaces can progress independently under the same stream.

## Phase 6: Address Cost and Operational Limits

- [x] Add a short cost section to the PR description.
- [x] Compare the cost shape honestly to `NumPending`-style per-subject/per-filter tracking.
- [x] Call out memory growth for high-cardinality namespaces.
- [x] Call out filestore checkpoint growth for high-cardinality namespaces.
- [x] Add a small benchmark or measurement for many namespace keys.
- [x] Confirm checkpoint write cadence is not per-message.
- [x] Confirm restart recovery is checkpoint-plus-catchup, not full scan on every normal restart.
- [x] Include the cost trade-off as a reason for the feature being opt-in.
- [x] Decide whether v1 needs a guardrail such as max tracked namespaces.

## Phase 7: Clean Up PR Contents

- [x] Remove `YORDIS_RESEARCH.md` from the server PR.
- [x] Move long design rationale to the PR description, linked issue, or `nats-architecture-and-design`.
- [x] Decide whether `adr/ADR-gapless-per-subject-event-versioning.md` belongs in this repo or should move to the architecture repo.
- [x] If keeping an ADR draft in this repo temporarily, make it concise and maintainer-facing.
- [ ] Make naming consistent across code, tests, docs, and errors.
- [x] Check generated errors are stable and do not collide with upstream error IDs.
- [ ] Keep `TODO.md` out of the final PR unless the maintainers explicitly want it.
- [ ] Make sure all committed documentation uses the project voice, not first-person notes.

## Phase 8: Verification Commands

- [x] Run the targeted subject-sequence proof matrix:

```sh
mise exec -- go test ./server -run 'TestJetStreamSubjectVersioning(ExactSubjectNamespace|GroupedNamespace|ExpectedLastSubjectVersion|DuplicatePubAckReturnsOriginalMetadata|RejectsUnsupportedHeaders|ConfigValidation|UpdateRequiresEmptyStream)$|Test(MemStore|FileStore)SubjectVersionState|TestJetStreamClusterSubjectVersioning(ReplicatedPublish|ConcurrentPublish|SurvivesLeaderChange|SurvivesClusterRestart|FollowerCatchupFromSnapshot)$|TestJetStreamClusterAtomicBatchSubjectVersioningExpectedVersionFirstOnly$|TestJetStreamFastBatchSubjectVersioning' -count=1
```

- [x] Run the adjacent JetStream regression slice:

```sh
mise exec -- go test ./server -run 'TestJetStreamClusterExpectedPerSubjectConsistency|TestJetStreamAtomicBatchPublishExpectedPerSubject|TestJetStreamFastBatchPublishDuplicates|TestJetStreamFastBatchPublishDuplicatesCluster|TestJetStreamFastBatchSequentialDuplicateAndErrorPubAck|TestJetStreamClusterSubjectTransformWithExpectedSubjectSequenceHeader|TestJetStreamInterestStreamWithDuplicateMessages|TestJetStreamUpdateStream|TestStoreSubjectStateConsistency' -count=1
```

- [x] Run the high-cardinality cost benchmark:

```sh
mise exec -- go test ./server -run '^$' -bench 'BenchmarkSubjectVersioningHighCardinalityStore|BenchmarkFileStoreSubjectVersionStateCheckpoint' -benchtime=10x -count=1
```

- [x] Run the focused subject-sequence test set:

```sh
mise exec -- go test ./server -run 'SubjectVersioning|ExpectedPerSubjectConsistency' -count=1
```

- [x] Run the broader touched-area regression set:

```sh
mise exec -- go test ./server -run 'SubjectVersioning|TestJetStreamAtomicBatchPublishExpectedPerSubject|TestJetStreamFastBatchPublishDuplicates|TestJetStreamFastBatchPublishDuplicatesCluster|TestJetStreamFastBatchSequentialDuplicateAndErrorPubAck|TestJetStreamClusterSubjectTransformWithExpectedSubjectSequenceHeader|TestJetStreamInterestStreamWithDuplicateMessages|TestJetStreamUpdateStream|TestStoreSubjectStateConsistency' -count=1
```

- [ ] Get a clean full server package run before requesting final review:

```sh
mise exec -- go test ./server -count=1 -timeout=30m
```

- Attempted with `env GOCACHE=/Volumes/Otter/Cache/go-build-cache mise exec -- go test ./server -count=1 -timeout=30m`.
  - Failed in unrelated broad-suite coverage: `TestGatewayOCSPMissingPeerStapleIssue` returned an OCSP `404 Not Found`.
  - The package then hit the 30 minute timeout while `TestJetStreamClusterInterestPolicyEphemeral/InterestWithName` was still running.

- Note: plain `mise exec -- go test ./server -count=1` is not sufficient on this machine; it hit Go's default 10 minute package timeout in unrelated filestore/dirstore tests.

- [x] Run formatting/checks required by the repo.
- [x] Run `git diff --check`.
- [x] Capture the exact passing command output for the PR comment.

## Phase 9: PR Description Checklist

- [x] Explain why JetStream needs an opt-in committed-message namespace sequence.
- [x] Explain why namespace derivation is explicit rather than inferred from consumers.
- [x] Explain why this supports both per-subject and per-group sequencing.
- [x] Explain how ADR-60 consumer reset fits into recovery.
- [x] Explain replicated-stream behavior and failure semantics.
- [x] Explain cost and high-cardinality trade-offs.
- [x] Avoid a "Test plan" section.
- [x] Keep the PR description focused on why, not what/how.
- [x] Mention the branch started as a spike, but the PR scope is intentionally narrowed.
- [x] Link ADR-60 when explaining consumer recovery.
- [x] Ask for an explicit maintainer decision on Gate A if it remains uncertain.

## Open Design Questions To Resolve With Maintainers

- [ ] Is storing server-owned metadata headers acceptable for this feature?
- [ ] Should this be a stream write-path feature, a consumer/group delivery feature, or a generic transform/mutator?
- [ ] What naming would maintainers accept for the public API?
- [ ] Is grouped namespace sequencing required in v1, or should exact-subject sequencing ship first?
- [ ] What operational limits, if any, should protect high-cardinality namespace state?

# General

- [ ] Auth for queue groups?
- [ ] Blacklist or ERR escalation to close connection for auth/permissions
- [ ] Protocol updates, MAP, MPUB, etc
- [ ] Multiple listen endpoints
- [ ] Websocket / HTTP2 strategy
- [ ] T series reservations
- [ ] _SYS. server events?
- [ ] No downtime restart
- [ ] Signal based reload of configuration
- [ ] brew, apt-get, rpm, chocately (windows)
- [ ] IOVec pools and writev for high fanout?
- [ ] Modify cluster support for single message across routes between pub/sub and d-queue
- [ ] Memory limits/warnings?
- [ ] Limit number of subscriptions a client can have, total memory usage etc.
- [ ] Multi-tenant accounts with isolation of subject space
- [ ] Pedantic state
- [X] _SYS.> reserved for server events?
- [X] Listen configure key vs addr and port
- [X] Add ENV and variable support to dconf? ucl?
- [X] Buffer pools/sync pools?
- [X] Multiple Authorization / Access
- [X] Write dynamic socket buffer sizes
- [X] Read dynamic socket buffer sizes
- [X] Info updates contain other implicit route servers
- [X] Sublist better at high concurrency, cache uses writelock always currently
- [X] Switch to 1.4/1.5 and use maps vs hashmaps in sublist
- [X] NewSource on Rand to lower lock contention on QueueSubs, or redesign!
- [X] Default sort by cid on connz
- [X] Track last activity time per connection?
- [X] Add total connections to varz so we won't miss spikes, etc.
- [X] Add starttime and uptime to connz list.
- [X] Gossip Protocol for discovery for clustering
- [X] Add in HTTP requests to varz?
- [X] Add favico and help link for monitoring?
- [X] Better user/pass support using bcrypt etc.
- [X] SSL/TLS support
- [X] Add support for / to point to varz, connz, etc..
- [X] Support sort options for /connz via nats-top
- [X] Dropped message statistics (slow consumers)
- [X] Add current time to each monitoring endpoint
- [X] varz uptime do days and only integer secs
- [X] Place version in varz (same info sent to clients)
- [X] Place server ID/UUID in varz
- [X] nats-top equivalent, utils
- [X] Connz report routes (/routez)
- [X] Docker
- [X] Remove reliance on `ps`
- [X] Syslog support
- [X] Client support for language and version
- [X] Fix benchmarks on linux
- [X] Daemon mode? Won't fix
