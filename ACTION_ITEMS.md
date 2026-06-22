# Action Items — `yordis/subject-seq`

Status legend:
- `[x]` resolved in this branch
- `[~]` resolved by audit (no code change needed; rationale captured)
- `[ ]` still open

Scope: what is still required on top of the existing branch to (a) prove subject namespace sequencing is correct under the supported configurations and (b) make the feature ready for use on the maintainer's own fork. Items already done on the previous iteration are not relisted; see `adr/ADR-subject-namespace-sequencing.md` and `.trogonai/todos/subject-sequence-pr-action-plan.internal.trogonai.md` for the prior done set.

## 0. Position and Accepted Constraints

This feature exists in a fork. Three constraints are accepted explicitly so the rest of the document doesn't relitigate them:

- **Upstream merge is not a requirement.** Rip declined the design on the write-side latency / scope argument; Derek suggested one-stream-per-aggregate-id with timestamp interleave. The fork keeps its own design and is free to diverge.
- **Client SDK adoption is not a requirement.** The HTTP-style header surface (`Nats-Expected-Last-Subject-Version`, `subject_version` in PubAck JSON) is set/read directly from application code on this fork until or unless an SDK chooses to wrap it.
- **Public naming is committed.** `subject_versioning` / `Nats-Subject-Version[-Key]` / `Nats-Expected-Last-Subject-Version`. Rationale in §1.

### Why one-stream-per-aggregate-id was rejected

Recorded so the next person reading this doesn't have to re-derive it:

1. **Cardinality.** Event sourcing's gapless line is per *aggregate id* (`account-123`, `order-456`), not per subject prefix. Realistic workloads land at millions of streams, each carrying its own raft state, file descriptors, in-memory subject tree, and consumer registry. Operationally non-viable.
2. **Timestamp interleave is not an ordering.** Cross-aggregate projections (fraud detection across orders, materialized views, sagas) need a deterministic total order to be replayable. Wall-clock timestamps tie under skew, and no tiebreaker survives leader changes — two replayers reading the same data diverge.
3. **Atomic cross-aggregate writes.** Commands that touch multiple aggregates atomically (account-to-account transfer is the canonical one) need single-stream atomic batch. One-stream-per-aggregate gives that up.

The in-stream feature on this branch is opt-in, costs one tree lookup + one counter bump per publish during apply (≈13µs/op file, ≈27µs/op memory at high cardinality), and is observable via `StreamInfo.subject_versioning.namespaces`.

## 1. Design Decisions

Each call below is the PR author's decision, defended in the PR description. Maintainers may push back during review; rollback cost is noted so any reversal can be scoped honestly.

- [x] **Stored server-owned metadata: SHIP.** Each committed message carries `Nats-Subject-Version` and `Nats-Subject-Version-Key` headers. Rationale: replay, `direct get`, and duplicate-publish acks all need the same committed metadata without recomputing policy. Payloads are never mutated; only server-managed headers are added; client-supplied versions are rejected. Rollback cost if reversed: the entire file-store/mem-store rebuild path and all replay-correctness tests get thrown out and the feature pivots to consumption-time decoration. That's a fresh design, not a tweak.
- [x] **Grouped + exact namespaces: SHIP BOTH.** `subject_versioning.subject_transform` is optional; when omitted, the stored subject is the namespace key. Rationale: exact-only would make the feature a thin wrapper over the existing per-subject sequence and miss the event-grouping use case. The grouped path is one extra config field plus one fallback rule (documented in the ADR). Rollback cost if reversed: delete the `SubjectTransform` field on `SubjectVersioningConfig`, the grouped tests, and the fallback rule — bounded, ~200 LOC.
- [x] **Public surface: KEEP `subject_versioning` / `Nats-Subject-Version[-Key]` / `Nats-Expected-Last-Subject-Version`.** Rationale: the name describes the semantic (an opaque version assigned per derived subject namespace). The most natural alternative — `subject_sequence` — collides with the long-standing `Nats-Expected-Last-Subject-Sequence` header that already means something different (the stream's per-subject sequence). Rollback cost if reversed: regenerate errors, rename headers in five `*.go` files and every test, update the ADR — mechanical but touches the protocol.
- [x] **High-cardinality guardrail: SHIP WITHOUT CAP, observability included.** Rationale: the cost is bounded by the number of unique subjects in the stream and the feature is opt-in. A fixed cap rejects legitimate workloads without production data behind it. Operators can monitor `StreamInfo.subject_versioning.namespaces` and adjust. Rollback cost if added later: pure addition (validate in `StoreMsg` against a new `StreamConfig.SubjectVersioning.MaxNamespaces` field), no existing test breaks.

## 2. Correctness Gaps in the Branch

Items here are concrete code or test changes the branch needs before correctness can be claimed.

### 2.1 Store-layer integrity

- [~] `updateSubjectVersionStateLocked` insert with `stringToBytes(key)`. **No bug**: `stree.SubjectTree` copies keys internally — `newLeaf` calls `copyBytes(suffix)` and `meta.setPrefix` uses `append([]byte(nil), pre...)`. The `Find`/`Insert` callers are safe to pass the alias.
- [x] `removeMsgFromBlock` / `removeMsgNoCB` full-rebuild fallback. Both store paths now log/comment the assumption that config validation should make them unreachable. `filestore.go` emits a warn log identifying the seq that triggered the rebuild; `memstore.go` has the matching comment. The assumption itself is pinned by `TestJetStreamSubjectVersioningRemovalPathsAreUnreachable`, which exercises `DeleteMsg` and `PurgeStream` on a versioned stream and asserts they are rejected before any svs mutation happens. Targeted decrement was rejected because the contract makes a localized update lower-value than a clearly-instrumented safety net.
- [x] `decodeSubjectVersionState` rebuild reason. The recovery path now tags the trigger (`checkpoint missing` / `corrupt` / `older than first sequence` / `ahead of stream` / `trails stream state`) and includes it in the rebuild log line. Corrupt checkpoints already log explicitly.
- [x] Lock-order audit. Callers of `stream.subjectVersionState` (`processJetStreamMsgWithBatch`, `checkMsgHeadersPreClusteredProposal`) do not hold `store.mu`. Added a doc comment near `subjectVersionState` pinning the lock order: callers may hold `mset.mu` and/or `mset.clMu`, but must not hold the underlying store mutex.
- [x] Stale-checkpoint recovery tests. Added `TestFileStoreSubjectVersionStateRebuildsAfterCheckpointDeleted` (asserts svs is rebuilt from message scan with correct counters after `sver.db` is removed) and `TestFileStoreSubjectVersionStateRebuildsAfterCheckpointAheadOfStream` (asserts a hand-crafted checkpoint past `LastSeq+1` triggers the rebuild path and finishes with the correct entry).

### 2.2 Replication / cluster integrity

- [x] Snapshot rebuild semantics. Added a comment on `recoverSubjectVersionState` documenting that the stream snapshot does NOT include svs and that stored `Nats-Subject-Version[-Key]` headers are the source of truth for follower rebuild.
- [x] Concurrent publishers contiguity/uniqueness. `TestJetStreamClusterSubjectVersioningConcurrentPublish.checkContiguous` now explicitly fails on duplicate version assignment per namespace (in addition to the existing contiguity assertion). The sort-then-enumerate pattern still validates monotonicity and zero-based contiguity.
- [x] `inflightSubjectVersions` cleared on leader change. Confirmed in `processStreamLeaderChange`. No further change needed; existing `TestJetStreamClusterSubjectVersioningSurvivesLeaderChange` exercises step-down + immediate writes.
- [x] **Quorum-loss/partition recovery: covered by existing matrix.** Decision: the leader-change, cluster-restart, and snapshot-catchup tests on the branch are sufficient for v1. A bespoke quorum-loss harness would re-derive infrastructure that already exists in `jetstream_cluster_*_test.go`. If a partition-specific regression appears in review, it gets added then.
- [x] **Atomic batch rollback explicitly proven.** Originally claimed covered indirectly. Now backed by `TestJetStreamClusterAtomicBatchSubjectVersioningRollbackPreservesCounters` — 3-message batch across two namespaces, middle message fails its precondition, both namespace counters confirmed unchanged via follow-up publishes that require version 0.

### 2.3 Publish API surface

- [x] Reserved-header rejection on batch paths. Added `TestJetStreamFastBatchSubjectVersioningRejectsReservedHeaders` (fast batch) and `TestJetStreamClusterAtomicBatchSubjectVersioningRejectsReservedHeaders` (atomic batch). Both assert `NewJSStreamSubjectVersionHeaderServerManagedError` for `Nats-Subject-Version` and `Nats-Subject-Version-Key` supplied client-side.
- [x] Expected-version parser edge cases. Added `TestJetStreamSubjectVersioningExpectedLastSubjectVersionInvalidValues` covering `-1`, `1e1`, `v1`, `NaN`, `0x1`, `abc`, `3.14`, and a value that overflows uint64. Whitespace-only values are excluded because `sliceHeader` strips leading whitespace and the wire protocol's trailing-whitespace handling is not a feature contract.
- [x] Subject-transform non-match fallback. Added `TestJetStreamSubjectVersioningSubjectTransformFallback`: publish to a subject that matches the transform yields the grouped key; publish to one that doesn't match falls back to the stored subject and gets its own per-subject counter. ADR §"Namespace Derivation" updated with the explicit rule and the recommended subjects-shape narrowing for callers who want strict grouping.
- [x] **Empty-destination rejection: already covered by `ValidateMapping`.** Decision: no additional validator. `checkStreamCfg`'s `ValidateMapping` rejects malformed destinations today (proved by `TestJetStreamSubjectVersioningConfigValidation/bad-transform`). Adding a second "non-empty destination" check would be redundant.

### 2.4 PubAck contract

- [x] PubAck JSON shape. Added `TestJetStreamSubjectVersioningPubAckJSONShape`: zero-value `subject_version` is emitted as `0` (not omitted, not `null`); duplicate acks still carry `subject_version` and `subject_version_key`; non-versioned streams omit both fields.
- [x] **Duplicate ack after compaction: unreachable on versioned streams.** Decision: no test added. Subject versioning denies deletes/purges/TTLs/max-msgs/max-bytes/max-age, so the message backing a dedupe entry cannot be compacted away while versioning is enabled. The `loadStoredSubjectVersionMetadata` fallback (omit version on `ok=false`) remains as defense-in-depth in case versioning is ever loosened.

### 2.5 Config and update path

- [x] Update-path re-validation. Added `TestJetStreamSubjectVersioningUpdateRejectsDisallowedCombos` covering `MaxAge` (with `Duplicates` adjusted to bypass an earlier validator), `MaxMsgs`, `MaxBytes`, `MaxMsgsPer`, `AllowMsgTTL`, `Mirror`, `Sources`, and `RePublish`. `AllowRollup` is omitted because every reachable combination fires an earlier `roll-ups require the purge permission` / `requires deny purge` validator first — the subject-versioning rollup check is effectively redundant in `checkStreamCfg` ordering. Leave the redundant check in place as defense-in-depth.
- [x] **Sealed / replicas-scale interactions: out of scope.** Decision: `Sealed` blocks updates of any kind via an earlier validator (`checkStreamCfg` rejects updates to a sealed stream before reaching the subject-versioning block). Replicas changes don't reshape svs because each replica rebuilds it from stored headers. No dedicated test needed.

## 3. Operational & Observability

- [x] `StreamInfo.subject_versioning.namespaces`. New `SubjectVersioningInfo` struct attached to `StreamInfo`. Populated at every `StreamInfo` construction site in `jetstream_api.go` (create, update, info, list, restore). When versioning is disabled the field is `omitempty` so non-versioned streams stay unchanged on the wire. Covered by `TestJetStreamSubjectVersioningStreamInfoSurfacesNamespaceCount`.
- [~] Checkpoint-failure advisories. `writeSubjectVersionState`/`writeFullState` errors already bubble up through `_writeFullState`, which uses the same pathway as `writeTTLState` and `writeMsgSchedulingState`. No new advisory channel needed because the existing failure path matches the conventions of the surrounding feature subsystems.
- [x] **High-cardinality guardrail: shipped without cap (see §1).** `StreamInfo.subject_versioning.namespaces` gives operators the signal; a cap can be added later without breaking existing streams.

## 4. Documentation

- [x] ADR updates. `adr/ADR-subject-namespace-sequencing.md` now documents the non-matching `subject_transform` fallback, the snapshot/header rebuild semantics, and the new `StreamInfo.subject_versioning.namespaces` surface. Validation list cross-checked against `checkStreamCfg` — every entry in the ADR has a matching switch case in the validator.
- [x] **Reference + how-to landed in the ADR.** `adr/ADR-subject-namespace-sequencing.md` now carries a "Usage Patterns" section covering exact-subject sequencing, grouped namespace sequencing, optimistic concurrency, duplicate-publish acks, the consumer-side recovery flow that pairs with ADR-60, observability, and an error code reference. The `nats-server` repo does not ship a Diátaxis docs tree (operator docs live in `nats-docs`); a downstream PR there can lift this content once this branch lands.

## 5. PR Hygiene

- [x] **Test-infrastructure changes stay in this PR.** `reserveJetStreamClusterPortBlock` in `jetstream_helpers_test.go` plus the timing/port fixes in `jetstream_cluster_3_test.go`, `jetstream_cluster_4_test.go`, `jetstream_consumer_test.go`, `leafnode_test.go`, `norace_2_test.go`, `websocket_test.go` ship together with the subject-versioning work. The flake fixes were discovered while iterating on the subject-versioning cluster tests and are kept in-PR so reviewers see the full context. If reviewers ask for a split during code review, that's a separate operation post-feedback.
- [x] Generated error IDs. The new IDs 10224–10228 are still contiguous and do not collide with any existing entry in `errors.json`. Confirmed via `grep -E '"error_code":\s*(102[0-9][0-9])' server/errors.json | sort -u`.
- [x] `go vet ./server/` runs clean against the touched files. Pre-existing diagnostic noise from other files is not introduced by this change.
- [x] `gofmt -l server/` is clean for every file touched by this branch (the only stale-format file is `disk_avail_solaris.go`, unrelated).
- [x] **Full `./server` test run on CI.** Confirmed green on `yordis/nats-server#2`: 45/45 checks SUCCESS after the build-tag fix and the `subjectVersioningInfo` cfgMu race fix. Local machine still has `otelcol-c` on `127.0.0.1:8888` blocking the OCSP gateway test from running locally; CI is the authority.

## 6. Verification Matrix (re-run after the items above)

All four commands below were executed on this branch and passed.

```sh
mise exec -- go test ./server -run 'TestJetStreamSubjectVersioning(ExactSubjectNamespace|GroupedNamespace|ExpectedLastSubjectVersion|ExpectedLastSubjectVersionInvalidValues|DuplicatePubAckReturnsOriginalMetadata|RejectsUnsupportedHeaders|RemovalPathsAreUnreachable|ConfigValidation|UpdateRequiresEmptyStream|UpdateRejectsDisallowedCombos|StreamInfoSurfacesNamespaceCount|SubjectTransformFallback|PubAckJSONShape)$|Test(MemStore|FileStore)SubjectVersionState|TestJetStreamClusterSubjectVersioning|TestJetStreamClusterAtomicBatchSubjectVersioning|TestJetStreamFastBatchSubjectVersioning' -count=1
# ok   github.com/nats-io/nats-server/v2/server   13.092s
```

```sh
# Race detector across the full subject-versioning suite + the existing concurrent-config test.
mise exec -- go test ./server -run 'SubjectVersioning|TestJetStreamStreamConfigConcurrentReadWrite' -race -count=1
# ok   github.com/nats-io/nats-server/v2/server   19.350s
```

```sh
mise exec -- go test ./server -run 'TestJetStreamClusterExpectedPerSubjectConsistency|TestJetStreamAtomicBatchPublishExpectedPerSubject|TestJetStreamFastBatchPublishDuplicates|TestJetStreamFastBatchPublishDuplicatesCluster|TestJetStreamFastBatchSequentialDuplicateAndErrorPubAck|TestJetStreamClusterSubjectTransformWithExpectedSubjectSequenceHeader|TestJetStreamInterestStreamWithDuplicateMessages|TestJetStreamUpdateStream|TestStoreSubjectStateConsistency' -count=1
# ok   github.com/nats-io/nats-server/v2/server   10.609s
```

```sh
mise exec -- go test ./server -run '^$' -bench 'BenchmarkSubjectVersioning|BenchmarkFileStoreSubjectVersionStateCheckpoint' -benchtime=10x -count=1
# Cold-cache:
# BenchmarkSubjectVersioningHighCardinalityStore/Memory-14   10   26675 ns/op   1403 B/op   18 allocs/op
# BenchmarkSubjectVersioningHighCardinalityStore/File-14     10   13538 ns/op  27577 B/op   19 allocs/op
# BenchmarkFileStoreSubjectVersionStateCheckpoint-14         10  363250 ns/op 907932 B/op   32 allocs/op
#
# Steady-state (200k publishes / 100k namespaces):
# BenchmarkSubjectVersioningSustainedPublish/Memory-14    200000    961.4 ns/op    919 B/op   15 allocs/op    100000 namespaces
# BenchmarkSubjectVersioningSustainedPublish/File-14      200000   2874   ns/op    776 B/op   15 allocs/op    100000 namespaces
```

```sh
mise exec -- go test ./server -run 'SubjectVersioning|TestJetStreamAtomicBatchPublishExpectedPerSubject|TestJetStreamFastBatchPublishDuplicates|TestJetStreamFastBatchPublishDuplicatesCluster|TestJetStreamFastBatchSequentialDuplicateAndErrorPubAck|TestJetStreamClusterSubjectTransformWithExpectedSubjectSequenceHeader|TestJetStreamInterestStreamWithDuplicateMessages|TestJetStreamUpdateStream|TestStoreSubjectStateConsistency' -count=1
# ok   github.com/nats-io/nats-server/v2/server   22.326s
```

## 7. Production Hardening Backlog

Items below are things we can do on the fork now that scope is locked. Not required for the feature to function; required for confident production use.

### Closed in the current session

- [x] **Explicit atomic-batch rollback test.** `TestJetStreamClusterAtomicBatchSubjectVersioningRollbackPreservesCounters` (in `server/subject_versioning_cluster_test.go`) publishes a 3-message atomic batch across two namespaces with message 2 referencing a wrong precondition; asserts both namespace counters remain unchanged after rollback by probing each with a follow-up publish that requires version 0.
- [x] **`removeMsgFromBlock` unreachability regression.** `TestJetStreamSubjectVersioningRemovalPathsAreUnreachable` (in `server/subject_versioning_publish_test.go`) attempts `DeleteMsg` and `PurgeStream` against a versioned stream, asserts each is rejected, and verifies `svs.Size()` and the next-version assignment are unchanged.
- [x] **`mset.svtr` race audit under `-race`.** `go test ./server -run SubjectVersioning -race` and `go test ./server -run TestJetStreamStreamConfigConcurrentReadWrite -race -count=3` both pass. The svtr access pattern matches the existing `mset.itr` reads in publish hot paths and exhibits no race against the test that exercises concurrent config update + read.
- [x] **High-cardinality soak benchmark.** `BenchmarkSubjectVersioningSustainedPublish` (in `server/subject_versioning_store_test.go`) runs 200k publishes across 100k namespaces and reports steady-state cost: **961 ns/op memory, 2874 ns/op file**. Captured in the ADR cost section.
- [~] **CHANGELOG / release-notes entry.** Resolved by audit: `nats-server` does not maintain a CHANGELOG file (release notes live on GitHub Releases). On this fork, the feature record is the ADR header (`Date: 2026-04-30`, `Status: Committed on fork`), the PR description, and git history. Adding a synthetic CHANGELOG would be unconventional for this project.
- [x] **PR description.** `yordis/nats-server#2` body is now the why-focused description (motivation, multi-stream rejection rationale, design properties, fork status). No longer the placeholder template.

### Open

- [x] **Failure-injection — checkpoint corruption.** `TestFileStoreSubjectVersionStateRebuildsFromCorruption` covers six corruption modes — truncated mid-entry, trailing garbage, wrong magic byte, wrong version byte, empty file, header-only — and asserts each triggers the rebuild-from-headers path with correct counters.
- [x] **Failure-injection — write failure / disk full.** `TestFileStoreSubjectVersionStateWriteFailureDoesNotCorruptInMemoryState` removes the `msgs/` directory under the store while the file store is alive, calls `writeSubjectVersionState`, asserts the write errors out, and confirms the in-memory `svs` still reflects the committed messages. Models disk-full / read-only mount scenarios at the checkpoint write path.
- [ ] **Failure-injection — OOM.** Not testable from a unit test (the kernel kills the process before assertions run). Documented as a gap; covered operationally by the ulimit/cgroup config of the host.
- [ ] **Failure-injection — follower lag spike without snapshot.** The existing `TestJetStreamClusterSubjectVersioningSurvivesLeaderChange` and `TestJetStreamClusterSubjectVersioningFollowerCatchupFromSnapshot` cover catch-up via leader change and via stream snapshot. A pure-log-replay catch-up after sustained lag is partially redundant with these but worth adding if a regression appears.
- [ ] **Failure-injection — partial filestore block during svs rebuild.** Hard to set up deterministically (requires forging a corrupted msgblock mid-stream). The linear-rebuild loop already does `continue` + warn on `fetchMsg` errors, so the in-memory svs would be partial in that scenario. Documented as a known gap.
- [ ] **Multi-version single-node upgrade** test: stream created on a versioned-feature build, opened by a pre-feature build of this fork, then reopened by the feature build. Validates that `sver.db` is treated as opaque junk by the older binary and that `Nats-Subject-Version[-Key]` headers stored on messages are preserved across the downgrade.
- [x] **Mixed-version cluster (closest deterministic simulation).** Two tests model the rolling-upgrade story without spinning two binaries in one Go process:
  - `TestJetStreamClusterSubjectVersioningHealthyClusterConvergesSVS` pins the invariant rolling upgrades depend on — in an R3 cluster, all replicas' `svs` are byte-identical across multiple namespaces. If this ever regresses, no upgrade scenario can be safe.
  - `TestJetStreamClusterSubjectVersioningRollingUpgradeRecovery` shuts a follower down, removes its `sver.db` checkpoint (modeling "upgrade from a binary that never wrote svs"), forces a snapshot install on the leader after more writes, restarts the follower, and asserts svs rebuilds from the replicated message headers, matches the leader, and continues correctly after the recovered node is promoted to leader.
  
  Note: a true two-binary mixed cluster (one node literally without the apply-time injection code) cannot be safely simulated inside the apply path — stripping `subjectVersioning` on one node mid-stream causes header divergence in stored bytes by design. The recovery path validated above is what makes rolling upgrade safe in practice: shut a node down before changing its binary, let it rejoin and rebuild svs from headers replicated by the upgraded peers.
- [ ] **Sustained ≥1M ops soak.** Current `BenchmarkSubjectVersioningSustainedPublish` runs to 200k. Re-run with `-benchtime=1000000x` on a dedicated machine and capture p50/p99 latency, namespace-count growth, and `sver.db` size growth at checkpoint intervals.

## 8. Things explicitly NOT being done

Recorded so they don't reappear as gaps:

- Opening a PR against `nats-io/nats-server`. Upstream merge is not a goal.
- Submitting changes to `nats-io/nats-docs`. The ADR Usage Patterns section serves as docs for the fork.
- Client SDK PRs (`nats.go`, `nats.js`, etc.). Application code on the fork uses the headers directly.
- Renaming to `subject_sequence` or any other public surface. Naming is committed.

```sh
# Full ./server suite — run in CI; see green run at https://github.com/yordis/nats-server/pull/2
mise exec -- go test ./server -count=1 -timeout=30m
```
