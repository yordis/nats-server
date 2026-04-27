# Gapless Per-Subject Event Versioning

| Metadata | Value |
|----------|-------|
| Date     | 2026-04-24 |
| Author   | Yordis Prieto |
| Status   | Proposed |
| Tags     | jetstream, server, event-sourcing, adr-draft |

## Context and Problem Statement

JetStream today has a global stream sequence and subject-scoped optimistic concurrency using `Nats-Expected-Last-Subject-Sequence`, but it does not provide a portable, canonical, gapless version per logical event stream.

For event sourcing, this is a real semantic gap. If an application observes event version `5` and then event version `9` for the same logical stream, it must be able to conclude whether `6`, `7`, and `8` exist without making hidden assumptions about global stream ordering, batching, pipeline timing, or producer behavior.

The requirement here is stricter than ordered delivery:

- the version must be monotonic
- the version must be gapless
- the version must travel with the stored event
- the version must remain meaningful outside NATS

User-land version assignment is insufficient for this requirement. If a producer computes the next version before the server actually commits the write, batching, pipelining, retries, or other concurrent writes can make the precomputed version stale by the time the event is stored.

The question this ADR answers is:

- can `nats-server` provide a gapless, server-owned, per-subject event version, and if so, what semantics are required to make that credible?

## Context, References, and Prior Work

Existing server capabilities relevant to this ADR:

- `LoadLastMsg(subject, ...)` can retrieve the last committed message for a subject
- `JSDirectGetLastBySubject` already provides direct last-by-subject reads
- `Nats-Expected-Last-Subject-Sequence` provides subject-scoped optimistic concurrency using stream sequence
- input subject transforms already exist and run before store-level persistence decisions
- clustered publish already tracks in-flight per-subject state for consistency checks

Related architecture ADRs in `nats-architecture-and-design`:

- `ADR-30`: Subject Transform
- `ADR-31`: JetStream Direct Get
- `ADR-36`: Subject Mapping Transforms in Streams
- `ADR-44`: Versioning for JetStream Assets
- `ADR-50`: JetStream Batch Publishing
- `ADR-56`: JetStream Consistency Models

## Design

### Overview

This ADR proposes an opt-in JetStream stream mode that gives each stored event exactly one canonical, server-owned, gapless version under an explicit version namespace.

The core design rule is:

- assign the version at commit/apply time, not in producer code and not at initial proposal time

This ensures the assigned version reflects real committed order rather than predicted order.

### Goals

- Provide one canonical version per stored event
- Ensure versions are gapless for the defined version namespace
- Make the version namespace explicit and inspectable
- Support batching and pipelining without allowing stale precomputed versions
- Preserve semantics across replay and historical inspection

### Non-goals

- Generic server-side mutation of arbitrary event payload formats
- Multiple authoritative subject versions on the same event
- Implicit inference of version namespace from consumer wildcards
- Full support in the first version for mirrors, sources, republish, or scheduled messages

### Terminology and Invariants

Terms used by this ADR:

- stored subject: the concrete subject under which the message is actually persisted after any input subject transform
- version namespace: the identity whose revisions must be gapless
- subject version: the canonical gapless revision for one version namespace
- `no_stream`: the explicit empty-state used by optimistic concurrency when no committed event exists yet for the namespace

Invariants:

- each stored event has exactly one authoritative subject version
- subject versions are 0-based and increase by exactly 1 within a namespace
- only committed writes consume a subject version
- rejected writes do not consume a subject version
- duplicate writes identified by `Nats-Msg-Id` return the originally assigned subject version
- gaplessness is only credible if the stream remains append-only in practice

### Stream Mode

This should be an opt-in stream mode, not a generic JetStream behavior.

Final v1 configuration shape:

```json
{
  "subject_versioning": {
    "mode": "gapless",
    "subject_transform": {
      "src": "events.order.*.*",
      "dest": "events.order.$1"
    }
  }
}
```

The v1 contract is:

- stream config field name is `subject_versioning`
- the enablement literal is `mode: "gapless"`
- `subject_transform` under `subject_versioning` uses the existing `SubjectTransformConfig` grammar
- if `subject_transform` is omitted, the version namespace defaults to the final stored subject
- if `subject_transform` is present, the version namespace is derived from the final stored subject using that transform

Update semantics:

- the mode may be set when creating a stream
- the mode may be enabled on an existing stream only if the stream is empty
- once committed messages exist, the subject-versioning mode is immutable

### Version Namespace

The server must know exactly which logical stream the version belongs to.

The namespace must not be inferred from how clients consume data.

For v1, the namespace must support the actual event-sourcing case where many event-type subjects share one version line for the same aggregate.

Namespace resolution order should be:

1. accept the client publish subject
2. apply any configured input subject transform
3. persist under the resulting concrete stored subject
4. if `subject_versioning.subject_transform` is configured, derive the version namespace key from the stored subject using that transform
5. otherwise use the stored subject as the version namespace key

This means the feature must support both:

- exact stored-subject namespace
- explicit transform-derived namespace

The latter is not optional for the motivating requirement. If the desired behavior is:

- `created` -> version `0`
- `cancelled` -> version `1`
- `shipped` -> version `2`

for the same order, then the version namespace is not the full stored subject. It is the derived aggregate key.

Wildcard consumers are explicitly irrelevant to namespace selection.

Example:

- `events.order.123.created`
- `events.order.123.cancelled`

sharing one logical revision line such as `events.order.123`.

This must remain explicit in stream config and reuse the same version-key contract rather than inferring anything from consumers.

### Canonical Metadata

Each stored event should carry canonical server-owned metadata:

- `Nats-Subject-Version`
- `Nats-Subject-Version-Key`

`Nats-Subject-Version` is the gapless version.

`Nats-Subject-Version-Key` is the version namespace identity.

Version numbering should be 0-based:

- the first committed event gets version `0`
- the absence of any prior committed event is represented explicitly as `no_stream`, not by overloading version `0`

Final header names:

- `Nats-Subject-Version`
- `Nats-Subject-Version-Key`

In v1, `Nats-Subject-Version-Key` may differ from the final stored subject. Storing it is required because the namespace identity is no longer always recoverable from the stored subject alone.

Ownership rules:

- `Nats-Subject-Version` is server-owned
- `Nats-Subject-Version-Key` is server-owned
- publishes that attempt to set either header should be rejected rather than silently overwritten

Rejecting spoofed headers is preferable to overwriting them because it prevents hidden client bugs and makes the server-owned contract explicit.

### Write Preconditions

This feature should introduce a version-based optimistic concurrency header:

- `Nats-Expected-Last-Subject-Version`

Behavior:

- if omitted, the server blindly appends and assigns the next version
- if present with an integer revision, the server verifies the current committed version for the namespace
- if present as `no_stream`, the server verifies that no committed event exists yet for the namespace
- if it does not match, the write is rejected
- rejected writes do not consume a version

This is intentionally distinct from `Nats-Expected-Last-Subject-Sequence`, which is tied to stream sequence rather than canonical event version.

Contract details:

- this header is client-supplied publish command metadata, not canonical event metadata
- it should be validated during write processing and not preserved as stored event metadata
- the exact empty-state literal is the ASCII string `no_stream`
- any non-empty value other than `no_stream` or an unsigned integer revision is invalid
- a mismatch should return `JSStreamWrongLastSubjectVersionErrF`
- the error should identify the version namespace key and the expected/current revision values, at least in the error description if not as structured fields
- if the header is used on a stream without subject versioning enabled, the publish should be rejected
- `Nats-Expected-Last-Subject-Sequence` and `Nats-Expected-Last-Subject-Sequence-Subject` remain independent and may still be used on subject-versioned streams

Recommended error names:

- invalid header value: `JSStreamExpectedLastSubjectVersionInvalidErr`
- precondition mismatch: `JSStreamWrongLastSubjectVersionErrF`

### PubAck

`PubAck` should be extended to include:

- `subject_version`
- `subject_version_key`

This ADR recommends making both fields canonical in v1:

- `subject_version`
- `subject_version_key`

This gives publishers the exact version assigned at commit/apply time and the namespace it belongs to.

Existing `PubAck` fields such as `stream`, `seq`, and `duplicate` remain meaningful.

Old clients that ignore unknown JSON fields should continue to work.

### Illustrative Protocol Examples

These are illustrative examples of the intended contract, not final wire encodings.

Create-if-empty on a brand new subject:

- publish subject:
  - `orders.123`
- client headers:
  - `Nats-Expected-Last-Subject-Version: no_stream`
- committed stored headers:
  - `Nats-Subject-Version: 0`
  - `Nats-Subject-Version-Key: orders.123`
- `PubAck` fields:
  - `seq: <stream-sequence>`
  - `subject_version: 0`
  - `subject_version_key: orders.123`

Append with optimistic concurrency:

- current committed state:
  - `orders.123` is at subject version `4`
- client headers:
  - `Nats-Expected-Last-Subject-Version: 4`
- committed stored headers:
  - `Nats-Subject-Version: 5`
  - `Nats-Subject-Version-Key: orders.123`
- `PubAck` fields:
  - `subject_version: 5`
  - `subject_version_key: orders.123`

Precondition mismatch:

- current committed state:
  - `orders.123` is at subject version `5`
- client headers:
  - `Nats-Expected-Last-Subject-Version: 4`
- result:
  - write is rejected
  - no subject version is consumed
  - error describes the key plus expected/current revision mismatch

Idempotent retry with `Nats-Msg-Id`:

- first committed write on `orders.123` gets:
  - `seq: 22`
  - `subject_version: 5`
- retry with the same `Nats-Msg-Id` gets:
  - `duplicate: true`
  - `seq: 22`
  - `subject_version: 5`
  - `subject_version_key: orders.123`

### Duplicate Publish Semantics

`Nats-Msg-Id` remains the idempotency primitive.

When a publish is recognized as a duplicate of an already committed message:

- the server must not assign a new subject version
- the reply should set `duplicate=true`
- the reply should include the original `seq`
- the reply should include the original `subject_version`
- the reply should include the original `subject_version_key`

When a publish hits an in-process duplicate conflict rather than a committed duplicate:

- the existing conflict behavior should remain
- no subject version is consumed while the original write is unresolved

This matters because retries are one of the main places where gaplessness would otherwise become untrustworthy.

### Batch Semantics

Two cases need to be documented separately.

Atomic batch publish:

- messages are ordered by batch position
- validation sees one logical batch commit attempt
- if multiple messages in the batch target the same version namespace, revisions are assigned in batch order using a batch-local overlay of pending namespace revisions
- if the batch aborts, none of those revisions are consumed

Non-atomic or fast batch publish:

- each message is still validated and committed independently
- each committed message gets the next revision visible at its own commit/apply point
- failed writes consume no revision
- only the first message for a given namespace in a fast batch may carry `Nats-Expected-Last-Subject-Version`
- later fast-batch messages for the same namespace must omit that header so behavior does not depend on race timing between proposal and apply

The authoritative rule is still the same:

- only committed writes consume revisions
- assignment reflects actual commit/apply order, not client submission order

Worked examples:

Atomic batch publish with repeated namespace:

- current committed state:
  - `events.order.123` is at revision `4`
  - `events.order.999` is at revision `1`
- client sends one atomic batch in this order:
  1. `events.order.123.created`
  2. `events.order.999.created`
  3. `events.order.123.cancelled`
- assigned revisions:
  1. `events.order.123.created` -> `5` under key `events.order.123`
  2. `events.order.999.created` -> `2` under key `events.order.999`
  3. `events.order.123.cancelled` -> `6` under key `events.order.123`

The second `events.order.123.*` message does not re-read committed state and accidentally reuse `5`. The batch-local overlay carries the provisional advance for the derived key `events.order.123`.

If that atomic batch aborts, none of `5`, `2`, or `6` become visible.

Non-atomic or fast batch publish with reordered commits:

- current committed state:
  - `events.order.123` is at revision `4`
- client submits two messages for the same derived namespace in this order:
  1. message `A`
  2. message `B`
- because the writes are independent, `B` reaches commit/apply before `A`
- assigned revisions:
  - `B` -> `5` under key `events.order.123`
  - `A` -> `6` under key `events.order.123`

This is expected. In non-atomic mode, subject revisions follow actual commit/apply order, not submission order.

### Assignment Point

#### R1 / Non-clustered

On publish:

1. resolve the final stored subject after input subject transform
2. resolve the version namespace key
3. load the current committed version state for that namespace key, if any
4. determine the current state from committed namespace state; if no version exists, the state is `no_stream`
5. validate `Nats-Expected-Last-Subject-Version` if present
6. assign `next = 0` if the current state is `no_stream`, otherwise `current + 1`
7. inject canonical metadata, including `Nats-Subject-Version` and `Nats-Subject-Version-Key`
8. store the message
9. return `subject_version = next` and `subject_version_key = <namespace-key>`

#### Clustered

On leader receive:

1. accept the publish request
2. carry any expected-version requirement in the replicated command
3. do not assign the final version yet

On apply:

1. resolve the final stored subject after transform
2. resolve the version namespace key
3. load the current committed version state for that namespace from committed local state, if any
4. determine the current state from committed namespace state; if no version exists, the state is `no_stream`
5. validate `Nats-Expected-Last-Subject-Version` if present
6. assign `next = 0` if the current state is `no_stream`, otherwise `current + 1`
7. inject canonical metadata, including `Nats-Subject-Version` and `Nats-Subject-Version-Key`
8. store the message
9. return or propagate the assigned version and key

This places version assignment at the point where true committed order is known.

### Failure, Replay, and Failover Semantics

The gapless claim depends on documenting failure behavior precisely.

Required semantics:

- if a leader accepts a publish but crashes before the write is committed/applied, no subject version is consumed
- if replication is attempted but the write never commits, no subject version is consumed
- if a write is rejected during validation, no subject version is consumed
- if an atomic batch fails, no subject versions from that batch are consumed

Replay and recovery semantics:

- restart recovery should derive the current revision from committed stored state
- replicas must reach the same subject version because assignment happens in deterministic apply order
- this design avoids needing a per-subject analogue of stream-level failed-last-sequence bookkeeping

### Stream Restrictions

Observable gaplessness requires more than just assignment. If messages can later disappear, downstream observers can still see holes.

For this mode, the stream should behave as append-only in practice.

Initial restrictions should reject or disable:

- `MaxAge`
- `MaxMsgs`
- `MaxBytes`
- `MaxMsgsPer`
- delete
- purge
- rollup
- per-message TTL
- subject delete markers
- interest retention
- work-queue retention

For v1, this should be enforced concretely as:

- `Retention == LimitsPolicy`
- `MaxAge == 0`
- `MaxMsgs == -1`
- `MaxBytes == -1`
- `MaxMsgsPer == -1`
- `DenyDelete == true`
- `DenyPurge == true`
- `AllowRollup == false`
- `AllowMsgTTL == false`
- `SubjectDeleteMarkerTTL == 0`
- `AllowMsgCounter == false`

This is intentionally stricter than the long-term design space. V1 should not try to thread the needle of finite but non-evicting stream limits.

The server should still reject writes when account or storage resources are exhausted rather than evicting older data.

### First Version Scope

To keep the first implementation credible, it should likely reject:

- mirrors
- sources
- republish
- scheduled messages

These can be revisited later once the core semantics are stable.

The first version should support explicit derived namespaces because they are part of the motivating requirement:

- version namespace defaults to the final stored subject if no subject-version transform is configured
- version namespace may instead be derived from the final stored subject using `subject_versioning.subject_transform`
- `Nats-Subject-Version-Key` may therefore differ from the stored subject

### Migration and Compatibility

This mode should be treated as a creation-time or empty-stream-only choice in v1.

Recommended rules:

- allow enabling the mode when creating a new stream
- allow enabling it on an existing stream only if the stream is empty
- reject enabling it on non-empty streams
- reject disabling or changing the mode once the stream has committed messages

Why:

- existing historical messages do not carry canonical subject-version metadata
- historical gaplessness cannot be asserted retroactively without an explicit migration procedure
- backfilling versions offline is possible in theory, but it is not part of a credible v1 contract

Compatibility notes:

- stored messages remain normal JetStream messages with additional metadata
- consumers that ignore the new headers still function normally
- older clients can ignore the new `PubAck` fields

### Performance and Storage Costs

The first version should optimize for semantic correctness over maximal throughput.

Expected costs:

- additional header bytes per stored event
- more metadata surfaced in publish acknowledgements
- persisted namespace state or equivalent index metadata for derived keys

Why this is still acceptable for v1:

- the derived-namespace requirement is core to the target use case
- it is better to admit the need for explicit namespace state than to implement the wrong semantics quickly

Known pressure points:

- very high-cardinality namespaces may amplify state-management cost
- namespace state and recovery logic will be more invasive than exact-subject-only write-time lookup

### Store Shape Decision

The current recommended implementation shape is:

- `memStore`: separate in-memory namespace tree keyed by `Nats-Subject-Version-Key`
- `fileStore`: the same in-memory namespace tree plus a stream-level checkpoint side file
- checkpoint contents: checkpoint sequence plus namespace entries containing at least `last_version` and `last_seq`
- recovery model: load checkpoint, then catch up by scanning committed messages from the checkpoint sequence to the current last sequence

This explicitly means:

- namespace version state belongs in the store layer
- namespace version state should remain distinct from existing real-subject indexes such as `fss` and `psim`
- committed messages remain the source of truth if checkpoint state is stale or missing

### Current Implementation Concerns

The remaining concerns are implementation-shape concerns, not semantic uncertainty.

- namespace entry shape should stay minimal in v1; the current bias is to store only `last_version` and `last_seq`
- namespace state should likely remain store-internal until a clear `StreamStore` API boundary emerges
- `fileStore` checkpoint write cadence needs to balance steady-state overhead against restart catch-up scan cost
- duplicate publish responses should first optimize for correctness by loading the original stored message and reading its canonical headers; metadata caching can wait for evidence
- atomic batches need a plain batch-local namespace overlay keyed by version namespace rather than a more elaborate second indexing layer
- recovery of namespace state must complete before subject-versioned streams accept new writes
- very high-cardinality namespaces will increase both memory pressure and checkpoint size; this is an expected cost of the guarantee and should be measured rather than hand-waved away

### Frozen Implementation Defaults

The following defaults are now recommended as the concrete first implementation shape.

- namespace entry value stores only:
  - `last_version uint64`
  - `last_seq uint64`
- namespace helpers remain store-internal in v1 rather than expanding the public `StreamStore` interface
- `fileStore` uses a stream-level side-state file named `sver.db`
- `sver.db` should encode:
  - file magic
  - file version
  - checkpoint sequence
  - entry count
  - repeated `(namespace key, last_version, last_seq)` records using compact binary/uvarint-style encoding
- `sver.db` should be written on normal side-state/sync cadence, on clean shutdown, and after recovery rebuild; it should not be written per message
- committed duplicate publish handling should load the original stored message and read `Nats-Subject-Version` / `Nats-Subject-Version-Key` from its headers
- atomic batches should use a plain batch-local `map[string]uint64` keyed by version namespace
- only the first message for a given namespace in an atomic batch should carry `Nats-Expected-Last-Subject-Version`; later messages for that namespace advance from the batch-local provisional version
- namespace recovery is a write-resume gate: subject-versioned streams do not accept writes until namespace state is recovered and catch-up scanning is complete

### Implementation Guidance From The Current Codebase

The existing codebase already points to the likely implementation shape.

Store/index implications:

- `StreamStore` currently exposes subject-oriented lookup primitives such as `LoadLastMsg(subject, ...)`
- `memStore` tracks subject state in `fss`
- `fileStore` tracks subject state in `psim` and per-block `fss`
- those indexes are keyed by actual stored subjects, not arbitrary derived namespace keys

This strongly suggests that derived namespace versioning needs explicit store-managed namespace state rather than repeated scans or opportunistic re-derivation on the write path.

Recommended store shape:

- `memStore` should keep a separate in-memory namespace tree keyed by `Nats-Subject-Version-Key`
- each entry should minimally track:
  - `last_version`
  - `last_seq`
- this should remain distinct from `fss`, which models actual stored-subject state

Recommended `fileStore` shape for v1:

- keep the same namespace tree in memory
- persist it in a stream-level side-state file, similar in spirit to the existing TTL and scheduling checkpoint files
- encode:
  - a checkpoint sequence
  - the namespace-key -> latest-version/latest-seq map
- on recovery, load the checkpoint file and then catch up by scanning committed messages from the checkpoint sequence to the current last sequence

This gives `fileStore` a credible checkpoint-and-catch-up model without forcing a full new block-level namespace index format in the first version.

In-flight consistency implications:

- current clustered expected-per-subject consistency already uses in-memory key tracking such as `expectedPerSubjectInProcess` and `expectedPerSubjectSequence`
- that pattern should be reused, but keyed by version namespace instead of only by stored subject

Duplicate implications:

- duplicate detection already exists via `Nats-Msg-Id`
- duplicate publish responses already return the original stream sequence and `duplicate=true`
- subject-version support should extend that path so the original committed `subject_version` and `subject_version_key` are returned as well

Batch implications:

- atomic batch publish already stages consistency checks for the full batch and commits in batch order
- fast batch publish already validates and commits messages independently

This means the existing batch model is compatible with the proposal:

- atomic batches need a batch-local overlay keyed by version namespace
- fast batches should continue to follow actual commit/apply order

Recovery implications:

- `EncodedStreamState` currently captures global replicated stream state, not arbitrary namespace-revision state
- namespace revision state therefore does not come for free from current snapshot encoding

That leaves two credible implementation options:

- rebuild namespace revision state during recovery before writes resume
- extend store and snapshot persistence so namespace revision state is first-class persisted state

Either approach is more honest than pretending derived namespace state can be inferred cheaply at publish time.

### Observability and Introspection

Operators and clients need a way to inspect the feature without reading implementation code.

Recommended baseline:

- `PubAck` returns the assigned subject version and version key
- mismatch errors should identify the namespace key and conflicting revisions
- tracing or advisories should expose subject-version assignment and mismatch events

Because derived namespaces are now core scope, direct last-by-subject is no longer sufficient for all namespace introspection. A dedicated namespace lookup or stream-inspection API is still follow-up work, but the write-path semantics should not wait on it.

### Acceptance Criteria

The first implementation should not be considered complete unless all of the following are true:

- publishing the first event to an empty version namespace with `no_stream` stores subject version `0`
- publishing the next event to the same namespace stores subject version `1`
- optimistic concurrency mismatch rejects the write and consumes no subject version
- invalid `Nats-Expected-Last-Subject-Version` values are rejected
- `Nats-Expected-Last-Subject-Version` is rejected on streams without subject versioning enabled
- `Nats-Expected-Last-Subject-Sequence` and `Nats-Expected-Last-Subject-Sequence-Subject` remain independent on subject-versioned streams
- publishes that try to set `Nats-Subject-Version` or `Nats-Subject-Version-Key` are rejected
- duplicate `Nats-Msg-Id` publish returns the originally assigned subject version and key
- atomic batches assign contiguous per-namespace revisions in batch order or consume none if the batch fails
- non-atomic batches assign per-namespace revisions in actual commit/apply order
- fast batches deterministically reject `Nats-Expected-Last-Subject-Version` beyond the first message for the same namespace
- restart recovery derives the next subject version from committed stored state without introducing holes
- clustered failover before apply does not consume a subject version
- enabling the feature on a non-empty stream is rejected
- incompatible retention and deletion features are rejected when the mode is enabled

### Alternatives Considered and Rejected

This ADR explicitly rejects the following as authoritative semantics:

- user-land version assignment based on reading last state and predicting the next value
- treating global stream sequence as if it were event version
- treating consumer-local sequence as if it were event version
- silently overwriting client-supplied canonical version headers
- multiple authoritative subject versions on one event
- generic arbitrary payload rewriting by the server

### Payload Considerations

This ADR does not propose generic server-side rewriting of arbitrary application payloads.

The server can reliably assign canonical metadata in headers and acknowledgements.

This ADR makes a stronger choice for v1:

- canonical subject versioning is header-only by design
- payload shape is outside the scope of this feature

If a deployment needs the version copied into a payload, that is an application or bridge concern, not part of the canonical `nats-server` subject-version contract.

### Deferred Future Scope

The following items are intentionally deferred from v1 and do not block implementation of this ADR.

Future namespace lookup/admin API:

- derived namespaces are part of the core feature scope
- a dedicated namespace lookup or admin API is still deferred even though the write path must already support derived keys

## Decision

This feature is technically feasible if we constrain the problem correctly.

The decision proposed here is:

- implement gapless per-subject event versioning only as an opt-in JetStream stream mode
- make the version server-owned and assign it only at commit/apply time
- define exactly one canonical version namespace per stored event
- store canonical version metadata with the event
- make namespace explicit, not inferred
- support explicit transform-derived namespaces in the first version
- require append-only semantics for any stream using this mode

This ADR explicitly rejects:

- user-land authoritative version assignment
- multiple authoritative versions per event
- implicit namespace inference from consumer shape
- generic arbitrary payload rewriting by the server

## Consequences

### Positive

- Event version becomes a server-owned semantic rather than a producer convention
- Batching and pipelining no longer invalidate correctness
- Replay and debugging become clearer because version identity is explicit
- Downstream systems can trust the canonical version without consulting global stream sequence

### Negative

- This is not a small feature; it affects write semantics, acknowledgements, and stream configuration
- The mode is incompatible with several current retention/deletion features
- Additional metadata must be carried and stored with each event
- The first version likely has to reject several JetStream features rather than integrating with all of them

### Follow-up Work

- add the dedicated subject-version mismatch API error and tests
- revisit whether a dedicated admin lookup is needed once derived namespaces exist
- determine whether the longer-term home of this ADR should be `nats-architecture-and-design`
