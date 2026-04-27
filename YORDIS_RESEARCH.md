# YORDIS_RESEARCH

Updated: 2026-04-24

## Purpose

This is the working note for understanding what you want right now.

I will keep writing into this file as we talk so the discussion stays grounded in one place.

Companion ADR draft:

- `adr/ADR-gapless-per-subject-event-versioning.md`

## Current Repo Context

- Repository: `github.com/nats-io/nats-server`
- Working directory: `/Users/yordisstudio/Developer/github.com/nats-io/nats-server`
- Current branch: `yordis/subject-seq`
- Current branch state when this file was created: aligned with `main` and `upstream/main`, with no local diff
- Go module: `github.com/nats-io/nats-server/v2`
- Go version in `go.mod`: `1.25.0`
- Toolchain in `go.mod`: `go1.25.9`
- Server version constant: `2.14.0-RC.1`

## What I Think You Want Right Now

- A live research document, not a final polished report yet
- A place where I can write down findings, hypotheses, references, and decisions as we discuss them
- A shared scratchpad that can later turn into a concrete implementation or investigation plan

## Current Inference

This is now the confirmed requirement direction:

- The actual topic is not generic "subject sequence"
- The real question is whether NATS could ever provide a monotonic increasing gapless number per subject
- The concrete use case is event sourcing
- The deeper need is not only ordering inside NATS, but a version number that can travel with the event outside of NATS

## Confirmed Problem Statement

Yordis needs to answer a question like this without making major assumptions:

- If an event stream is modeled by subject, can each subject have its own monotonic increasing gapless version number?
- If a consumer receives `Event(number: 5)` and then `Event(number: 9)` for the same subject, should it assume `6`, `7`, and `8` must also exist?

The key issue is that the existing stream sequence is global, not per subject.

## Hard Requirement

- Gapless is not optional
- This is not "nice to have"
- Any solution that only gives monotonic but allows holes does not satisfy the requirement
- Any solution that requires downstream consumers to infer whether missing numbers should exist does not satisfy the requirement

## What The Slack Thread Clarified

### Current Maintainer Position

- There are no current plans for a built-in per-subject monotonic gapless sequence
- For a specific subject or wildcard consumer, ordered delivery will not miss messages just because the global stream sequence has gaps for that subject
- For normal event sourcing usage, gaps caused by deletions should be unusual

### Important Distinction

- Global stream sequence and per-subject version are not the same thing
- A gap in the global stream sequence for one subject does not imply messages were missed for that subject
- So `5` then `9` in the global stream sequence does not mean subject-local versions `6`, `7`, `8` should exist

### Existing Partial Approximation

- A non-redelivering consumer pinned to one concrete subject effectively has a consumer-local increasing number
- That is not the same as a subject-native version on the stored event
- It is also not considered an efficient or general solution, especially for wildcard or many-subject use cases

### Technical Cost Mentioned

Even if the feature sounds like "just one more integer", the conversation identifies extra system behavior behind it:

- Subject-level state management
- Semantics around an expected-subject-version style header
- New expectations around publish-time concurrency control and validation

### What Does Not Fully Solve It

- Ordered consumption across many subjects with only one ack outstanding can preserve ordering
- That reduces some consumer-side concerns
- It does not solve publish-side gap detection
- It also does not solve the need for a version field that leaves NATS and travels everywhere with the event

### Current Conclusion From The Thread

- Inside NATS, the system may already be "good enough" for ordered consumption
- Outside NATS, there is currently no built-in primitive that gives Yordis a portable subject-level event version
- The direct answer from the thread is effectively: today, NATS does not provide this

## Requirement As I Understand It Now

This appears to be the actual requirement, stated more precisely:

- Each event stream identified by subject should ideally expose a per-subject version
- That version should be monotonic increasing
- It must be gapless
- It should be carried in the event itself, not inferred from consumer behavior
- It should remain meaningful outside of NATS
- Yordis wants to avoid telling downstream systems to make assumptions from global sequence numbers

## Questions Hidden Inside The Requirement

These should stay visible because they affect design later:

- Does "gapless" mean gapless among committed writes only, or gapless across all attempted writes?
- Is the version expected to be assigned at publish acceptance time, at stream commit time, or at consumer delivery time?
- Must wildcard consumers observe a coherent per-subject version space across many subjects?
- Is the requirement primarily about correctness semantics, interoperability, observability, or all three?
- Is a user-land solution acceptable, or is the concern specifically that the server should own the guarantee?

## Relevant Starting Points

- Entry point: `main.go`
- Server lifecycle: `server/server.go`
- Config parsing: `server/opts.go`
- Reload path: `server/reload.go`
- Core account model: `server/accounts.go`
- JetStream root: `server/jetstream.go`
- Stream logic: `server/stream.go`
- Consumer logic: `server/consumer.go`
- Raft logic: `server/raft.go`
- Subject routing: `server/sublist.go`

## Subject-Sequence Hotspot To Revisit If That Is The Topic

- `server/stream.go`: expected last sequence per subject header parsing and standalone validation
- `server/jetstream_batching.go`: clustered/batched expected-per-subject handling
- `server/jetstream_cluster.go`: cleanup/reset of in-flight expected-per-subject state
- `server/jetstream_test.go`: standalone subject-sequence tests
- `server/jetstream_cluster_3_test.go`: clustered subject-transform coverage
- `server/jetstream_cluster_4_test.go`: consistency checks for expected-per-subject state

## Research: What Already Exists In The Server

### Existing Read Primitive

- The store interface already supports `LoadLastMsg(subject, ...)`
- There is already a direct API path for last-by-subject lookup via `JSDirectGetLastBySubject`
- This means the server can already answer "what is the latest message for this subject?"

### Existing Subject-Level Optimistic Concurrency Primitive

- `JSExpectedLastSubjSeq` already lets a client say "only accept this publish if the last message for this subject is still stream sequence X"
- `JSExpectedLastSubjSeqSubj` can scope that check to a supplied subject filter
- In standalone mode, validation happens in `processJetStreamMsg`
- In clustered mode, validation happens pre-proposal in `checkMsgHeadersPreClusteredProposal`

### Existing Clustered Coordination

- Clustered publish already tracks in-flight per-subject state with:
- `expectedPerSubjectSequence`
- `expectedPerSubjectInProcess`
- `inflight`
- This is already used to stop conflicting same-subject publishes while a clustered proposal is unresolved

### Existing Tests That Matter

- `TestJetStreamLastSequenceBySubject`
- `TestJetStreamLastSequenceBySubjectWithSubject`
- `TestJetStreamLastSequenceBySubjectConcurrent`
- `TestJetStreamClusterExpectedPerSubjectConsistency`
- `TestJetStreamClusterSubjectTransformWithExpectedSubjectSequenceHeader`

## Research: Can This Be Accomplished Today Without Server Changes?

Yes, but only as a cooperative application protocol.

### What That Protocol Would Look Like

- Read the latest message for the concrete subject
- Extract the current event version from the event body or headers
- Extract the current stream sequence for that same latest subject message
- Compute `next_version = current_version + 1`
- Publish the new event carrying that `next_version`
- Include `JSExpectedLastSubjSeq` with the last known stream sequence for that subject
- Include `Nats-Msg-Id` for idempotent retry behavior
- If publish fails with wrong-last-sequence, re-read and retry

### Why This Can Be Gapless

- No version is consumed unless a publish is actually accepted
- Concurrent writers to the same subject are forced to race through the expected-last-subject-sequence check
- A failed publish does not create a visible version hole because the event was never committed
- A client retry can be made idempotent with `Nats-Msg-Id`

### Why This Is Still Only A Partial Solution

- The server is not validating the continuity of the embedded event version itself
- A non-cooperative publisher can write a bad version into the payload
- Readers have to trust all producers to follow the protocol
- This is not a server-owned semantic

### Why This Is Not Good Enough For Yordis

The batching objection is decisive:

- in user-land, version assignment happens before the server has actually committed the write
- if publishes are buffered, pipelined, retried, or batched, the predicted "next version" can be stale by the time the write lands
- that means the payload can already carry the wrong version before the real commit order is known
- so user-land only works if writers serialize aggressively per subject and avoid hidden batching assumptions

That is exactly the kind of assumption this requirement is trying to eliminate.

Practical conclusion:

- user-land is not a trustworthy foundation for a hard gapless guarantee when batching or pipelining is part of the system
- if gaplessness must remain true at actual commit order, the version must be assigned at the commit/apply boundary, not earlier in producer code

## Research: The Hard Constraint Most Likely To Force The Design

If the version must exist inside the domain event payload before publish, the server cannot solve that generically by itself.

Why:

- The server can add or validate headers
- The server can include more information in `PubAck`
- The server cannot safely rewrite arbitrary JSON, Protobuf, Avro, or opaque payloads into every event schema

This creates two fundamentally different problem shapes:

- If a canonical header is acceptable, a server-native feature is feasible
- If the domain payload itself must already contain the final version at publish time, some client-side protocol remains unavoidable

## Research: Observable Gaplessness Requires More Than Assignment

Even if the server assigns a perfect gapless version at append time, the stream can still later expose holes if messages disappear.

So a real "gapless subject version" mode likely has to restrict or disable:

- `MaxAge`
- `MaxMsgs`
- `MaxBytes`
- `MaxMsgsPer`
- explicit delete
- explicit purge
- rollups
- per-message TTL
- subject delete markers

The current stream config already contains the right kinds of controls:

- `DenyDelete`
- `DenyPurge`
- `AllowRollup`
- `AllowMsgTTL`
- `Sealed`

But `Sealed` is too strong because it also stops new writes.

## Research: Viable Server-Native Designs

### Option A: Cooperative Client Protocol On Top Of Existing Server Features

This is the lowest-effort path.

- Keep version assignment in the producer or shared client library
- Use last-by-subject reads plus `JSExpectedLastSubjSeq`
- Carry the version in payload and optionally in headers
- Enforce the protocol socially or via SDKs

Benefits:

- Could work today
- No server changes required
- Gives payload-level version immediately

Costs:

- Not server-enforced
- Easy for one bad publisher to violate
- Not a NATS-native guarantee

### Option B: Server-Assigned Header Version On An Opt-In Stream Mode

This is the cleanest server-native direction if header-level version is acceptable.

High-level shape:

- Add an opt-in stream mode for subject-local versions
- Define the version namespace as the stored concrete subject after subject transforms
- On publish, the server computes the next version for that subject
- The server injects a canonical header such as `Nats-Subject-Version`
- `PubAck` grows a new field for the assigned subject version
- Optional expected-version header can be added for optimistic concurrency

Benefits:

- Server owns the guarantee
- Every stored message carries the canonical version
- Consumers can trust the header without payload conventions

Costs:

- Does not automatically place the version inside the application payload
- Requires stream-mode restrictions to preserve observable gaplessness
- Needs new API surface and migration semantics

### Option C: Full Server-Owned Event-Sourcing Stream Mode

This is the strongest semantic option.

High-level shape:

- Introduce a dedicated stream mode for append-only subject-local event streams
- Require immutability-compatible limits and policies
- Assign canonical per-subject versions
- Reject operations that would later create holes
- Potentially require an expected-subject-version header for writes

Benefits:

- Matches the semantics more honestly
- Avoids pretending a generic JetStream stream can be gapless under all existing behaviors

Costs:

- Largest surface area
- Likely more pushback upstream
- Requires very explicit rules for mirrors, sources, transforms, and admin operations

## Research: How A Server-Native Implementation Could Work Internally

### R1 / Non-Clustered

A straightforward implementation is possible in `processJetStreamMsg`.

- Before `StoreMsg`, load the last message for the concrete subject
- Read the previous subject version from its header
- Compute `next = previous + 1`
- Inject the new version header
- Store the message
- Return the assigned subject version in `PubAck`

This should happen after validation and immediately before successful store, so failed writes do not consume versions.

### Clustered

There are two broad implementation styles.

#### Style 1: Assign Version At Proposal Time

- Leader computes the next subject version before raft proposal
- Proposed bytes already contain the final assigned version

This is attractive because the version is part of the replicated command.

But it introduces harder bookkeeping:

- if a proposal later fails, a pre-assigned version can create a hole
- that would require per-subject equivalents of `clfs`

#### Style 2: Assign Version At Apply Time

- Leader proposes the message without the final subject version embedded
- During `applyStreamEntries` and `processJetStreamMsg`, each replica computes the next version from committed local state
- The version is assigned only when the message is actually being stored

This looks more viable for gaplessness because:

- no version is consumed before actual application
- apply order is deterministic
- failed proposals do not need per-subject failure offset accounting
- batching and pipelining do not invalidate the assigned version because assignment happens at commit/apply time

The tradeoff is that the canonical version would not literally be part of the original raft payload.

My current research bias is that apply-time assignment is the safer semantic fit for gaplessness.

## Research: Whether We Need New Store Metadata

### Initial Implementation Without New Store Indexes

An initial version could avoid deep store changes.

- Use `LoadLastMsg(subject, ...)`
- Read the last committed version from the last stored message header
- Compute the next version from that

Why this is attractive:

- both memory and file stores already support last-by-subject lookup
- restart recovery comes "for free" because the last stored message already carries the canonical version
- lower implementation risk

### More Invasive Optimization Later

A later optimization could persist the last committed subject version in store metadata.

Possible places:

- `SimpleState`
- memstore's per-subject state
- filestore index metadata

Benefits:

- avoids loading/parsing the last message on every publish
- cleaner direct access to current subject version

Costs:

- more invasive changes
- snapshot, recovery, and index compatibility work
- higher migration risk

## Research: Wildcards And Subject Transforms Need A Precise Semantic

The current API already allows expected-last-subject-sequence checks against a filter subject, not only a literal subject.

That does not mean a gapless event version should be defined over arbitrary wildcard filters.

My current view:

- A portable event version should belong to one concrete stored subject
- Subject transforms should probably define the version namespace after transform, because that is the actual stored stream identity
- Wildcard-scoped expected checks are useful as a concurrency primitive, but they are not a good definition for a portable event version namespace

## Research: What Seems Most Practical Right Now

If the requirement is "the payload itself must already contain the final version before publish":

- use a client-side protocol today
- combine last-by-subject read, embedded version, `JSExpectedLastSubjSeq`, and `Nats-Msg-Id`
- standardize it in an SDK so writers cannot drift

If the requirement is "a canonical portable version may live in NATS-managed metadata":

- a server-native opt-in header-based feature looks feasible
- make it append-only in practice by enforcing stream restrictions
- add a matching `PubAck` field

## Research: Concrete Code Touchpoints For A Server-Native Path

- `server/stream.go`
- `server/jetstream_cluster.go`
- `server/jetstream_batching.go`
- `server/store.go`
- `server/memstore.go`
- `server/filestore.go`
- `server/jetstream_api.go`
- `server/jetstream_test.go`
- `server/jetstream_cluster_3_test.go`
- `server/jetstream_cluster_4_test.go`

## Decision Log

These are the decisions that currently look settled.

### Problem Shape

- The problem is portable per-subject event versioning, not merely ordered delivery
- The version must be monotonic and gapless
- Global stream sequence is not an acceptable substitute for the event version

### Rejected Approaches

- Consumer ordering is not enough
- Consumer-local sequence is not enough
- User-land version assignment is not trustworthy enough when batching, pipelining, buffering, retries, or hidden write reordering exist

### Server Ownership

- If this feature exists, the authoritative version must be assigned by the server
- Version assignment should happen at commit/apply time, not in producer code and not at initial proposal time
- Rejected writes must not consume a subject version

### Feature Shape

- This should be an opt-in stream mode, not a generic JetStream behavior
- The stream should behave as append-only for this mode in order to preserve observable gaplessness
- In practice, incompatible retention/deletion features should be rejected or disabled

### Version Namespace

- The server must know the version namespace explicitly
- Wildcard consumers do not define the namespace
- The safest default is the final concrete stored subject after input subject transforms
- If event-sourcing needs a broader identity than the exact subject, that broader namespace must be configured explicitly
- The stated requirement requires support for explicit derived namespace keys such as `events.order.123`

### Event Semantics

- Each stored event should have exactly one canonical authoritative subject version
- There should not be multiple authoritative subject versions on the same event
- Canonical subject versions should be `0`-based
- The first committed event should get version `0`
- "No prior event exists" should be represented explicitly, for example as `no_stream` in precondition checks, not by overloading version `0`

### Metadata Shape

- A canonical server-owned subject version should be stored with the event
- If namespace can differ from the exact stored subject, a version key should also be stored with the event
- Re-deriving the version namespace later from current config is not acceptable for historical events
- Because the required namespace may differ from the stored subject, storing the version key is required

### Current Design Bias

- A header/metadata-based server-native solution is technically credible
- Generic server-side mutation of arbitrary application payloads is not technically credible
- Canonical subject versioning should be header-only by design
- Payload-level final versioning is outside this proposal

## Research: Deferred Future Scope After V1

These items remain open by design, but they are not blockers for v1 implementation.

### Derived Namespaces

What stays deferred:

- nothing about the existence of derived namespaces themselves

What is already required:

- many stored subjects may need to share one version namespace, such as:
  - `events.order.123.created`
  - `events.order.123.cancelled`
  both advancing one logical key like `events.order.123`

### Future Namespace Lookup / Admin API

V1 cannot always lean on existing last-by-subject reads because the version namespace may be a derived key rather than a stored subject.

What stays deferred:

- whether a future namespace-oriented lookup or admin API is needed once derived namespaces exist

Why it is deferred:

- the write-path requirement exists now, but a dedicated admin/introspection surface can still follow later
- the need becomes visible once the namespace can differ from an actual stored subject

## Recommended Design

This is the first design that looks technically credible for the hard requirement.

### 1. Make It An Opt-In Stream Mode

Do not treat this as a generic JetStream behavior.

The final v1 shape should be:

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

Why this shape:

- `subject_versioning` leaves room for future expansion without changing the top-level field name later
- `mode: "gapless"` is explicit enough to be self-describing
- nested `subject_transform` can reuse the existing `SubjectTransformConfig` grammar
- if `subject_transform` is omitted, the namespace defaults to the stored subject
- if `subject_transform` is present, the namespace is derived from the stored subject

Update rule:

- the mode can be set on stream creation
- it can be enabled later only while the stream is empty
- once committed messages exist, it should be treated as immutable

### 2. Define The Version Namespace Precisely

The server must know the version namespace explicitly.

It cannot infer this safely from arbitrary wildcard usage or consumer filters.

So there are really two defensible choices:

- default: the version belongs to the final concrete stored subject
- advanced: the version belongs to an explicitly configured derived subject namespace

That means:

- if an input subject transform runs, the version namespace is based on the transformed subject
- wildcard filters are not the version namespace
- a version is always attached to one concrete event stream identity

For v1, my recommendation is:

- support explicit transform-derived namespaces in v1
- still store `Nats-Subject-Version-Key` on every event
- admit that this likely requires persisted namespace state or equivalent index metadata

### 2a. Why Exact Subject Is The Safest Default

It is the only choice that is generic and unambiguous.

If a message is stored under:

- `events.order.123.created`

then the simplest rule is:

- the version is for `events.order.123.created`

This is mechanically clear, but it is too narrow for the stated event-sourcing requirement if multiple event types should share one version line.

### 2b. Why Event Sourcing May Need A Derived Namespace

If these two subjects should share the same version counter:

- `events.order.123.created`
- `events.order.123.cancelled`

then the real stream identity is not the full subject.

It is something like:

- `events.order.123`

That means the feature should not pretend the exact stored subject is always the right identity.

Instead, it would need explicit configuration for a version namespace, for example:

- exact stored subject
- a configured subject transform used only for versioning
- a token template such as `events.order.*.* -> events.order.*`

The important point is:

- this must be explicit in stream config
- it must never be inferred from how consumers subscribe

### 2c. Why A Version Key Is Needed

If the feature supports only:

- exact stored subject

then the version key is mostly redundant, because the message subject already tells you the namespace.

But as soon as the feature supports:

- derived version namespaces
- subject transforms
- aggregate-style event streams where many event types share one version line

then the version number alone is no longer self-describing.

Example:

- stored subject: `events.order.123.cancelled`
- assigned version: `42`

Without a version key, `42` is ambiguous:

- is it version `42` of `events.order.123.cancelled`?
- is it version `42` of `events.order.123`?
- is it version `42` under some transform-derived namespace?

That ambiguity is exactly what the feature is supposed to remove.

So if derived namespaces are allowed, each stored event should carry both:

- `Nats-Subject-Version: 42`
- `Nats-Subject-Version-Key: events.order.123`

### 2d. Why The Key Should Be Stored On The Event, Not Re-Derived Later

Re-deriving from current stream config is unsafe.

Reasons:

- historical events should keep the exact namespace semantics they were written under
- stream config may change over time
- subject transform rules may change over time
- replaying old events should not reinterpret their version namespace based on today's configuration

So the version key is not only for convenience.

It makes the meaning of the version immutable and inspectable on the event itself.

### 2e. Practical Rule

The most defensible rule is:

- if version namespace is exact stored subject, the key may be omitted because it is identical to the subject
- if version namespace can differ from stored subject, the key should be stored explicitly on every event

My bias is to store it whenever the feature is enabled, even if redundant in the exact-subject case, because:

- it keeps the wire semantics uniform
- it makes debugging easier
- it avoids clients having to know which mode produced a given event

### 2f. Should One Event Ever Have More Than One Subject Version?

My current answer is:

- not as authoritative semantics

There should be exactly one canonical subject version per stored event for this feature.

Why:

- a gapless version only makes sense if there is one clear counter identity
- optimistic concurrency needs one authoritative expected-version target
- `PubAck` should return one authoritative assigned version
- replay and debugging should not require asking "which of these versions is the real one?"

If one event carried multiple authoritative versions, several things become unclear:

- which one controls write acceptance?
- which one is guaranteed gapless?
- which one should downstream systems trust?
- which one should be shown as "the" event version?

So the cleaner rule is:

- one event
- one version namespace key
- one canonical subject version

If there is a future need for additional counters, they should be modeled as:

- separate non-authoritative metadata
- or a different feature entirely

But they should not all be treated as "the subject version".

### 3. Assign The Version At Commit / Apply Time

This is the critical rule.

Do not assign the subject version in client code.
Do not assign it when the leader first receives the publish.
Do not assign it when proposing the raft entry.

Assign it only when the message is actually being applied and stored.

Why:

- batching no longer breaks correctness
- pipelining no longer breaks correctness
- retries do not pre-consume versions
- failed proposals do not create subject-version holes
- the assigned version matches the actual committed order

### 4. Add Canonical Stored Metadata

Store the assigned metadata in canonical headers, for example:

- `Nats-Subject-Version`
- `Nats-Subject-Version-Key`

These headers become the server-owned truth.

Every stored event in this mode carries them.

Version numbering should be `0`-based:

- the first committed event gets version `0`
- an empty namespace is an explicit state such as `no_stream`, not a numeric sentinel

Final v1 header names should be exactly:

- `Nats-Subject-Version`
- `Nats-Subject-Version-Key`

### 5. Add Optimistic Concurrency On Subject Version

Add a write header for event-sourcing semantics, for example:

- `Nats-Expected-Last-Subject-Version`

Write behavior:

- if omitted, server appends blindly and assigns the next version
- if present with an integer, server verifies the current committed subject version equals the expected value
- if present as `no_stream`, server verifies that no committed event exists yet for the namespace
- if it does not match, reject the write
- rejected writes do not consume a subject version

This is different from the current `JSExpectedLastSubjSeq`, which is based on stream sequence, not canonical subject version.

Final v1 parsing rule:

- the header value `no_stream` means no committed event may yet exist for that version namespace
- any other accepted value must be an unsigned integer revision
- any other non-empty value is invalid

Final error recommendation:

- invalid value -> `JSStreamExpectedLastSubjectVersionInvalidErr`
- mismatch -> `JSStreamWrongLastSubjectVersionErrF`

Compatibility rule:

- if subject versioning is not enabled on the stream, `Nats-Expected-Last-Subject-Version` should be rejected
- `Nats-Expected-Last-Subject-Sequence` and `Nats-Expected-Last-Subject-Sequence-Subject` remain independent and may still be used on subject-versioned streams

### 6. Return The Assigned Version In PubAck

Extend `PubAck` with fields such as:

- `subject_version`
- `subject_version_key`

That gives the client the exact version assigned at commit time and the namespace it belongs to.

### 7. Enforce Append-Only Semantics On The Stream

If the stream can later delete, purge, age out, or roll up messages, then observable gaplessness is gone.

So this mode should reject or disable:

- `MaxAge`
- `MaxMsgs`
- `MaxBytes`
- `MaxMsgsPer`
- delete
- purge
- rollup
- per-message TTL
- subject delete markers
- interest/work-queue retention modes

Practical shape:

- allow only append-only retention semantics
- reject writes when storage/resources are exhausted instead of evicting older data

Final v1 validation should be stricter and explicit:

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

This is stricter than the theoretical minimum, but I think that is the right call for v1. If we ever want to support finite but non-evicting limits later, that can be revisited as a separate design decision instead of being smuggled into the first implementation.

### 8. Limit The First Version Of The Feature

To keep it technically credible, the first version should probably reject:

- mirrors
- sources
- republish
- scheduled messages

Those features can be revisited later once the core version semantics are stable.

## Recommended Internal Flow

### Non-Clustered

On publish:

1. resolve the final concrete stored subject
2. resolve the version namespace key from `subject_versioning.subject_transform` if configured, otherwise use the stored subject
3. load the current committed version state for that namespace key, if any
4. determine the current state from committed namespace state; if no version exists, the state is `no_stream`
5. validate `Nats-Expected-Last-Subject-Version` if present
6. assign `next = 0` if the current state is `no_stream`, otherwise `current + 1`
7. inject `Nats-Subject-Version: next` and `Nats-Subject-Version-Key: <namespace-key>`
8. store the message
9. return `subject_version = next` and `subject_version_key = <namespace-key>` in `PubAck`

### Clustered

On leader receive:

1. accept the publish request
2. carry any expected-subject-version requirement in the proposed entry
3. do not pre-assign the final version

On apply:

1. resolve the final concrete stored subject
2. resolve the version namespace key from `subject_versioning.subject_transform` if configured, otherwise use the stored subject
3. load the current committed version state for that namespace key from committed local state, if any
4. determine the current state from committed namespace state; if no version exists, the state is `no_stream`
5. validate expected-subject-version if present
6. assign `next = 0` if the current state is `no_stream`, otherwise `current + 1`
7. inject the canonical metadata
8. store the message
9. send `PubAck` with the assigned version and key

This keeps the version tied to the real commit order instead of proposal order.

## The Payload Problem

This proposal does not attempt to put the canonical subject version into the application payload.

Why:

- the server can add headers
- the server can return richer `PubAck`
- the server cannot safely rewrite every possible event payload format

So the design choice here is:

- canonical subject version lives in headers / metadata
- payload shape remains an application concern

If some downstream path fails to preserve headers, that path is failing to preserve the canonical subject-version contract. That is not something this feature should solve by moving canonical versioning into arbitrary payloads.

## Research: Contract Details That Should Be Written Down Explicitly

### Invariants

These should be explicit because the feature gets hand-wavy fast without them:

- one stored event has one authoritative subject version
- versions are `0`-based
- only committed writes consume versions
- rejected writes do not consume versions
- duplicate writes return the original assigned version
- observable gaplessness depends on append-only behavior after the write as well, not only at write time

### Public Contract Recommendation

The public contract should be explicit enough that clients can reason about it without reading the server code.

Recommended pieces:

- stream config enables an opt-in mode such as `subject_versioning.mode = gapless`
- v1 version namespace is the derived key produced from the final stored subject by `subject_versioning.subject_transform`, or the stored subject itself when no version transform is configured
- stored canonical headers:
  - `Nats-Subject-Version`
  - `Nats-Subject-Version-Key`
- publish precondition header:
  - `Nats-Expected-Last-Subject-Version`
- `PubAck` extension:
  - `subject_version`
  - `subject_version_key`

### Header Ownership Rules

This needs to be explicit or the semantics will be muddy.

My recommendation:

- `Nats-Subject-Version` is server-owned and client-supplied values should be rejected
- `Nats-Subject-Version-Key` is server-owned and client-supplied values should be rejected
- `Nats-Expected-Last-Subject-Version` is client-supplied command metadata and should be validated but not stored as canonical event metadata

Rejecting spoofed canonical headers is better than silently overwriting them because:

- it prevents hidden producer bugs
- it makes the ownership boundary obvious
- it keeps replay semantics cleaner

### Expected-Version Semantics

This needs to be more explicit than "there is some expected version header".

Recommended rule:

- if the header is absent, the server may blindly append and assign the next version
- if the header is an integer, it means "the current committed version for this namespace must equal this integer"
- if the header is `no_stream`, it means "no committed event must yet exist for this namespace"
- mismatch rejects the write and consumes no version

The semantic contract should be:

- invalid value -> `JSStreamExpectedLastSubjectVersionInvalidErr`
- mismatch -> `JSStreamWrongLastSubjectVersionErrF`
- the error should identify the namespace key plus expected/current revision values at least in the description

## Research: Duplicate, Batch, And Failure Semantics

### Duplicate Publish Behavior

This absolutely has to be documented because retries are where trust usually falls apart.

Recommended rule:

- `Nats-Msg-Id` remains the idempotency primitive
- if the server identifies a committed duplicate, it should return the original `seq`, `subject_version`, and `subject_version_key`
- it should also mark the `PubAck` as `duplicate=true`
- if there is only an in-process duplicate conflict and no committed result yet, no version is consumed

That preserves idempotent retry semantics without inventing holes.

### Batch Semantics

Batching has to be spelled out because it is one of the reasons user-land was rejected.

For atomic batch publish:

- the messages in the batch should be assigned versions in batch order
- if multiple messages target the same version namespace, a batch-local overlay should advance that namespace within the batch
- if the batch fails, none of those provisional assignments become visible

For non-atomic / fast batch publish:

- each message is still assigned at its own commit/apply point
- failures consume nothing
- only the first message for a given namespace in a fast batch may carry `Nats-Expected-Last-Subject-Version`
- later fast-batch messages for the same namespace must omit that header so behavior does not depend on race timing between proposal and apply
- the authoritative order is still actual commit order, not submission order

Worked example for atomic batch publish:

- current committed state:
  - `orders.123` is at revision `4`
  - `orders.999` is at revision `1`
- client sends one atomic batch in this order:
  1. `orders.123`
  2. `orders.999`
  3. `orders.123`
- provisional batch-local assignment becomes:
  1. `orders.123` -> `5`
  2. `orders.999` -> `2`
  3. `orders.123` -> `6`

The important detail is that the second `orders.123` message must see the first `orders.123` message's provisional advance inside the same batch. Otherwise the batch could assign `5` twice, which would be wrong.

If the atomic batch fails, none of those revisions are consumed.

Worked example for non-atomic / fast batch publish:

- current committed state:
  - `orders.123` is at revision `4`
- client submits two messages for `orders.123` in this order:
  1. message `A`
  2. message `B`
- because the writes are independent, `B` reaches commit/apply first
- resulting revisions:
  - `B` -> `5`
  - `A` -> `6`

That looks surprising only if someone assumes submission order is authoritative. In this design it is not. Commit/apply order is authoritative.

### Cluster, Replay, And Failover Semantics

This is central to whether "gapless" is believable.

Recommended rule set:

- leader acceptance alone must not consume a version
- proposal alone must not consume a version
- only actual commit/apply should consume a version
- leader failover before apply must consume nothing
- restart recovery should derive the current version from committed stored state
- deterministic apply order should make all replicas derive the same next version

One practical consequence of the required derived-namespace support is:

- existing last-by-subject primitives are no longer enough by themselves to recover current revision state for every namespace

## Research: Namespace Resolution Rules

These rules need to be explicit because otherwise people will project wildcard semantics onto the feature.

Recommended v1 resolution order:

1. client publishes to an input subject
2. stream input subject transform runs, if configured
3. the server gets the final concrete stored subject
4. if `subject_versioning.subject_transform` is configured, the server derives the version key from the stored subject
5. otherwise the stored subject itself becomes `Nats-Subject-Version-Key`
6. version lookup and assignment happen under that key

Important exclusions:

- wildcard consumers do not define the namespace
- filter subjects do not define the namespace
- batch grouping does not define the namespace

The moment where the second transform runs must be specified explicitly. It should never be left to inference.

## Research: Migration And Compatibility

This needs to be documented because retroactive guarantees are dangerous.

My recommendation for v1:

- enable the mode when creating a new stream
- allow enabling it on an existing stream only if the stream is empty
- reject enabling it on a non-empty stream

Why:

- old messages do not carry canonical subject-version metadata
- their historical gaplessness cannot be proven retroactively
- offline backfill/migration is possible in theory, but should not be part of the initial feature contract

Compatibility notes:

- old consumers can ignore the new headers
- old clients can ignore the new `PubAck` fields
- the messages are still ordinary JetStream messages with additional metadata

## Research: Performance And Storage Cost

This is worth documenting because the required derived-namespace mode has a higher implementation cost than exact-subject-only.

For the required aggregate-style namespace support:

- existing `LoadLastMsg(subject, ...)` is not enough by itself
- a derived namespace like `events.order.123` may not be an actual stored subject
- that likely forces new persisted namespace state or index metadata
- recovery must rebuild or persist namespace-level current revision state

Pressure points:

- very high-cardinality namespaces will amplify the amount of maintained state
- returning more metadata in `PubAck` adds small but real wire cost

This is the main reason the feature is more invasive than an exact-subject-only design.

## Research: Store Shape Decision

Current recommendation:

- `memStore` should keep a separate in-memory namespace tree keyed by `Nats-Subject-Version-Key`
- `fileStore` should keep the same namespace tree in memory and persist it as a stream-level checkpoint side file
- the file-store checkpoint should encode a checkpoint sequence plus namespace entries containing at least `last_version` and `last_seq`
- recovery should load that checkpoint and then catch up by scanning committed messages from the checkpoint sequence to the current last sequence

This means:

- namespace version state lives in the store layer
- it does not live in existing real-subject indexes such as `fss` or `psim`
- committed messages remain the source of truth if checkpoint state is stale or missing

What we are explicitly not recommending for v1:

- overloading `fss`
- overloading `psim` or block `fss`
- discovering the current namespace version by scanning history on every write
- forcing a new per-block namespace sidecar/index format before the first version ships

## Research: Current Implementation Concerns

These are not product-level doubts anymore. They are the remaining places where a poor implementation choice could still hurt performance, correctness, or maintainability.

### 1. Namespace Entry Shape

Concern:

- the first namespace entry struct should stay minimal

Why:

- the write path only needs the latest committed facts
- over-designing the entry too early makes persistence, checkpointing, and recovery harder to evolve

Current bias:

- start with:
  - `last_version`
  - `last_seq`

### 2. Store API Boundary

Concern:

- whether namespace state should be exposed through `StreamStore` or kept as store-internal machinery

Why:

- if it is exposed too early, we freeze an API before learning what the write path really needs
- if it stays entirely hidden, the stream layer can become awkward or duplicate logic

Current bias:

- keep the first implementation as store-internal helpers unless a clean `StreamStore` abstraction becomes obviously necessary

### 3. `fileStore` Checkpoint Cadence

Concern:

- how often the namespace checkpoint side file should be written

Why:

- writing too often adds steady-state overhead
- writing too rarely increases restart catch-up scan cost

Current bias:

- follow the existing checkpoint-and-catch-up model used by TTL and scheduling state
- treat the file as a best-effort checkpoint, not a transactional source of truth

### 4. Duplicate `PubAck` Cost

Concern:

- how to return original `subject_version` metadata on duplicate publish without creating a hot slow path

Why:

- the simplest design is to reload the original stored message and read the canonical headers
- that is correct, but it may be more expensive than necessary under heavy duplicate/retry traffic

Current bias:

- start with correctness by loading the original stored message on committed duplicate
- optimize later only if measurements justify caching the subject-version metadata alongside duplicate state

### 5. Atomic Batch Namespace Overlay

Concern:

- the batch-local overlay keyed by namespace must be simple enough to trust under concurrency

Why:

- atomic batch already has staged consistency machinery
- adding namespace revision overlays is straightforward conceptually, but easy to get subtly wrong if the state is too clever

Current bias:

- use a plain namespace-key -> provisional latest version map inside the staged batch diff
- avoid introducing a second complicated batch-specific indexing layer

### 6. Recovery Ordering

Concern:

- namespace state must be ready before new writes are accepted after restart or leadership change

Why:

- if writes resume before namespace state is rebuilt or caught up, the next assigned version can be wrong

Current bias:

- recovery of namespace state is a startup gate for subject-versioned streams, not optional background work

### 7. High-Cardinality Namespace Pressure

Concern:

- very high-cardinality aggregate keys can grow memory use and checkpoint size quickly

Why:

- each namespace requires maintained live state
- file-store checkpoints will grow with namespace count

Current bias:

- accept this cost for v1 because it is inherent to the guarantee
- measure it explicitly rather than weakening semantics to hide it

## Research: Frozen Implementation Defaults

These are the defaults I would now treat as fixed for the first implementation.

### 1. Namespace Entry Struct

Decision:

- the namespace key lives in the map/tree key
- the namespace entry value stores exactly:
  - `last_version uint64`
  - `last_seq uint64`

Not in v1:

- timestamps
- counters for stats
- embedded copies of the namespace key
- extra metadata for diagnostics

Why:

- the write path only needs the latest committed revision and the stream sequence that produced it
- anything more should wait until a concrete need appears

### 2. Store API Boundary

Decision:

- do not add namespace-version methods to `StreamStore` in v1
- implement namespace lookup/update as store-internal helpers inside package `server`

Shape:

- `memStore` and `fileStore` get narrow internal helpers for:
  - lookup current namespace entry
  - update namespace entry after committed store
  - rebuild/reset namespace state

Why:

- this avoids freezing a broader interface too early
- it keeps the first implementation free to change shape while the design settles in code

### 3. `fileStore` Checkpoint File

Decision:

- use a stream-level side-state file named `sver.db`

Format:

- file magic
- file version
- checkpoint sequence
- entry count
- repeated records of:
  - namespace key length
  - namespace key bytes
  - `last_version`
  - `last_seq`

Encoding bias:

- use the same style of compact binary encoding already used by other store side-state files
- use uvarint encoding for integer fields

Write cadence:

- do not write this file per message
- write it on the normal `fileStore` side-state/sync cadence
- write it on clean shutdown
- write it after recovery rebuild if the recovered in-memory state had to be reconstructed

Recovery rule:

- load `sver.db` if present
- restore the namespace map
- catch up by scanning committed messages from `checkpoint_seq` through current `LastSeq`

### 4. Duplicate `PubAck` Strategy

Decision:

- optimize for correctness first

Behavior:

- when a committed duplicate is detected, load the original stored message
- read `Nats-Subject-Version`
- read `Nats-Subject-Version-Key`
- return those original values in `PubAck`

Not in v1:

- caching subject-version metadata inside duplicate-tracking entries

Why:

- the simpler path is easier to trust
- caching can be added later only if duplicate-heavy workloads prove this is too expensive

### 5. Atomic Batch Namespace Overlay

Decision:

- use a plain batch-local map:
  - `map[string]uint64`
- key:
  - namespace key
- value:
  - provisional latest version for that namespace inside the batch

Rules:

- the first message for a namespace in the batch may carry `Nats-Expected-Last-Subject-Version`
- that first message validates against committed namespace state
- later messages for the same namespace in the same atomic batch must not carry a separate expected-version precondition
- later messages advance from the batch-local provisional version

Why:

- this keeps the overlay obvious
- it avoids inventing a second complicated batch-specific state model

### 6. Recovery Gate Behavior

Decision:

- namespace state recovery is a gate before writes resume

`memStore`:

- rebuild or clear namespace state synchronously on reset-style operations

`fileStore`:

- recover namespace checkpoint state during store recovery
- run the catch-up scan before the stream is considered ready for writes

Clustered behavior:

- a subject-versioned stream must not accept new leader writes until local namespace recovery is complete

Why:

- if writes resume before namespace state is ready, the next assigned version can be wrong

### 7. Final Scope Reminder

These defaults intentionally assume:

- header-only canonical version metadata
- config-driven derived namespace selection
- separate namespace state in the stores
- correctness-first duplicate handling
- checkpoint-plus-catch-up recovery in `fileStore`

That is enough to start implementation without reopening the design.

## Research: What The Current Codebase Suggests About The Remaining Work

The good news is that the current codebase already has the right broad write model:

- atomic batches already stage and isolate a whole batch before commit
- fast batches already treat each message as independently validated and committed
- clustered apply already has cleanup hooks for in-flight per-subject consistency state

The hard part is narrower:

- the current server assumes that subject-local state is keyed by a real stored subject
- the required event-sourcing design needs state keyed by a derived namespace that may not itself be stored

### 1. Namespace State / Index

What the codebase already tells us:

- `StreamStore` exposes `LoadLastMsg(subject, ...)`, `SubjectsState(...)`, and related subject APIs, all keyed by actual subjects
- `memStore` uses `fss` as a `SubjectTree[SimpleState]` keyed by actual stored subjects
- `fileStore` uses `psim` and per-block `fss`, also keyed by actual stored subjects

What that implies:

- a derived namespace like `events.order.123` cannot rely on existing subject indexes if no message is actually stored on `events.order.123`
- we should not fake this with repeated scans or wildcard lookbacks on every publish

Practical conclusion:

- the feature likely needs explicit store-managed namespace revision state keyed by `Nats-Subject-Version-Key`
- that state should be treated as first-class persisted state in both `memStore` and `fileStore`

### 1a. Recommended Shape In `memStore`

For `memStore`, the cleanest shape is:

- add a separate in-memory namespace tree, not a hack on top of `fss`
- key it by `Nats-Subject-Version-Key`
- store only the latest committed namespace facts needed for the write path

Suggested record:

- `last_version`
- `last_seq`

Why this shape:

- `fss` is about actual stored subjects
- namespace version state is about canonical derived keys
- mixing the two would blur different semantics and make the code harder to trust

Practical behavior:

- on every store of a subject-versioned message, parse the canonical headers and update the namespace entry
- on `Purge`, `Compact`, `Truncate`, or any reset-style path, clear or rebuild this namespace state from the current in-memory messages
- because `memStore` is not restart-persistent, no separate on-disk encoding is needed there

### 1b. Recommended Shape In `fileStore`

For `fileStore`, the most credible v1 shape is:

- keep an in-memory namespace tree keyed by `Nats-Subject-Version-Key`
- persist it as a stream-level side-state file, similar in spirit to TTL and scheduling state
- treat that file as a checkpoint, not the only source of truth

Suggested contents:

- checkpoint sequence up to which the namespace state is known to be encoded
- for each namespace key:
  - `last_version`
  - `last_seq`

Recovery shape:

- load the namespace state file if it exists
- decode the checkpointed namespace map
- linearly scan committed messages from `checkpoint_seq` to `LastSeq`
- apply any newer `Nats-Subject-Version` / `Nats-Subject-Version-Key` updates found in headers

Why this is the best fit for v1:

- it matches the existing file-store pattern used by TTL and scheduling state
- it avoids write-path scans
- it avoids forcing per-block namespace sidecars immediately
- it keeps committed messages as the source of truth if the side file is stale or missing

What I would avoid for v1:

- trying to overload `psim` / block `fss` with namespace semantics
- relying on full-stream scans on every startup without a checkpoint file
- making the write path search history to discover the current namespace version

### 2. In-Flight Concurrency Protection

What the codebase already tells us:

- `batchStagedDiff.expectedPerSubject`
- `mset.expectedPerSubjectInProcess`
- `mset.expectedPerSubjectSequence`

already implement the pattern "block conflicting writers on the same key while the proposal is unresolved".

What that implies:

- we do not need a new concurrency model
- we need the same model, but keyed by version namespace rather than only by stored subject

Practical conclusion:

- the current expected-per-subject machinery is a strong template for expected-per-namespace behavior
- cleanup should still be tied to clustered sequence / apply completion exactly the way it is today

### 3. Duplicate Publish Semantics

What the codebase already tells us:

- duplicate detection already exists through `checkMsgId` / `storeMsgId`
- duplicate `PubAck` today already returns the original stream sequence and `duplicate=true`

What that implies:

- the missing part is not duplicate detection itself
- the missing part is attaching the original `subject_version` and `subject_version_key` to that duplicate response

Practical conclusion:

- we should reuse the current duplicate-detection path
- on committed duplicate, the server should resolve the original stored message and read the canonical headers back out
- if that turns out to be too expensive, a later optimization could cache subject-version metadata alongside the duplicate entry

### 4. Batch Semantics

What the codebase already tells us:

- atomic batch publish already performs a staged consistency pass using `batchStagedDiff`
- atomic batch commit already uses `ProposeMulti`
- fast batch publish already validates and proposes messages independently

What that implies:

- atomic batch already has the right place to hold a batch-local overlay of provisional namespace revisions
- fast batch already has the right semantics for "commit/apply order wins"

Practical conclusion:

- the main implementation work is extending the staged diff to carry namespace-key revision state
- the batch model itself does not need to be reinvented

### 5. Recovery, Snapshot, And Replay

What the codebase already tells us:

- `EncodedStreamState` currently snapshots only global replicated stream state such as:
  - `Msgs`
  - `Bytes`
  - `FirstSeq`
  - `LastSeq`
  - `Failed`
  - `Deleted`
- it does not carry arbitrary namespace-revision state

What that implies:

- derived namespace recovery does not come "for free" from the current binary stream snapshot
- if we add namespace-key revision state, we must decide how it survives recovery

Practical conclusion:

- either store-level recovery must rebuild namespace revision state from committed stored messages before writes resume
- or the store/snapshot formats must be extended to persist namespace state directly

The codebase strongly argues against pretending this part is optional.

### 6. Observability

What the codebase already tells us:

- existing last-by-subject and direct-get paths are useful only when the lookup key is an actual stored subject

What that implies:

- once derived namespaces exist, introspection by namespace key is no longer covered by current subject APIs

Practical conclusion:

- this is a real follow-up item, but not a blocker for write-path correctness
- we should not hold the core feature on a new admin API, but we also should not pretend current lookup surfaces fully explain derived namespace state

## Research: Observability And Introspection

This should be documented because debugging a version mismatch without tooling will be painful.

Useful baseline capabilities:

- `PubAck` tells the publisher the assigned version and version key
- mismatch errors should mention namespace key and conflicting revisions
- tracing / advisories should surface assignment and mismatch events

One important note:

- because derived namespaces are part of the core requirement, last-by-subject is no longer sufficient for every namespace lookup, so the server will likely need a dedicated admin/introspection path later even if the initial write path ships first

## Research: Alternatives That Should Stay Rejected

These should remain documented so the proposal does not drift back into weaker semantics later.

- user-land authoritative version assignment
- global stream sequence as event version
- consumer-local sequence as event version
- silent overwrite of client-supplied canonical version headers
- multiple authoritative subject versions on one event
- generic mutation of arbitrary application payloads

## Research: Illustrative Protocol Flows

These are not final wire encodings. They are here to make the intended contract concrete.

### First Event On A New Subject

Example:

- publish subject:
  - `orders.123`
- client header:
  - `Nats-Expected-Last-Subject-Version: no_stream`

Committed result:

- stored headers:
  - `Nats-Subject-Version: 0`
  - `Nats-Subject-Version-Key: orders.123`
- `PubAck` includes:
  - `subject_version: 0`
  - `subject_version_key: orders.123`

This is the create-if-empty path for a brand new version namespace.

### Normal Append

Example:

- current committed state:
  - `orders.123` is at subject version `4`
- client header:
  - `Nats-Expected-Last-Subject-Version: 4`

Committed result:

- stored headers:
  - `Nats-Subject-Version: 5`
  - `Nats-Subject-Version-Key: orders.123`
- `PubAck` includes:
  - `subject_version: 5`
  - `subject_version_key: orders.123`

### Mismatch

Example:

- current committed state:
  - `orders.123` is at subject version `5`
- client header:
  - `Nats-Expected-Last-Subject-Version: 4`

Result:

- write is rejected
- no subject version is consumed
- the error should identify the version key plus expected/current values

### Duplicate Retry

Example:

- first committed publish:
  - `Nats-Msg-Id: abc-123`
  - assigned `subject_version: 5`
- retry with the same `Nats-Msg-Id`

Result:

- no new subject version is assigned
- `PubAck` returns the original stream sequence
- `PubAck` returns the original `subject_version`
- `PubAck` returns the original `subject_version_key`
- `duplicate=true`

## Research: Suggested Incremental Implementation Plan

If this moves from proposal to code, I would stage it like this.

### Slice 1: Configuration And Validation

Goal:

- add the opt-in stream mode
- reject incompatible stream settings up front

Likely touchpoints:

- `server/stream.go`
- `server/jetstream_api.go`
- stream config validation paths

Expected outcomes:

- subject-versioning config exists, including explicit namespace derivation via `subject_versioning.subject_transform`
- non-empty stream enablement is rejected
- incompatible retention/deletion features are rejected

### Slice 2: Namespace State / Index

Goal:

- support current-version lookup by derived namespace key rather than only by stored subject

Likely touchpoints:

- `server/store.go`
- `server/memstore.go`
- `server/filestore.go`

Expected outcomes:

- the server can read current committed revision for a derived namespace key
- recovery preserves or reconstructs namespace revision state
- the design does not require a write-path scan to find the next version

### Slice 3: Standalone Write Path

Goal:

- assign and store canonical subject version metadata in standalone mode

Likely touchpoints:

- `server/stream.go`
- `server/store.go`
- `server/memstore.go`
- `server/filestore.go`

Expected outcomes:

- `Nats-Subject-Version` and `Nats-Subject-Version-Key` are injected at store time
- `Nats-Expected-Last-Subject-Version` is validated
- `PubAck` returns `subject_version` and `subject_version_key`

### Slice 4: Duplicate And Retry Semantics

Goal:

- preserve idempotent retry behavior without consuming new subject versions

Likely touchpoints:

- `server/stream.go`
- existing duplicate detection and `PubAck` response paths

Expected outcomes:

- committed duplicates return original version/key
- in-process duplicate conflicts consume nothing

### Slice 5: Clustered Apply-Time Assignment

Goal:

- make clustered behavior match standalone semantics while keeping assignment at apply time

Likely touchpoints:

- `server/jetstream_cluster.go`
- `server/jetstream_batching.go`
- clustered publish validation paths

Expected outcomes:

- leader receive does not consume a subject version
- apply-time assignment is deterministic across replicas
- failover before apply consumes nothing

### Slice 6: Batch Semantics

Goal:

- make atomic and non-atomic batch behavior obey the same per-subject invariants

Likely touchpoints:

- `server/jetstream_batching.go`
- batch publish ack paths

Expected outcomes:

- atomic batches use a batch-local overlay for repeated subjects
- failed atomic batches consume no subject version
- non-atomic batch writes assign by actual commit/apply order

### Slice 7: Hardening And Recovery

Goal:

- prove the invariants under restart, recovery, and edge cases

Likely touchpoints:

- `server/filestore.go`
- `server/memstore.go`
- restart/recovery tests

Expected outcomes:

- recovery derives current subject version from committed state
- no synthetic hole appears after restart or leader change

## Research: Suggested Test Matrix

This should be explicit now so implementation work has a target.

### Standalone

- first write with `no_stream` gets subject version `0`
- second write gets subject version `1`
- wrong expected version is rejected and consumes nothing
- invalid `Nats-Expected-Last-Subject-Version` values are rejected
- `Nats-Expected-Last-Subject-Version` on a stream without subject versioning is rejected
- `Nats-Expected-Last-Subject-Sequence` remains independent on a subject-versioned stream
- client-supplied canonical headers are rejected
- duplicate `Nats-Msg-Id` returns original version/key

### Namespace Resolution Behavior

- `subject_versioning.subject_transform` derives one shared key for:
  - `events.order.123.created`
  - `events.order.123.cancelled`
- the assigned versions advance under `events.order.123`, not separately under each full stored subject
- input subject transform still runs before namespace derivation
- wildcard consumers do not affect namespace choice

### Batching

- atomic batch with repeated same subject assigns contiguous versions in batch order
- atomic batch abort consumes nothing
- non-atomic batch with reordered commits assigns by actual commit/apply order
- mixed-subject batch advances each subject independently

### Clustered

- concurrent publishers to the same subject do not create holes
- failover before apply consumes nothing
- retry after leader change preserves duplicate behavior
- all replicas converge on the same assigned versions

### Recovery

- restart from stored state preserves next subject version
- file store and memory store both preserve the invariant

### Configuration Validation

- enabling mode on non-empty stream is rejected
- enabling mode with incompatible retention/deletion options is rejected
- mirrors, sources, republish, and scheduled messages are rejected in v1

## Research: Definition Of Done For V1

I would treat v1 as done only if all of these are true:

- the standalone write path enforces the contract
- the clustered write path enforces the same contract
- duplicates return original subject version metadata
- batch behavior is explicitly tested for atomic and non-atomic modes
- restart and failover do not create gaps
- incompatible configs are rejected
- namespace revision lookup does not rely on scanning committed history per write
- docs and API surface match the actual implementation

## Discussion Notes

- We now have the real requirement direction from the Slack thread
- The core issue is per-subject event versioning, not merely subject ordering
- The strongest constraint is that the version must remain useful outside NATS
- Gaplessness is a hard semantic requirement, not an optimization target
- I should avoid collapsing the problem into "ordered consumer semantics" because that does not answer the portability/versioning need
