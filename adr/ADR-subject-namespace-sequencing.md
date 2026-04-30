# Subject Namespace Sequencing for JetStream Streams

| Metadata | Value |
| --- | --- |
| Date | 2026-04-30 |
| Status | Draft for maintainer discussion |
| Tags | jetstream, server, subject-sequence, adr-draft |

## Context

JetStream has a global stream sequence and existing subject-scoped optimistic concurrency through `Nats-Expected-Last-Subject-Sequence`, but there is no server-assigned sequence scoped to an explicit logical namespace that is stable across batching, retries, pipelining, and replicated apply order.

Some applications need a committed-message sequence line that is narrower than the stream and sometimes broader than the final stored subject. For example:

| Publish subject | Stored subject | Namespace key | Namespace sequence |
| --- | --- | --- | --- |
| `orders.123` | `orders.123` | `orders.123` | `0` |
| `orders.123` | `orders.123` | `orders.123` | `1` |
| `orders.456` | `orders.456` | `orders.456` | `0` |
| `events.order.123.created` | `events.order.123.created` | `events.order.123` | `0` |
| `events.order.123.cancelled` | `events.order.123.cancelled` | `events.order.123` | `1` |

The namespace must not be inferred from consumers. Wildcard consumers describe delivery interest, not the write-time identity that owns a sequence line.

## Decision

Add an opt-in stream mode that assigns a server-owned sequence to each committed message within an explicit namespace.

The proposed v1 configuration shape is:

```json
{
  "subject_versioning": {
    "mode": "gapless",
    "subject_transform": {
      "src": "events.*.*.*",
      "dest": "events.$1.$2"
    }
  }
}
```

The nested `subject_transform` is not the stream's top-level subject transform and does not rewrite where the message is stored. It only derives the namespace key used for sequencing from the final stored subject.

With the example above:

| Stored subject | Derived namespace key | Assigned namespace sequence |
| --- | --- | --- |
| `events.order.123.created` | `events.order.123` | `0` |
| `events.order.123.cancelled` | `events.order.123` | `1` |
| `events.invoice.900.issued` | `events.invoice.900` | `0` |

The proposed public name is still open. The branch currently uses `subject_versioning`, `Nats-Subject-Version`, and `Nats-Expected-Last-Subject-Version`, but a rename toward `subject_sequence` or subject namespace sequencing should happen only after maintainer preference is clear because it affects config, headers, generated errors, and tests.

## Namespace Derivation

Namespace derivation is explicit and based on the final stored subject:

1. Accept the publish subject.
2. Apply any stream-level input subject transform.
3. Store the message under the final stored subject.
4. If `subject_versioning.subject_transform` is configured, derive the namespace key from the stored subject without changing the stored subject.
5. Otherwise, use the stored subject as the namespace key.

This supports exact-subject sequencing and grouped namespace sequencing without coupling the write contract to consumers.

## Stored Metadata

The branch currently stores server-owned metadata headers on committed messages:

- `Nats-Subject-Version`
- `Nats-Subject-Version-Key`

The payload is not mutated. Client publishes that include either server-owned header are rejected.

The reason to store these headers is that replay, direct get, and duplicate publish acknowledgements need to return the same committed namespace sequence without recomputing policy later. This is the main design gate for maintainers: if stored server-owned metadata is not acceptable, the feature should pivot to consumption-time decoration or consumer/group behavior instead of continuing as a write-path stream feature.

## Publish Preconditions

The branch supports a client-supplied publish command header:

- `Nats-Expected-Last-Subject-Version`

Behavior:

- Omitted: append and assign the next namespace sequence.
- Integer value: require the current namespace sequence to match.
- `no_stream`: require the namespace to have no committed messages.
- Invalid value: reject the publish.
- Mismatch: reject the publish without consuming a namespace sequence.

This command header is stripped before storage and is not canonical event metadata.

## Replication and Failure Semantics

The sequence is assigned from committed/apply order. The proof tests cover:

- replicated publish on `Replicas: 3`
- concurrent publishers writing the same namespace
- independent progress for different namespaces
- leader change
- cluster restart
- follower catch-up from snapshot
- duplicate `Nats-Msg-Id` returning original sequence metadata
- rejected writes not consuming namespace sequences
- failed atomic batches not consuming namespace sequences

## Consumer Recovery Boundary

This feature does not replace JetStream delivery semantics, publisher responsibility, consumer acks, dedupe, or recovery.

[ADR-60 consumer reset](https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-60.md#consumer-delivery-state-reset-api) remains the recovery mechanism for walking a consumer backward by stream sequence. The namespace sequence gives committed messages an unambiguous sequence line for the configured namespace; consumers still own gap detection, redelivery handling, and reset behavior.

## Cost Boundary

The feature is opt-in because each active namespace has tracked state.

High-cardinality usage increases:

- in-memory namespace entries
- file-store checkpoint size
- restart catch-up work when checkpoints are stale

Local benchmark on an Apple M4 Max using `-benchtime=10x`:

```text
BenchmarkSubjectVersioningHighCardinalityStore/Memory-14          5583 ns/op    1428 B/op  18 allocs/op
BenchmarkSubjectVersioningHighCardinalityStore/File-14           12775 ns/op   27613 B/op  19 allocs/op
BenchmarkFileStoreSubjectVersionStateCheckpoint-14              398067 ns/op  907932 B/op  32 allocs/op
```

The current branch does not add a namespace-count guardrail. That should remain a maintainer/operator decision because a fixed v1 limit could reject legitimate workloads without production data.

## Compatibility and Scope Limits

Subject namespace sequencing currently requires append-only stream semantics in practice. The branch rejects configurations that would undermine stable namespace sequences, including:

- non-limits retention
- max age
- max messages
- max bytes
- max messages per subject
- deletes or purges
- rollups
- message TTLs
- subject delete markers
- counter mode
- message scheduling
- mirrors
- sources
- republish

The mode can be enabled only on empty streams and cannot be changed once committed messages exist.

## Alternatives

### Exact-Subject Only v1

This reduces scope by using the final stored subject as the only namespace. It does not cover grouped namespaces such as multiple event-type subjects sharing one logical sequence line.

### Generic Write-Time Transform

A generic transform/mutator could produce similar metadata. It would still need to answer the same consistency, replication, checkpointing, and stored-metadata questions.

### Consumption-Time Decoration

Decorating messages on delivery avoids storing server-owned headers. It may fit better with future consumer groups, but it does not give direct get, replay, and duplicate publish acknowledgements the same committed metadata without additional design.

### Wait for Consumer Groups

Consumer groups may eventually provide named behaviors and dynamic partitioning where sequencing can be attached to delivery. Waiting reduces immediate scope but leaves publishers without a committed namespace sequence in the current stream model.

## Maintainer Decisions Needed

- Is storing server-owned metadata headers acceptable for this opt-in stream mode?
- Should this stay a stream write-path feature, become a consumer/group feature, or be modeled as a generic transform/mutator?
- What public naming should be used for config, headers, errors, and tests?
- Should grouped namespace sequencing remain in v1, or should v1 be exact-subject only?
- Should v1 include a high-cardinality namespace guardrail?
