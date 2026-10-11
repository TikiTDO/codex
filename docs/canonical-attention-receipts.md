# Canonical attention receipts

Design candidate for the lifecycle section of `quiet-attention-plan.md`. This document does not approve runtime implementation or activation. Attention readiness must be independent of ordinary foreground and model-history readiness.

## Ownership and API

Core owns one `AttentionLedger` in `SessionServices`, shared by every handle to the exact session. `CodexThread::attention_snapshot` and `CodexThread::submit_attention` delegate to that owner. The app-server validates transport identity/capability and projects results; its thread-state map must not remain a second admission cache. Existing Core scheduling remains the only turn scheduler.

Persistence stays with `LiveThread` and `ThreadStore`. Add `ThreadStore::load_canonical_attention_receipts(thread_id)` and a `LiveThread` delegate. Its successful result binds the requested logical identity, ordered receipt events and complete coverage of each selected canonical segment: physical rollout identity, start ordinal, exclusive end ordinal and byte frontier. Storage supplies completeness; an empty vector does not prove it. Coverage and paths remain internal rather than entering model context.

The trait default returns `Unsupported` for this operation. A nonlocal store must implement the same identity, completeness and barrier guarantees before its attention capability becomes usable. Do not downcast to local storage and substitute tolerant model history.

## Strict local capture

Reuse the sole `LocalThreadStore` rollout lineage resolver. Extend its segment descriptor with the logical owner obtained from canonical session metadata. Physical rollout IDs continue to identify immutable history positions; lifecycle/writer coordination uses logical owner IDs. The requested logical target is included in the coordination set even if its current physical rollout ID differs.

Capture a finite prefix through these steps:

1. Resolve the existing lineage. Acquire shared lifecycle leases for the deduplicated logical owners in the same string-ID order used by archive operations, without holding writer mutexes. Re-resolve under those leases. If membership or bounds changed, release and retry once; another change refuses recovery.
2. Acquire required writer mutexes in that same order, after all lifecycle leases. Revalidate using the existing non-reacquiring lineage path. Flush the target live recorder through an under-guard primitive and capture each selected prefix through decoder-owned representation handles and finite durable frontiers. Never call a public helper that reacquires an already-held writer mutex.
3. Release writer mutexes before scanning the captured prefixes; retain lifecycle leases through validation. Later append-only suffixes cannot enlarge the result. Archive/delete/revert are excluded by the lifecycle leases. Representation replacement must preserve the captured handle/prefix or return unavailable; never substitute a replacement path after capture.
4. Validate every record inside the selected prefixes with the existing decoder, preserving identity and parse-error diagnostics. Wrong identity, missing metadata, malformed or truncated in-prefix data, unsupported representation and incomplete byte/ordinal coverage refuse recovery. Errors beyond an inherited cutoff are outside that snapshot.
5. Publish ordered receipt events only when every selected segment is complete. The Core reducer excludes ancestor notices whose logical thread identity differs from the requested identity, and rejects contradictory bindings for an exact-thread batch.

This read uses the existing representation/decoder and lineage owners. It does not introduce another history-base walker. SQLite pages, summaries and model-context compaction suffixes are not completeness authorities. Ordinary tolerant model-history consumers remain unchanged.

## Initialization and unavailable recovery

The ledger states are `Initializing`, `Ready` and `Unavailable`, with a separate closing flag. Ordinary foreground startup may remain usable when attention is unavailable. Attention admission and listing then return a structured RPC error; they must not return an empty success or pretend an unavailable whole ledger is a batch-local uncertain admission.

Known-empty state requires successful acquisition and committed ownership of a genuinely new exact persistence lifecycle, plus support for the strict receipt contract. A New/Cleared/Forked enum, absent file, missing SQLite row or canceled acquisition is insufficient. Same-thread model-context clearing retains receipts. Resume strictly reconstructs after acquisition returns and before attention capability becomes ready; it must not acquire lifecycle leases from inside the acquisition writer guard.

A new child has its own logical receipt identity. It may inherit model context and bounded canonical history without inheriting ancestor admissions. Unsupported stores or absent persistence leave attention unavailable, including on new sessions. Foreground history tolerance is not strengthened as a side effect.

## Admission transaction

One Core ledger mutex serializes same-thread receipt transitions, overlap checks and snapshot publication. The lock order is ledger transition, then storage lifecycle, then storage writer. Existing scheduler locks are acquired after storage locks are released. No scheduler/persistence callback may acquire the ledger through the inverse order.

Keep the accepted origin-validation and source-overlap semantics. An unchanged batch returns an existing Accepted or uncertain outcome without scheduling again. Changed origins refuse. Accepted/uncertain sources remain quarantined across alternative keys. A retryable Held outcome can retry only its original immutable batch.

The existing Core submission loop owns the complete finite transaction through a typed internal operation, alongside its existing turn-input and shutdown operations. After validation and durable intent, its handler invokes existing running admission or calls the existing idle turn-input handler directly. It never queues another operation on its own loop and waits for that reply: doing so while shutdown waits for settlement could deadlock.

Canceling before enqueue owns no transaction and schedules nothing. After enqueue, canceling or disconnecting the RPC waiter cancels observation, not dispatch or settlement. A transaction processed before shutdown finishes settlement before the loop processes shutdown. Explicit shutdown, channel-close cleanup and successful root suspension close new admission and release persistence only after the current handler finishes. Requests left behind a terminal boundary that never reached intent are provably unadmitted; return stopped/draining rather than inventing an uncertain receipt. A failed suspension keeps the ledger usable. A process restart reconstructs durable intent. This uses the existing scheduler and loop, with no recurring worker.

| Boundary | Required behavior |
| --- | --- |
| Validation | Check binding and overlap under the ledger mutex; rejection schedules nothing. |
| Intent | Append and flush uncertain intent before Core can admit input. An unknown write outcome makes the ledger unavailable, retains quarantine and requires strict recovery. |
| Admission | After confirmed intent, invoke the existing running/idle Core admission once, without holding storage locks. |
| Settlement | Append and flush the definite Accepted or retryable Held result before publishing it or notifying clients. |
| Lost outcome | Scheduler uncertainty or failed final persistence retains quarantine. Loss of a reply never implies rejection or permits a new-key retry. |
| Panic/cancellation | The transaction guard must not leave Ready state after an unobserved possible admission; mark unavailable/uncertain and require recovery. |

A durable Accepted result proves admission, not model consumption or source delivery. A missed notification does not make an admitted input repeatable.

## Structure and validation

The grounded nodes are exact identity, acquired persistence, immutable source binding, durable intent, Core admission and canonical frontier. A batch transaction associates these nodes; transactions compose into the exact-thread ledger. RPC/TUI projections expose that state without acquiring execution authority. Forward execution is validation, durable intent, existing scheduler, durable final, publication. A failed identity, coverage or transition check returns the correction to its storage/transaction owner. This slice has no learned component or gradient path.

Focused falsifiers must cover:

- Genuine new identity versus failed/canceled acquisition; same-thread context clearing preserves receipts.
- Cold legacy and paginated resume, receipts older than compaction, compressed representation, SQLite lag and unsupported stores.
- Damaged middle records, wrong identity, incomplete prefix and missing ancestors refuse attention while ordinary foreground remains usable.
- Fork isolation and immutable ancestor cutoffs.
- Concurrent same-key/overlapping-source requests admit at most once; ordinary foreground still uses its existing scheduler.
- RPC cancellation before intent, after intent, after admission and before reply; session shutdown and process restart retain settlement or uncertainty.
- Attention/shutdown ordering in both directions, successful suspension and channel closure: no self-wait, no writer use after release, and no uncertain receipt for a request that never reached intent.
- Intent/final persistence failure, transaction panic and missed notifications do not permit re-admission.
- Bounded membership recapture and lifecycle-before-writer ordering, including different logical/physical IDs and concurrent archive/revert/representation replacement.
- First idle admission without seeded user input and listing/retry after reconnect.

These checks complement the whole plan's tool-adapter, pending-question, TUI, export, provider and disposable canary acceptance. Focused unit results do not substitute for API persistence, model ingestion or activation evidence. The strict representation-handle contract and transaction lifetime must be implemented explicitly; compiler feedback cannot weaken them.

## Lifetime receipts and context replacement — reopened relation

Current model-context lineage is not lifetime receipt coverage. An unloaded same-thread revert can exclude an admitted source while keeping the same logical ID; legacy rollback and bounded-subagent migration can also filter receipt events. Strictly parsing the resulting selected model prefix cannot establish an empty receipt ledger. This relation requires independent plan review before the affected implementation can be integrated.

The selected correction keeps receipt state in canonical rollout events. Before a same-ID replacement selects a shorter model prefix, the existing strict capture and reducer obtain the full exact-thread state, including retryable Held bindings. The replacement writes one current notice per batch independently of its retained model prefix, flushes and closes that staged recorder, then performs the existing pointer CAS. A failed CAS leaves the original authoritative and removes only the unpublished replacement. Repeated revert carries the preserved state again, including revert wholly into parent context. A child with another logical ID remains isolated.

The capture owner supplies a closed-writer, caller-held-guard path as well as its live-session path. Replacement resolves its logical-owner set first, acquires sorted lifecycle guards (exclusive target and shared ancestors), then sorted writers, and revalidates membership with at most one recapture. It cannot call a public method that reacquires its writer lock. The sole lineage walker, retained representation handles, strict decoder and finite frontiers remain the owners.

Canonical migration preserves attention events independently of rollback instruction boundaries. Bounded-subagent migration also preserves the exact target's full receipt state alongside selected model context. Both validate the pinned source and preserved bindings before publishing the staged replacement. They reuse strict receipt capture; they do not add a receipt sidecar, a second lineage resolver or SQL admission truth.

An older writer's replacement is not assumed complete. Existing rollout ownership/index and retained representations may permit bounded repair; their actual completeness must be proved. If that closure is unavailable, attention returns structured unavailable while ordinary foreground remains usable. A migration progress journal or valid current file is not evidence of retained lifetime receipt contents. This remains an owned repair-or-refuse requirement, not a deletion of legacy support.

Additional falsifiers: Accepted and uncertain receipts followed by unload, revert before their turn, cold resume and same-/cross-key retry; repeated revert; revert into parent history; rollback then migration; bounded-subagent migration; staged write failure and failed CAS; older replacements with unprovable coverage refuse attention. No blanket revert prohibition or live activation follows from this correction.

The selected historic discriminator is an internal receipt-lifetime contract version in canonical SessionMeta and recorder creation. New exact lifecycles carry version 1; same-ID replacement preserves it only after durable full receipt preservation, and migration sets it only after strict full-source capture. Unknown versions refuse. Unmarked paginated historical output cannot become Ready from a valid current file alone. The existing migration journal is an empty progress marker, and the ordinary index skips bad metadata; neither establishes historical completeness. Append-only legacy source may be read in full under its format contract before conversion. The version is storage evidence, not another admission owner or an authentication credential. This deterministic boundary remains part of the reopened plan review.


### Reduced-state copy order

A retryable Held batch A can retain its immutable binding while batch B subsequently quarantines the same source. The resulting latest-state map is valid, but arbitrary batch-ID/map replay can put B before A and fail chronological validation. A display-sorted snapshot is therefore not a preservation sequence.

The selected preservation order writes nonquarantining retryable Held states first, then Accepted/AdmissionUncertain states, with batch ID only a deterministic tie-breaker within each group. The shared chronological reducer skips an exactly identical previously observed notice before cross-key quarantine checks. Immutable binding checks, Accepted outcome consistency and refusal of two quarantining keys on one source remain. Runtime validation prevents A from writing a changed Held after B quarantines their source, so a retained prefix containing B already includes the unchanged latest A; copying it again is an identical observation. A prefix containing neither exact-target notice restores Held before quarantine. This uses existing canonical events without a second owner or ordinal schema.

Additional falsifiers cover lexical B-before-A, arbitrary map insertion, Accepted and uncertain B, retained prefixes with both or neither target notices, repeated replacement, and contradictory quarantine still refused. This ordering refinement is part of the pending lifetime design review; it has not been implemented or tested.


## Reviewed first slice

The first slice admits attention only when the newest exact-target SessionMeta carries receipt-lifetime contract version1. Genuine new logical lifecycles explicitly write it; a generic recorder constructor defaults to unproved. Unknown or missing versions report structured attention unavailable while foreground remains tolerant. Parent lifecycle versions do not become a new child's admission identity.

Same-ID revert and legacy migration deliberately omit that marker in this slice. They keep ordinary foreground operations usable and prevent recovery from claiming empty receipt state after selecting a shorter model context. Full guarded preservation, historic repair and transition-safe reduced-copy ordering above remain the separately held next slice; the first slice does not satisfy those requirements. Source review and focused falsifiers remain required before integration or activation.
