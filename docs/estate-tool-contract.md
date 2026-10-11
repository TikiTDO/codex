# Selected estate tool adapter contract

Design for review, not an installed capability or runtime implementation approval.
This is the tool relation of the whole quiet-attention integration plan.

## Adapter and activation

Extend the existing bin-MCP versioned argv projection as
`observatory.bin-mcp.contract.v2`, profile `estate-tools.v2`. Reuse Core's existing
MCP runtime; do not add native handlers that implement estate databases,
authentication, addressing or question state. Generate the six tool schemas below
from one operation/argv/result manifest bound to the existing semantic owners.
Keep v1 contracts and their separately admitted communication grants unchanged.
The descriptor-derived command MCP and enrolled coordination MCP remain separate.

The launcher selects the owner-controlled contract and backend root, and binds the
exact workspace/registered term through the existing identity and credential
runtime. Tools cannot select those paths, credentials, public author or acting
term. A copied UUID or model-supplied actor is not admission. Estate profile
activation is explicit and optional elsewhere; missing support is a typed hold,
never a native-question, legacy-backend or guessed-identity fallback. No globally
required estate MCP is introduced.

The existing v1 projection already provides selected CC, literal/semantic Search
and Desk read/publication/comment tools. It does not provide this complete v2
surface or its effect settlement. Extending that leverage is preferable to a
parallel native backend. Configured tool availability is not candidate ingestion.

## Model-visible operations

Each input is a closed discriminated operation schema generated from the manifest:
explicit required fields and bounds, with additional properties forbidden in
every branch. There is no free-form argv, environment, endpoint, backend path,
author, actor UUID or credential argument. Peer, channel and source selectors are
data resolved by existing owners, not caller-selected authority.

Keep the existing 5,000-byte normalized-input-schema policy; do not raise it as
an unmeasured precaution. Generate and measure every schema before admission.
A design-only six-family specimen measures 147, 1,427, 4,743, 1,342, 1,187 and
1,323 UTF-8 schema bytes, respectively (10,169 total; MCP descriptors 11,511).
This specimen is not the final owner grammar, native descriptor or token count:
owner string/link/source bounds and normalization still need closure. It shows
that grouping alone does not require a 32-KB ceiling. Use concise descriptions,
closed branches and reachable shared definitions; measure the completed manifest
again. If it cannot fit intact, first simplify or split the affected family. Only
a separately reviewed measured need plus explicit margin may change this estate
server's existing `tool_input_schema_max_bytes`; other servers remain unchanged.
The default policy can remove definitions, nested structure or composition from
an oversized schema, so `tools/list` alone cannot prove the model-visible contract.
Capture both final direct and code-mode descriptors and compare operation
discriminators, required fields and closed branches with the manifest. Any
loss holds profile admission. Descriptor/request measurements decide grouping;
neither a byte ceiling nor this design-only specimen establishes token savings.
The complete [design-only specimen](fixtures/estate-tool-schema-design.json) and
its [byte receipt](fixtures/estate-tool-schema-size-measurement.json) are review
artifacts, not a launcher-selectable manifest. Counts use compact UTF-8 JSON
(`ensure_ascii=false`, separators `,` / `:`), not the pretty-printed file size.

Size is not the only schema transformation. The current native
`tools/src/json_schema/types.rs::JsonSchema` omits `minLength`, `pattern`, numeric
limits and `maxItems` even below the compaction ceiling. Keep explicit bounds in
the owner manifest/validator and in concise supported descriptions; retain their
model-visible annotations in direct and code-mode descriptors. Refs may hide
definition descriptions in code-mode type declarations, so UUID-format rules
also appear in the tool description. Do not claim unsupported JSON keywords were
preserved or that the provider enforces the grammar. Closed branches, required
fields, discriminators, definitions and bound annotations are the admission
comparison; the semantic owner independently validates every input before effects.
This uses the existing native subset, not a global parser relaxation. A missing
annotation or widened branch holds profile admission. The initial 3,518-byte
specimen lacked those annotations; its smaller size is superseded, not savings.

| tool | operations | class |
| --- | --- | --- |
| `estate_capabilities_v2` | inspect | read-only |
| `desk_read_v2` | recent, show, question, effect_status | read-only |
| `desk_act_v2` | wake, catchup, publish, comment, question, answer, close, release, cancel | write; wake/catchup are cursor effects |
| `cc_read_v2` | now, since, channels, cursor_status | read-only |
| `cc_progress_v2` | catchup, adopt | cursor write |
| `search_v2` | literal, semantic, show | read-only |

Capabilities report profile/schema/adapter versions, enabled operations and actual
owner refusal reasons. A list does not certify credentials or consumption. Six
tools are a context-economy candidate, not measured savings; measure descriptors,
requests and successful composition before retaining that grouping.

Branch inputs use the following fields; omitted optional values use the visible
owner defaults. Every effect branch additionally requires `operation_id` (UUID).

| operation | fields |
| --- | --- |
| inspect | none |
| Desk recent/wake/catchup | optional count and room |
| Desk show/question | reference (positive message ID or exact comment ID for show) |
| Desk effect_status/release/cancel | target_operation_id |
| Desk publish | text, optional room |
| Desk comment | message_id, text |
| Desk question | asked_of, text, optional room/request/link/options |
| Desk answer | message_id, exactly one of choice_key or text |
| Desk close | message_id, optional reason |
| CC now/since | channel, optional limit/width; since additionally requires since |
| CC channels | none |
| CC cursor_status/catchup | channel; catchup also requires expected_epoch and optional limit/width |
| CC adopt | channel, admitted_checkpoint_id |
| Search literal | terms, optional any/oldest/limit/width |
| Search semantic | question, optional limit/width |
| Search show | source_slug, optional window |

IDs select objects, not caller identity. Counts, widths, text/link/option bounds and
valid selector syntax derive from the selected owner's versioned grammar. Options
are structured key/label pairs projected to its repeated option argv, never argv
chosen by the caller. An admitted checkpoint ID names an owner-approved immutable
epoch/checkpoint; it cannot carry arbitrary model-supplied cursor positions.

### Desk

Recent/show/question/effect-status reads are cursor-neutral and cannot flush pending
writes incidentally. The existing generic command can flush due held posts while
reading; select a no-write machine read path under its existing owner for v2,
without changing v1 behavior. Show accepts an exact positive message or comment
ID. Question returns the existing question/answer/closure state. Effect-status
reads only the authenticated caller's operation.

Wake advances only the separate bounded wake high-water; catchup advances the
ordinary oldest-first cursor. Return captured before/after positions, exact
expandable IDs and omitted-source/coverage information. Neither proves the reader
understood the page. Counts are bounded 1..500, default 8. Recent/show leave both
cursors unchanged; clipping is explicit.

Publish uses the existing configured default voice and public author. Comment is
AI-facing and targets an exact message. No alternate voice, long-form, raw upload
or Project movement effect is introduced implicitly. Question requires asked-of
and text, with request kind decision/review/validation, an optional owner-validated
link and optional 2..6 stable key/label options. No options means a text question.
Answer selects one stable choice key or text, exclusively, and retains the owner's
intended-answerer authorization; it cannot act as the operator. Close retains the
asker's existing closure/withdrawal authorization and is not an answer. Release
and cancel name an opaque caller-owned operation ID, not a raw held-key secret or
recomposed payload.

### CC and Search

CC now/since read one explicit channel/address per structured request and preserve
the normalized view, exact source positions, epoch/horizon and expansion IDs.
Multiple reads may be composed without claiming one shared snapshot. Address or
channel ambiguity and reader/current-binding refusal remain with the registry.
Literal Search uses bounded exact terms, explicit ALL/ANY and oldest-first
choices. Semantic Search uses a question and returns its actual corpus/route
horizon. Show expands a stable returned source slug. Defaults, clipping and
unavailable coverage are visible; no stale or literal/semantic substitution.

Progress uses a normalized v2 cursor namespace under the existing CC
progress/registry owner, never implicit acceptance of legacy positions. Adopt is
a separately effect-gated explicit operation for an admitted checkpoint/epoch;
reads never create it. Catchup requires that exact reader/channel/epoch binding,
oldest-first selection and a durable per-request result/cursor transaction. Same
operation ID replays the same captured result without advancing twice; changed
payload refuses. The current normalized command deliberately does not adopt
legacy cursors and its JSON route refuses catchup. Implement/admit this owner
contract as part of the full scope; until then return unsupported with the missing
owner, not read-only success or a legacy fallback. Database application and cursor
adoption remain separate effect gates.

The implementation owner for normalized progression is the existing `bin/cc`
family and registry/database owner, not Core. Select an isolated
`bin/_families/cc/progress_v2.py` semantic module reached by the fixed machine
grammar `cc progress-v2 status|adopt|catchup --request-json JSON --json`. The
projection builds JSON from the closed tool schema; it cannot accept caller argv.
Existing read/query and registry authorization logic is reused, not reimplemented
in the MCP server. The tool stream owns this finite source deliverable and its
disposable validation; it is not deferred to the receipt stream.

### Existing cursor leverage and migration

Extend the existing `observatory_cc.cursors` owner rather than install a parallel
three-table cursor system. Its present key is reader UUID / target UUID and its
progress is one legacy bigint `last_record_id`. `_cc_registry.py::read_db_cursor`,
`write_db_cursor` and `mark_db_read` share that owner. The normalized `bin/cc`
route explicitly forces read-only operation; its JSON route refuses catchup.
Existing progression is therefore leverage, not already-working normalized
catchup. A normalized view may span writers and filtered source coordinates;
one writer may occur in several views. Reinterpreting a legacy bigint or adding
one normalized position column to the old two-UUID key cannot represent these
independent boundaries.

Selected migration: version the existing cursor relation in place. Preserve
legacy rows and the unique reader/target constraint required by existing
`ON CONFLICT (reader_session_id, target_session_id)` callers. Introduce a closed
namespace (`legacy` or `normalized-v2`) and generated cursor key: target UUID
for legacy, canonical view digest for normalized. The primary key becomes exact
reader / namespace / cursor key. Normalized rows have a null legacy target and
legacy record coordinate, an explicit stable adoption epoch, validated view/source
binding and per-writer position map/version. Legacy rows cannot contain normalized
state; normalized rows cannot pass through legacy advancement. The old callers
continue selecting nonnull exact targets and retain their existing semantics.
The existing owner migration and its complete caller/constraint closure must be
reviewed before application; source implementation is not a production migration.

One new operation journal under that same owner is justified by a different
requirement: a cursor high-water cannot reconstruct the immutable page returned
before a lost reply, or detect changed payload for a reused operation UUID.
Its key is exact reader / operation UUID; it stores digest, operation kind,
captured result and before/after positions. Commit the journal and extended cursor
in the same database transaction. It is not another cursor or admission owner.
Store immutable checkpoint-admission receipts as a distinct journal operation
kind, not a third checkpoint table. An admitted checkpoint binds exact reader,
view digest, source epochs and initial positions; an owner-issued ID selects that
receipt. A changing normalized read-health receipt is not checkpoint admission.
The model cannot create approval, choose initial positions or overwrite it.
Disposable fixtures can provision admitted checkpoints through the owner; live
provisioning requires an explicitly authorized owner path, otherwise adoption is
unsupported. No production checkpoint is created by this implementation.

Adoption epoch is stable cursor identity, distinct from each read's changing
result epoch/horizon. A changed view/source epoch needs explicit owner
reconciliation, not automatic reinterpretation of stored positions. No legacy
row is silently adopted, overwritten or counted as normalized progress. There is
one progress owner before and after migration; remove superseded draft
`cc_tool_checkpoints_v2` / `cc_tool_cursors_v2` machinery rather than retain it.

Catchup is one database-owner transaction/function: authorize the reader/view,
replay an existing identical operation or refuse a changed digest, lock the cursor,
capture the admitted normalized source-prefix snapshot, select bounded oldest-first
records, and atomically store result plus new captured positions. Concurrent calls
serialize at that cursor; an operation-ID race replays the winning immutable result.
Advance only certified complete captured source prefixes, leaving omitted writers
and frontiers unchanged. Unsealed/mismatched coverage holds progression without
disabling otherwise readable now/since. The result carries actual before/after
positions and coverage, not a claim that current intake is absent. Status and
replay inspect these owner records; a lost reply cannot advance a second time.

Adopt consumes an already admitted checkpoint through that same owner's guarded
transaction, initializes the normalized namespace in the extended cursor and
records its operation result. It
cannot create checkpoint approval from a model request. Unsupported checkpoint,
identity, schema, epoch or complete-prefix capability refuses before changing a
cursor. Implementing migrations/functions does not authorize applying them or
admitting a production checkpoint; those remain explicit later gates.

## Request, result and settlement

Write/cursor calls require a canonical UUID operation ID and immutable normalized
request. The ID is an idempotency coordinate, not authorization. Read calls may
carry a correlation ID, never actor identity.

Owner result schema `observatory.estate-tool-result/v2` contains:

- adapter/profile/contract/schema versions and selected operation;
- phase: completed, held, pending, uncertain, refused or unsupported;
- effect: read-only, cursor-write, publication, question-answer or question-close;
- operation ID, caller-binding receipt and immutable request digest for effects;
- owner data, coverage/epoch and exact expansion metadata;
- relevant before/after cursors, message/comment ID, question state, held operation
  state and scheduled-send time;
- known voice state: stored, queued, rendered or failed, never inferred hearing;
- explicit clipping/unavailable coverage and one safe reconciliation operation.

Process exit/meaning and bounded stdout/stderr remain separate diagnostic
evidence. Add a reviewed machine-output mode to semantic owners where absent;
project their schemas rather than parsing human prose into success or authority.
Settlement identifiers, phase and horizon cannot disappear behind output clipping.

The closed envelope has `schema`, `profile`, `contract`, `operation`, `phase`,
`effect`, `operation_id`, `request_digest`, `caller_binding`, `process`, `data`,
`coverage`, `settlement` and `next`. Inapplicable optional fields are null, not
invented success. `process` is `{exit_code, meaning}`; `coverage` binds owner
epoch/horizon, complete/clipped status, omitted count/frontier and expansion IDs.
`settlement` binds cursor before/after, publication ID, question state, scheduled
send time and voice state where applicable. `next` is a closed safe operation
selector, not an executable string. `data` is the operation-specific owner schema
generated from the same manifest, not an unconstrained success map. Capability
inspection includes that schema version/digest and unsupported operation reasons.

Serialize machine envelopes compactly. For a read-only operation, effect-only
ID/digest/settlement fields are null; process may be null if no subprocess occurred.
Coverage is compact but never null merely to save bytes when the read has an
epoch, clipping or completeness boundary. Do not repeat full owner data in both
human diagnostics and the structured result. Measure the native serialized result
and subsequent request before claiming this avoids duplicate model context.

The authenticated Desk owner binds publication/question/comment operation ID to
the immutable request digest, storing its result together with message/question
and voice-queue insertion in one transaction. Courtesy hold is held/pending even
if a CLI exits zero and its grace sender will run later. Preserve the grace
behavior; its owner records publication/cancel/expiry in that same operation.
Same-ID retry replays/status-reads the record, never schedules a second send;
changed payload refuses. Release/cancel update only that caller-owned held
operation with existing freshness checks. Timeout or lost reply is uncertain:
read effect-status, never create a new ID and blindly repost. No durable owner
record means unsupported, not a claimed safe retry.

Answer/closure reuse existing intended-answerer/asker guards and state/idempotency
keys, binding the v2 operation to them. Same request replays; changed answer or
closure refuses. Open, answered, withdrawn and superseded remain distinct. None
grants native execution or security permission.

Desk/CC cursor operations bind selected result and final cursor update at their
existing owner's transaction/atomic-write boundary before completed publication.
Lost/partial reply reconciles the same operation. Advance only captured membership
and frontier, never skip an earlier omitted source or add new arrivals to retry.
No cursor journal belongs in Codex model context.

Bin-MCP invokes fixed repository commands without a shell, preserving existing
credential runtime and least environment. Timeout is not safe retry for effects.
Keep default native write approval and the three specifically admitted v1
communication grants; new v2 writes do not inherit those grants. Native execution,
network and credential approvals stay on their existing routes. A Desk
conversation decision is never a substitute.

## Review and acceptance

Compose this contract with the selected lifecycle receipt contract and retain the
rest of the whole plan before independent reread and runtime implementation.
This slice includes semantic-owner machine output and effect/cursor settlement,
not merely exposing existing argv. Its principal size risks are Desk operation
binding and normalized cursor adoption; optional projections cannot hide those
unfinished builds.

Falsify closed schemas/no actor or command selection, missing capability, wrong
workspace/public-author binding, read-only no incidental grace flush, held
publication with new arrivals, lost reply/timeout, altered same-ID payload,
same-ID replay, one publication/voice queue, answered-versus-closed state,
forbidden answerer/closer, independent Desk cursors, missing/adopted/mismatched
normalized cursor epochs and no legacy fallback, cursor replay/omitted frontiers,
literal/semantic horizons and clipped-result expansion. Preserve pending native
questions and ordinary execution approvals throughout.

Read code and ask predictors before focused tests. Separate mocked descriptor
capture, real disposable semantic-owner effects and actual provider ingestion.
Measure actual request bytes/tokens and repeated-read/wake costs for the same
task/history. Current general concierge vetting is not assumed callable. No
production writes as fixtures, merge, install, live switch, migration application,
cursor adoption or new credential scope follows from this draft.
