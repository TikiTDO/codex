# Quiet attention and repository-owned questions

Draft plan. The native attention candidate is based on the existing custom layer,
not the fork's older default branch. It is not installed or activated.

Quiet attention admits bounded source coordinates into an exact thread as internal
context, never user input. The native server owns admission and persists uncertain
intent before inference; exact-source retries cannot evade quarantine by changing
the batch key. A trusted background-turn marker precedes task execution. The TUI
uses that classification to preserve pending question identifiers, answers and
drafts through background terminal events, without changing ordinary input or
permission-approval cleanup. Admission is not proof of model consumption.

## Reviewed repository-owned question guidance

Question routing is independent of question preservation. Fixing background
attention must not silently undo a repository's chosen external question flow.
Append one shared paragraph to both final, model-visible question-tool descriptions:

> Repository question routing: When the active repository provides Desk and directs
> operator questions there, use its Desk question, choice, review and validation
> features instead of request_user_input or request_user_input_async. Follow the
> repository's documented Desk commands and answer/closure workflow. If the required
> route is unavailable, report the hold; do not silently fall back to the native
> question tool. Conversation questions are not native execution, permission or
> security approval mechanisms, which remain on their existing routes. Outside
> such a repository, use this tool normally.

Put the public-safe paragraph in `codex-prompts`. The synchronous final consumer is
`core/src/tools/handlers/request_user_input_spec.rs`; the asynchronous final consumer
is `core/src/tools/handlers/request_user_input_async.rs::spec`. Appending only to the
async fallback prompt would miss catalog-supplied descriptions. Preserve supplied
descriptions and schemas, tool availability, collaboration modes and native
permission/security approval behavior. This is advisory routing guidance, not a
new enforcement mechanism or proof that a provider loaded it.

Before implementing that description change, review this plan. Focused fixtures
must check both final descriptions, distinctive async catalog descriptions and
custom parameters, unchanged default schemas and mode availability. A disposable
model-visible tool-list check is separately required before claiming ingestion.

The shared paragraph is now appended at both final consumers. Production library
compilation passed; the new fixtures, affected context snapshots and ingestion
check remain unfinished. This does not establish quiet-attention API acceptance.

## Expanded integration design — before further runtime changes

The requested scope includes lifecycle-owned attention and receipt recovery,
native repository Desk, CC and Search tools, measured context economy, and
operator-visible control. This is a whole product relation, not permission to
replace the running executable or relax native execution approvals. Lifecycle
and tool integration can have coherent review boundaries without dropping either.

### 1. Resolve receipts at their lifecycle owner

The first native API fixture compiled and failed before admission: a newly created
thread advertises its lazily materialized rollout path, but receipt recovery tries
to read that nonexistent file. Starting a fake user turn or treating every missing
history as empty would hide the defect.

Review moving receipt custody to one exact-thread lifecycle owner. Creation can
prove New/Cleared prior receipt state; resume must establish complete canonical
receipt history under writer ownership. Forks must not inherit a parent's
admissions. Client-visible startup replay is not automatically complete receipt
history, especially with paginated or compacted model context. Recover latest
per-batch notices from the complete canonical source, validate exact-thread
identity, and refuse missing, malformed or incomplete recovery. Initialize before
publishing a usable runtime, and update uncertain/final receipt state through the
same owner as durable writes. Do not add a second app-server source of truth.

Core retains scheduling and interactive-request admission. Preserve durable
uncertain intent before inference, same-key retry, cross-key source quarantine,
background provenance before execution, and ordinary permission cleanup. A list
or TUI projection is a view of the same receipt owner, not another admission
decision. Admission remains distinct from consumption.

The present `attention_snapshot` cannot supply that contract by calling ordinary
`load_history(false)`: full-history reads reject paginated threads, and the legacy
history helper discards the rollout decoder's parse-error count. A skipped damaged
receipt could therefore look like an absent admission. Startup model context also
intentionally stops at a qualifying compaction; it is not the recovery ledger.
The reviewed recovery adapter must use the existing canonical lineage and writer
ownership, validate the requested thread, and refuse any decode error or unknown
coverage. Keep tolerant ordinary model-history behavior unchanged. Falsify recovery
with a damaged receipt between otherwise valid records, a receipt older than the
latest compaction, paginated cold resume, and a fork with ancestor admissions.

### 2. Integrate estate tools without replacing estate authority

Expose first-class, typed Desk, CC and Search calls inside the configured estate
profile. The backend must reuse the existing semantic and credential/effect
owners; do not inline their databases, addressing rules or authentication into
Codex. Existing command descriptors and MCP projections are candidate leverage,
not evidence that every needed operation is already available. The existing
read-only command MCP covers selected CC and literal Search paths; it does not
currently provide Desk, semantic Search or cursor/effecting operations. The
enrolled coordination MCP is a separate contract and cannot substitute silently.

Bind one reviewed adapter and capability version. Missing capability produces a
typed hold, not shell interpolation, guessed identity, wider credentials or
native-question fallback. Repository activation must be explicit; unrelated
repositories retain ordinary Codex behavior. Do not make an unconfigured estate
transport a globally required launch dependency.

Desk scope includes bounded reads, exact questions with options, review and
validation requests, answers, closure and human-readable publication. Preserve
public author versus exact-term cursor identity, room targeting, courtesy holds,
voice/publication receipts and answered-versus-closed states. CC must distinguish
cursor-neutral now from cursor-advancing catch-up. Search must preserve literal
versus semantic meaning and the returned corpus horizon. Cursor movement,
publication and question closure are effects, not generic read-only tools.
Conversation decisions must never authorize native execution or security effects.

### 3. Complete source routing and operator controls

The adapter's journal and native transport are independently reviewed helpers,
not a finished live producer or settlement loop. Directness comes from the actual
source's recipient, attention or body. A comment parent supplies room membership,
not directness. Operator bypass requires verified provenance. Extract and reuse
the existing exact addressing predicate; preserve the old route's behavior.
Applying a new source-owner database function remains a separate effect gate.

Bind every selected partition to its captured event coordinates and frontiers,
publish stable intent before submission, and settle only exact accepted membership.
New arrivals cannot change a retry's key or membership; omitted earlier sources
limit frontier advancement. Unsupported, uncertain or incomplete state holds.
Expose the three required timing controls and their live reload/refusal state.
Operator-direct bypass, peer-direct timing and ordinary batching remain separate
from Core admission. Native question preservation must survive background turns
without changing foreground input behavior.

### 4. Measure context and runtime economy

An initial static candidate is whole-receipt-map cloning and cumulative source
scanning on every admission. A source-membership index may help, but must be a
derived view of the single receipt owner, with exact restart/failure semantics.
Do not prune quarantine to obtain favorable numbers. Measure update, recovery,
memory and end-to-end costs, not only a tiny lookup benchmark.

For context economy, capture actual model-request bytes/tokens and wake counts
for the same task and source history. Compare bounded exact-source reads,
descriptor-selected tools and targeted expansion against the current flow;
retain complete failure/horizon evidence. A favorable predictor verdict proposes
a falsifier; it does not establish savings. Concierge delegation uses only a
currently callable native-engine route; shadow/static recipes are not arbitrary
code vetting or a working CLI supervisor.

### Review and acceptance boundary

Before implementation, close complete canonical recovery and the typed tool
adapter/effect contract in review. The associative tree is: exact thread, source,
credential provenance, receipt, question and cursor are rank-zero evidence nodes;
lifecycle state and routing cause are derived relations; scheduling, timing and
tool exposure compose those relations without creating authority. Forward
execution captures sources, persists intent, admits internally and settles exact
receipts. Counterexamples and measured costs revise the plan; no online learning
or third-party model runner is introduced.

Focused falsifiers must cover first idle admission without UserInput, cold resume
and fork isolation, missing/corrupt history refusal, uncertain intent and lost
responses, cross-key source retry, partial acknowledgements and omitted frontiers,
pending-question preservation, and unchanged execution approvals. Tool checks
must capture the actual model-visible descriptors and call the real selected
estate owners in a disposable context, including unavailable capability, wrong
identity, cursor effects, publication holds and repeated question/answer closure.
Regenerate and review only the affected context snapshots. Separate mock request
composition, real owner effects and actual provider ingestion in every claim.

Build focused targets on the designated build host, then use a disposable canary
with the candidate executable and selected features. Installation, merge and live
activation remain distinct gates. An announcement is not activation permission.

## Remaining acceptance boundaries

The current candidate's focused protocol/Core/TUI checks do not establish native API
replay, adapter crash settlement, generated binding consistency or live behavior.
The external adapter must use versioned capability negotiation, private stable
intent/settlement journals and credential-derived routing causes. Accepted sources
settle their exact captured frontiers; uncertain, unsupported and invalid state
holds. Direct operator addressing may bypass timing only through the credential
owner, never an actor label. Source/policy changes, timing controls, generated
exports and a disposable end-to-end canary remain unfinished. Installation and live
activation require separate authorization after those boundaries close.
