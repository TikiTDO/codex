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

## Proposed repository-owned question guidance

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
