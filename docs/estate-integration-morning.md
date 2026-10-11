# Estate integration: operator acceptance and morning use

Acceptance design, not installed behavior. Read this alongside
[quiet attention](quiet-attention-plan.md),
[canonical receipts](canonical-attention-receipts.md), and the
[estate tool contract](estate-tool-contract.md). The implementation and independent
whole-plan review gates in those documents remain in force. This document owns
operator-side acceptance and the morning handoff; it does not introduce another
receiver, scheduler, receipt ledger, or tool backend.

## The acceptance object

Return one bounded manifest for one exact candidate. Bind its source commit,
generated-export/schema digests, executable SHA-256, features, build host/toolchain,
adapter/profile versions, semantic-owner schema versions, disposable identities,
run receipts, and remaining holds. Keep credentials and private transcripts out
of the public artifact. Paths and identities needed for a private operator run
belong in its bounded execution receipt, not this reusable design.

Each required result is `passed`, `failed`, `held`, `unsupported`, or `unobserved`.
Record the evidence kind and exact candidate for every row. Unsupported capability
is not complete integration, and a result from an older executable cannot clear a
newer candidate. The manifest must preserve failed and unavailable rows instead
of publishing only successful output.

| Stage | Required evidence | Does not establish |
| --- | --- | --- |
| Source/build | Reviewed owner returns; affected exports/snapshots; designated executable and features built at the bound head | Installation or behavior of a running session |
| Disposable owner/API | Actual isolated persistence and semantic-owner effects, with exact identities and settlement | Provider ingestion or production effects |
| Model-visible composition | Final request descriptors/profile; separately admitted provider request, output, invocation and result where claimed | Comprehension, hearing, or consumption from registration alone |
| Operator handoff | Honest readiness/control display, real operator-entry canary, supported activation path and residual holds | Permission to replace or restart the running session |

Library compilation is not executable construction. CI is not the installed
artifact. Mocked descriptors are useful composition evidence, not a provider's
ingestion. A stored voice is not evidence that it was heard. Admission is not
model consumption or delivery. Merge, installation, and activation are separate.

## Disposable operator-entry matrix

Reuse receipt and tool owners' focused checks; do not duplicate their production
implementation or infer success from their unit results. Start with one minimal
candidate-executable canary and one disposable exact thread/workspace after the
whole-plan gate. Bind the caller, persistence, selected profile, and executable
before interpreting output. Declare any additional TUI fixture paths before edits.

The operator-facing matrix must cover these relations:

- First idle attention works without a seeded user turn. Failed or unavailable
  persistence is visibly held, never a fabricated empty ledger.
- Cold resume retains exact-thread receipts and retry/quarantine behavior; forks
  do not inherit an ancestor's admissions. Reuse strict-recovery owner evidence
  for damaged, incomplete, paginated, and older-than-compaction history.
- A background attention turn preserves the pending question ID, collected
  answers, and draft. Ordinary foreground input and native permission cleanup
  retain their existing behavior. Question routing follows the repository's
  selected route; conversation answers do not authorize execution.
- Tool capability, native ledger readiness, and actual delivery are independently
  visible. Unsupported tools remain unsupported; an unavailable owner does not
  justify native-question fallback or guessed identity.
- A courtesy-held publication stays held/pending until its owner settles it.
  Lost responses reconcile the same operation ID; they never blindly repost under
  a new ID. Capture owner effect separately from native admission.
- Cursor-neutral reads, advancing reads, question answers, and closure retain
  their distinct effects. Disposable cursor tests use admitted isolated bindings,
  not production checkpoint adoption or migration application.
- Operator-direct bypass, peer-direct timing, and ordinary batching preserve the
  selected policy. Capture all three timing controls, their reload/refusal state,
  captured source membership, and observed timing rather than assuming settings.
- An unchanged finished monitor or an idle no-task state does not generate an
  empty recurring wake. Suppressing the receiver, deleting watches, swallowing
  operator attention, or altering cursors is not a passing result.

Keep source publication, addressed receiver delivery, and observed operator
receipt separate. Missing coverage means unobserved or held, not no activity.
Timeout/uncertainty follows the relevant owner's safe status/reconciliation route.
Any required fixture seam outside the acceptance stream's custody returns to its
owner before editing; it does not license a parallel backend.

## Context and runtime economy

Compare the current flow and candidate against the same frozen, bounded source
history and task. Required message membership, source coordinates, horizons, and
outcomes must match. Record descriptor/request bytes, actual tokenizer or
model-reported tokens when available, wakes, reads, expansions, clipping, and
effect/admission outcomes. Characters are not measured tokens. A smaller but
incomplete read is not a saving.

Report absolute costs and differences with the measurement horizon. Include
receipt update/recovery, memory, and end-to-end costs from the production owners;
do not present a tiny lookup benchmark as system performance. Operation replay
must not create additional effects, and quarantine cannot be pruned to improve a
number. No measured savings follows merely from grouping tools into six schemas.

## Morning-use runbook boundary

The final handoff supplies exact commands only after their owner grammar,
candidate artifact, and launcher support are verified. Until then it reports the
missing capability; provisional commands must not be described as installed.

1. **Identify:** name the exact reviewed source and candidate executable/features,
   build and canary evidence, and any unresolved acceptance row.
2. **Decide activation:** preserve the running/system binary overnight. Use only
   the separately authorized installation and supported exact-session launcher
   path when activation is selected. A declared restart is not a completed one.
3. **Verify the new entry:** check actual executable/profile and registered
   identity. A configured path, shared name, or loaded bag is insufficient.
4. **Receive:** perform the repository's bounded source refresh. In the Observatory
   receiver route this is `bin/attention events peek`, then cursor-neutral
   `bin/cc now <sender>` for each exact source, and changed owned-monitor tails.
   An operator Chat wake reads `bin/desk wake 8` first. Preserve omitted-source and
   horizon evidence; do not widen repeatedly to manufacture coverage.
5. **Reconcile effects:** read the selected owner's status for held, pending, or
   uncertain operations. Do not infer publication from a zero exit, hearing from
   audio generation, or permission from a conversation answer.
6. **Stop needless wakes:** use the documented owner route when no task remains.
   If that route is unavailable, report the hold; do not repair the live launcher
   or suppress the receiver as an acceptance shortcut.

For a quick morning check, show the operator what is actually ready, all three
timing controls, and one bounded disposable addressed event while a question draft
is open. The draft must survive and the receipt must distinguish publication from
admission/delivery. This quick check supplements, not replaces, the full matrix.

## Return and falsifier

Return exact public source/PR, reviewed fixture paths, candidate executable
binding, actual canary receipts, context/control measurements, and the short
operator runbook with remaining holds. Producers own implementation and generated
bindings; the integrator owns the composed build; the independent reviewer owns
the whole-plan/source verdict. None owns activation by implication.

The acceptance claim is falsified by a passed row from another head, a library
check standing for a binary, a mock standing for provider ingestion, a scheduled
send standing for publication, an unsupported owner standing for functionality,
or empty recurring wakes standing for responsive attention. Exact identity,
source, candidate, descriptor, receipt, and observed outcome are the grounded
nodes; evidence pairs join them and stage manifests compose them. Counterexamples
revise their owning relation, not merely the wording of a favorable headline.
