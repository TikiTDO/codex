# Native attention as standalone tool evidence

`thread/attention` accepts authenticated source coordinates, not source prose. Core constructs a
standalone `function_call_output` with `name: "attention"`, `namespace: "observatory"`, and no
`call_id`. Standalone host tool notifications are an existing protocol form; this does not invent a
model tool call or send another reply to a tool call that already completed. The output is JSON
containing the exact `thread_id`, `batch_id`, and `origins` that bind the durable receipt.

No user/developer message is constructed for attention. The generic internal-context fragment is
unchanged from upstream, so this transport does not change the instruction role of goal steering or
daemon recovery. Core rejects a request whose supplied response item differs from its receipt's
notification; a caller cannot substitute an instruction message or another batch's result.

Scheduling and custody remain with the existing native attention owner: active regular turns buffer
at their next ordinary consumption boundary without a steer signal; an idle thread starts a
background turn through the existing admission gate. Outstanding blocking questions, shutdown,
Plan-mode restrictions, terminal receipt settlement, deduplication, and cold-resume provenance retain
their existing owners. A receipt is admission evidence, not proof of a source read or action.

## Hand-armed command watchers

On the interactive `exec_command` tool, `notify_on_exit: true` opts a yielded command into one
completion notification. For example, a hand can launch a bounded Desk wait with
`{"cmd":"<bounded watcher command>","yield_time_ms":250,"notify_on_exit":true}` and continue
working. Ordinary exec calls default to false. Inline completions return normally without a
second notification. One-shot exec does not support this argument.

The existing process manager still owns execution, sandboxing, approvals, output collection and
the durable command-completed event. Only after that event is emitted and flushed does the
subscription submit a standalone `observatory.exec_command_completion` output. Its data binds the
thread, originating task, originating call and terminal session; the actual `call_id` is absent,
so this is not a second reply to the original call. Output is bounded to 4096 characters; full
retained execution output remains with the command event. Command output never selects tool
arguments or gets promoted to instructions.

Delivery uses the existing quiet active/idle admission path. During a blocking question or the
finishing-to-idle seam the subscription waits locally, without model polling, steering or taking
the question's callback. It is consumed once on admission. An uncertain admission is not blindly
retried. This is admission evidence, not evidence that the model read or acted on the result.

Subscriptions survive ordinary turn completion so they can wake an idle hand. Interruption,
task replacement, context-compaction installation and shutdown cancel them and
terminate only their opted-in watcher commands; ordinary terminals retain their existing
lifecycle. A generation check prevents a command launched before cancellation from arming late.
The hand must explicitly re-arm after those boundaries. There is no automatic cold-resume replay
of shell processes or pending subscriptions.

Codex does not infer a quiet timeout from arbitrary stdout or exit codes. Filtering and quiet
retry belong to the hand's bounded watcher command, which can wait again without waking the model.
When that command actually finishes, its outcome is delivered, including errors. Monitor selection,
the external adapter cutover, and real-provider Guest validation remain separate checks before
calling the complete wake-monitor repair done.
