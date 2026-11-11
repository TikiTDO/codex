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

This change addresses the model-facing representation. It does not by itself add hand-armed
watchers, change monitor configuration, or replace the external adapter. Those lifecycle and
cutover steps, plus real-provider Guest validation of active/idle delivery and task progress under
notification traffic, remain required before calling the complete wake-monitor repair done.
