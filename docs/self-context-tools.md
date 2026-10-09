# Model self-context tools

This build exposes two direct model tools for managing its own conversation context:

- `context_status` reports the configured model and provider, effective reasoning effort, current
  thread and turn identifiers, whether that turn is active, current context-window use, the
  distinct auto-compaction budget, and cumulative/last-request token usage.
- `compact_context` requests a normal compaction of the current turn. It records the request in
  turn-scoped state, returns successfully to the model, and performs compaction at the next safe
  mid-turn boundary before model sampling continues.

`compact_context` does not launch the standalone `/compact` task because doing so would abort the
turn that invoked the tool. A pending request belongs only to that active turn, so interruption or
turn replacement cannot leak the request into later work.

The experimental token-budget tools remain separate. In particular, `new_context` starts a fresh
window without summarizing conversation history, while `compact_context` follows the build's normal
compaction backend and lifecycle.
