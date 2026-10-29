# Estate self-context tools

`context_status` is an observational model tool available directly, including in code-mode-only
sessions. It reports the invoking sampling request's pinned model and reasoning effort, the
thread-bound provider, thread/turn IDs, active matching turn kind, full context-window usage,
separate auto-compaction scope/limit, and cumulative/last model token usage. Unknown limits remain
null; remaining counts saturate at zero. It does not compact, replace context, or change session
settings. Live model updates do not relabel a previously captured request's settings.

The estate's context-management tools are ported separately as a lifecycle group; observational
status alone does not establish deferred compaction, caller guidance, image-discard or retry support.
