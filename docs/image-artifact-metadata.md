# Estate image artifact metadata

The built-in image-generation tool accepts optional `title` and `metadata`. A missing title uses
`generated image`, never a prompt excerpt. Titles are trimmed, nonempty and at most 120 characters.
Only explicitly selected `thoughts`, `text`, `commissioner_notes`, and `pinned_comments` are admitted;
unknown fields are refused. Each text field/list entry is limited to 8,000 characters, lists to
16 entries, and selected metadata JSON to 32 KiB. Do not select raw conversation, hidden reasoning,
secrets or incidental reference paths for these fields.

The saved PNG includes UTF-8 text and escaped XMP metadata without re-encoding image pixels. The
selected image model and local timestamp are recorded alongside that selected material. The
model-facing image output and upstream attachment-store/input-reference identities are unchanged.

`image_generation_output_dir` configures the app-server's artifact root, defaulting to
`$CODEX_HOME/generated_images`. Files use a local-date folder and timestamp/title/call filename.
When using the executor's working directory rather than a configured local root, existing files
and symlinked output/date directories are refused; the executor byte limit also applies after
metadata is embedded. Saving failure does not discard the generated model-facing image.
