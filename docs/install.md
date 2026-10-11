## Installing & building

### System requirements

| Requirement                 | Details                                                         |
| --------------------------- | --------------------------------------------------------------- |
| Operating systems           | macOS 12+, Ubuntu 20.04+/Debian 10+, or Windows 11 **via WSL2** |
| Git (optional, recommended) | 2.23+ for built-in PR helpers                                   |
| RAM                         | 4-GB minimum (8-GB recommended)                                 |

### DotSlash

The GitHub Release also contains a [DotSlash](https://dotslash-cli.com/) file for the Codex CLI named `codex`. Using a DotSlash file makes it possible to make a lightweight commit to source control to ensure all contributors use the same version of an executable, regardless of what platform they use for development.

### Build from source

```bash
# Clone the repository and navigate to the root of the Cargo workspace.
git clone https://github.com/openai/codex.git
cd codex/codex-rs

# Install the Rust toolchain, if necessary.
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
rustup component add rustfmt
rustup component add clippy
# Install helper tools used by the workspace justfile:
cargo install --locked just
# DotSlash fetches pinned development tools such as buildifier on first use.
cargo install --locked dotslash
# Install nextest for the `just test` helper.
cargo install --locked cargo-nextest

# Build Codex.
cargo build

# Launch the TUI with a sample prompt.
cargo run --bin codex -- "explain this codebase to me"

# After making changes, use the root justfile helpers (they default to codex-rs):
just fmt
just fix -p <crate-you-touched>

# Run the relevant tests (project-specific is fastest), for example:
just test -p codex-tui
# `just test` runs the test suite via nextest:
just test
# Avoid `--all-features` for routine local runs because it increases build
# time and `target/` disk usage by compiling additional feature combinations.
```

### Focused request-scenario prerequisites

Core request scenarios can launch `codex-code-mode-host` and `test_stdio_server`.
Building only the `codex-core` test target does not build those executables:

```bash
# From codex-rs, build only the helpers needed by the selected scenarios.
cargo build --locked -p codex-code-mode-host -p codex-rmcp-client \
  --bin codex-code-mode-host --bin test_stdio_server
# Match CI's test-thread stack for these scenarios; select the needed test filter.
RUST_MIN_STACK=8388608 cargo test --locked -p codex-core --test all <filter>
```

If the upstream V8 sandbox archive is unavailable, use the exact-version archive
**and** binding procedure in [the V8 consumer guide](../third_party/v8/README.md)
and [the existing CI setup action](../.github/actions/setup-rusty-v8/action.yml).
Verify the downloaded manifest against the committed trusted digest, then both
artifacts against that manifest, before setting child-process
`RUSTY_V8_ARCHIVE` and `RUSTY_V8_SRC_BINDING_PATH`. Keep both overrides for the
subsequent Cargo test command. Do not disable the sandbox or mix versions to make
the build pass. Review every regenerated snapshot: a passing snapshot update can
otherwise record a missing-helper error as its new expected output.

## Tracing / verbose logging

Codex is written in Rust, so it honors the `RUST_LOG` environment variable to configure its logging behavior.

The TUI records diagnostics in bounded local stores by default. Set `log_dir` explicitly to enable a plaintext TUI log for a run:

```bash
codex -c log_dir=./.codex-log
tail -F ./.codex-log/codex-tui.log
```

The non-interactive mode (`codex exec`) defaults to `RUST_LOG=error`, but messages are printed inline, so there is no need to monitor a separate file.

See the Rust documentation on [`RUST_LOG`](https://docs.rs/env_logger/latest/env_logger/#enabling-logging) for more information on the configuration options.
