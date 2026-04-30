# Contributing

Thanks for helping improve `prodjlink-rs`.

Compeller maintains this because REACT uses it, but the crate is meant to be useful on its own for DJ, lighting, and show-control tools.

## Useful contributions

- Hardware reports: model, firmware, network setup, what worked, what did not.
- Packet fixtures for tests. Please remove private names or track information before sharing.
- Metadata parser improvements.
- Examples and docs.
- Bug fixes with small focused PRs.

## Development

```bash
cargo test
cargo check --examples
cargo check --features serde
cargo clippy --all-targets --features serde -- -D warnings
```

## Safety

Do not test against a live production DJ network without operator approval. This crate sends Pro DJ Link LAN packets and may bind UDP ports 50000 and 50002.

## Style

- Keep public APIs small and documented.
- Prefer fixtures and tests for parser changes.
- Avoid adding REACT-specific behavior to the crate.
- No internet or SaaS calls in this crate.
