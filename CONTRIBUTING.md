# Contributing to Pulpitum

Thanks for contributing.

## Development checks

Run these before opening a pull request:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets --all-features
RUSTDOCFLAGS='-D warnings' cargo doc --locked --all-features --no-deps
```

The real CockroachDB/MinIO/Toxiproxy scenarios are opt-in:

```sh
./docker/scripts/run-e2e.sh
```

## Change expectations

- Keep the public table and archival safety contracts explicit.
- Add regression tests for behavioral or concurrency changes.
- Do not weaken a fail-closed validation merely to support a legacy or ambiguous input.
- Do not add credentials, certificate material, or production datasets to the repository.
- Discuss schema, archive-format, and compatibility changes before implementation.

## Issues and pull requests

Use issues for reproducible bugs and scoped proposals. For security-sensitive reports, follow [`SECURITY.md`](SECURITY.md) instead of filing a public issue.

By contributing, you agree that your contribution is licensed under the repository's Apache-2.0 license.
