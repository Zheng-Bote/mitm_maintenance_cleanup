# Fix: Build With Rust 1.100 – sqlx 0.8 (Issue #10)

Issue: https://github.com/Zheng-Bote/mitm_maintenance_cleanup/issues/10

## Problem

`./build.sh` fails with the current stable toolchain (Rust 1.100.0, 2026-11-09):

```
error[E0277]: the trait bound `!: sqlx_core::io::Decode<'_>` is not satisfied
  --> sqlx-postgres-0.7.4/src/connection/executor.rs:68:10
68 |         .recv_expect(MessageFormat::ParseComplete)
```

`sqlx-postgres` 0.7.x relies on the never-type fallback that earlier Rust versions only warned about ("contains code that will be rejected by a future version of Rust"); Rust 1.100 rejects it. The repository has no `rust-toolchain.toml`, so it builds with the default (newest) toolchain.

## Fix

Upgrade `sqlx` from `0.7` to `0.8` (0.8.6). No source change is needed; verified:

- Rust 1.99.0 + sqlx 0.7.4: builds (with the future-incompat warning).
- Rust 1.100.0 + sqlx 0.8.6: builds without warnings (`cargo build --release --target x86_64-unknown-linux-musl`).

## Requirements (EARS Syntax)

1. The component shall build with the current stable Rust toolchain (1.100.0) for `x86_64-unknown-linux-musl`.
2. The component shall use `sqlx` 0.8 with the same features (`runtime-tokio-rustls`, `postgres`).
3. The runtime behaviour (cleanup rules, IPC, `system_logs`) shall be unchanged.

## Scope

`Cargo.toml`, `Cargo.lock`, `CHANGELOG.md`, version 1.2.1.

Not in scope: `core-layer` pins `sqlx =0.7.3` (required by `sqlx-adapter 1.2.0`) and is protected by `rust-toolchain.toml` 1.99.0 — it will hit the same error when the toolchain pin is raised (separate issue). `mitm_transformation_main` already uses sqlx 0.8.

## SpecDD Architecture Alignment (Drift Control)

- [x] **Architecture:** The layered architecture is maintained.
- [ ] **Architecture:** Feature affects architecture: SpecKit feature forces update of the SpecDD .sdd — no.
- [x] **Security:** Envelope Encryption (AES-GCM) is NOT bypassed for PII data.
- [x] **Data Model:** Core PostgreSQL schemas remain intact.
- [x] **Standards:** SPDX headers and English documentation will be maintained.

## Acceptance Criteria

- [x] `./build.sh` succeeds with Rust 1.100.0 (`cargo build --release --target x86_64-unknown-linux-musl`, no warnings).
- [ ] A cleanup run against the dev DB works as before.
- [x] CHANGELOG.md is up-to-date.
