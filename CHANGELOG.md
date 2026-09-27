# Changelog

All notable changes to the `mitm_cleanup` component will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.1.0] - 2026-09-27

### Added
- **Database Cleanup:** Extended cleanup to include `program_runs`, `packages` (delivered), and `dead_letter_queue` (resolved) to prevent indefinite growth of these tables (Issue #7).
- **Filesystem Cleanup:** Introduced generic time-based file deletion mechanism. Accepts `fs_cleanup_rules` in the JSON parameters to prune directories (e.g. for medical chunks).
- **Logging Separation:** System logs explicitly restricted to Start/Stop/Cancel events and system errors, moving detailed job deletion metrics solely to the IPC job_audit_logs.

## [v0.10.0] - 2026-09-16

### Changed
- **Architecture:** Completely rewritten in Rust. `go build` replaced with `cargo build`.
- **Performance:** Replaced standard DB drivers with `sqlx` (async Postgres).

## [v0.9.0] - 2026-09-09

### Changed
- **Performance:** Optimized database cleanup job (Issue #4). `DELETE` operations are now batched (10k limit), connection pool reduced to 3, timeout made configurable (`timeout_minutes`), and explicit `VACUUM ANALYZE` commands added for reclaimed space.

## [v0.8.3] - 2026-09-01

### Fixed

- **SQL Schema Mismatch**: Fixed an issue where the cleanup queries for `job_audit_logs`, `admin_audit_logs`, `system_logs`, and `job_status_events` incorrectly referenced `created_at` instead of `ts`. (Fixes #2)
- **Incomplete Audit Logs**: By fixing the SQL schema mismatch, the cleanup job now successfully completes all deletions and properly sends the `ipc.SendAudit` messages for all tables to the scheduler. (Fixes #1)

## [v0.8.2] - 2026-09-01

### Fixed

- **IPC SSLMode Type Fix**: Changed `SSLMode` field in JSON parsing struct from `string` to `bool` to correctly unmarshal boolean values (`true`/`false`) sent by the scheduler.

## [v0.8.1] - 2026-09-01

### Fixed

- **IPC SSLMode Fix**: Fixed an issue where `SSLMode` was not correctly parsed from the scheduler's JSON configuration and improved the `MITM_DB_SSLMODE` fallback logic to support proper PostgreSQL sslmode strings (e.g., `require`, `verify-full`).

## [v0.8.0] - 2026-08-31

### Added

- **IPC Socket as Credential Broker**: The cleanup job now fetches database credentials and the master key at runtime from the Scheduler via a Unix Domain Socket request (`get_credentials` with `RUN_ID` and `SCHEDULER_SOCKET_PATH`), instead of holding them locally.

### Changed

- **IPC Audit Logging for Cleanups**: Added missing `ipc.SendAudit` calls for every cleaned table (`job_audit_logs`, `admin_audit_logs`, `system_logs`, `job_status_events`, `transformation_errors`), reporting the deleted row count and applied retention period.

## [v0.7.0] - 2026-08-29

### Changed/Added

- **Database Performance**: Configured `pgxpool` connection limits (`MaxConns=20`, `MaxConnIdleTime=5m`, `MaxConnLifetime=1h`).
- **Reliability**: Implemented `signal.NotifyContext` for graceful shutdown handling on `SIGTERM` / `SIGINT`.
- **Observability**: Overhauled error handling for deletion queries. Errors are now consistently logged and result in a `failed` IPC event, preventing silent failures.
- **Testing**: Added `main_test.go` to ensure CLI parameters are parsed and defaulted correctly.

## [v0.6.0] - 2026-07-29

### Changed

- **Components Logging**: Refactored component version logging mechanism across all layers (Collectors, Transformation, Delivery, Scheduler) to consistently output a clean `Major.Minor.Patch` version format.

## [v0.5.0] - 2026-07-19

### Changed

- **GO version**: update to GO v1.26.5.

## [v0.4.0] - 2026-07-07

### Added

- **SSL Support**: Added support for the `MITM_DB_SSLMODE` environment variable. The cleanup job now respects this setting and applies it to the MitM PostgreSQL connection string.

## [v0.3.0] - 2026-06-30

### Changed

- **Config Restructuring**: Updated database connection setup to read and parse the JSON configuration (`MITM_DB_CONFIG_JSON`) provided by the scheduler, successfully processing the nested `"db"` object format.
- **Database Connection**: The cleanup job now prioritizes the JSON configuration over direct environment variables (`MITM_DB_HOST`, etc.). Direct variables are strictly used as a fallback.
- **Audit Logging**: Added IPC audit logging during startup to explicitly record whether the database configuration was sourced from `JSON Config (MITM_DB_CONFIG_JSON)` or `Environment Variables`.

## [v0.2.0] - 2026-06-24

### Added

- **extending logging**

## [v0.1.0] - 2026-06-21

### Added

- Initial release of the `mitm_cleanup` maintenance worker.
- Supports dynamically cleaning `target_fragments`, `raw_ingestion`, `system_logs`, `job_audit_logs`, `admin_audit_logs`, and `transformation_errors`.
- Configurable retention periods via JSON arguments in `os.Args[1]`.
- Native Unix Socket IPC telemetry for integration with `mitm_scheduler`.
