# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This repository ships **documentation, runnable examples, and a burn-in soak harness** for the
`kubemq-amqp-1-0` connector — there is **no published package**. Version numbers track the
state of the docs and examples in this repo, not a client library release.

## [Unreleased]

## [1.0.0] - 2026-06-17

### Added

- Initial release of the `kubemq-amqp-1-0` direct-connect documentation and examples repository.
- Connector overview: bridging AMQP 1.0 (OASIS / ISO-IEC 19464) onto KubeMQ's native Queues,
  Events, Events-Store, Commands, and Queries patterns — a standard AMQP 1.0 client points at
  KubeMQ by changing only the connection string and node address, with no code rewrite.
- `docs/`: connector reference covering architecture, getting-started, configuration, concept
  docs, per-pattern guides, the `<pattern>/<channel>` address grammar (longest-prefix matching,
  dynamic / anonymous terminus, the write-only `/responses/<RequestID>` reply path),
  capabilities, and the `amqp:*` error conditions.
- `examples/`: per-pattern runnable examples across 6 (Go, Python, Java, C#, JS/TS, Rust; no
  Ruby) — 13 variants × 6 languages = 78 example folders (75 runnable + 3 justified Java `N/A`
  cells), using standard third-party AMQP 1.0 (OASIS / ISO-IEC 19464) libraries only (no KubeMQ
  proto bindings). Each language pins one native client: Go `Azure/go-amqp`, Python
  `python-qpid-proton`, Java `qpid-jms-client` (`javax.jms`), C# `AMQPNetLite.Core`, JS/TS
  `rhea` + `rhea-promise`, Rust `fe2o3-amqp`.
- `burnin/`: standalone Go soak-test harness exercising the connector under sustained
  multi-pattern load.
- `examples/SHARED-CONVENTIONS.md`: the per-language build/lint/run loop and repo-wide
  conventions (addressing, the single connection environment variable, settlement / credit
  defaults, and the per-example README template).

[Unreleased]: https://github.com/kubemq-io/kubemq-amqp-1-0/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/kubemq-io/kubemq-amqp-1-0/releases/tag/v1.0.0
