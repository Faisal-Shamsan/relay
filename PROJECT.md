# Relay

Relay is an open-source, self-hostable TypeScript durable execution engine.

Its purpose is to implement and demonstrate the mechanics behind durable execution:

- deterministic replay
- step memoization
- queues
- leases
- fencing
- workflow versioning
- failure recovery
- resumable streaming

Correctness under failure matters more than feature count. Relay is an engineering artifact: it exists to show how these mechanics behave, including their limits. It is not an attempt to replace mature production systems.

Guarantees are demonstrated through tests and deliberate failure injection.

Project scope, phase gates, and non-goals are defined by [docs/spec.md](docs/spec.md). That specification is authoritative.
