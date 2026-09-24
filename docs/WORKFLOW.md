# Engineering workflow

Use this lifecycle for Relay work:

```text
SPEC
→ INVARIANTS
→ MILESTONE
→ SLICE
→ PLAN
→ BUILD
→ AUTOMATED VALIDATION
→ INDEPENDENT REVIEW
→ MANUAL / FAILURE TEST
→ COMMIT
→ UPDATE PROJECT STATE
```

[spec.md](spec.md) is the authority for engine behavior. [invariants.md](invariants.md) records claims that must hold. [test-matrix.md](test-matrix.md) maps failure scenarios to tests. [../PROJECT_STATE.md](../PROJECT_STATE.md) records what is actually done.

## Rules

1. Define one coherent goal before coding. State scope and non-scope.
2. Identify the invariants and test-matrix rows the change can affect.
3. Use Plan mode before complex implementation.
4. Parallelize only work that is genuinely independent.
5. The agent that implements a change is not its only reviewer.
6. Run the relevant tests, typecheck, lint, and build before claiming completion. Report only commands that actually ran and passed.
7. Deliberately test the failure behavior the change can break.
8. Meaningful architectural changes require an ADR in [adr/](adr/).
9. Update `PROJECT_STATE.md` after meaningful work, and only for work that is finished.
10. Debug in this order: reproduce, evidence, root cause, fix, regression test.
11. AI may implement and review. It does not silently change architecture or scope.

## Slice contents

Before implementation, name:

- the goal;
- what is in scope and what is not;
- the invariants and failure scenarios involved;
- how the relevant failure will be checked.

Do not start a later roadmap phase while the current correctness gate is open. Phase scope and exit evidence are in [spec.md](spec.md) §13.

## Records named by the specification

Specification §15 also names `docs/project-map.md`, `docs/status.md`, `CHANGELOG.md`, and `AGENTS.md`, to be updated when their triggers apply. Those files are not part of this bootstrap. Until a real change introduces them, session status lives in `PROJECT_STATE.md`.
