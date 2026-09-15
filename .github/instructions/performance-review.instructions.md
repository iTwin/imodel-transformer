---
applyTo: "{packages/performance-tests/**/*.{js,ts,tsx,md,yml,yaml,json},.github/workflows/quick-performance*.yml}"
---

# Performance Review Rules

Use `packages/performance-tests/AGENTS.md`, `README.md`, and `ARCHITECTURE.md` as contract sources for quick and weekly performance harnesses, benchmark scenarios, fixtures, workflows, and documentation.

## Measurement validity

- Keep fixture creation, dependency resolution, compilation, setup, correctness checks, cleanup, and report writing outside the timed operation.
- Measure the intended transformer operation and keep functional correctness and semantic-digest validation as separate results.
- Use isolated baseline and candidate processes with bounded lifetime, explicit cleanup, and no writable database or mutable artifact reuse; an immutable shared fixture is valid when its identity is recorded and neither arm mutates it.
- Record fixture identity and transformer-build provenance independently so every A/B result is reproducible.
- Interpret wall time and peak RSS independently. The five-percent wall-time threshold is informational, not a statistical-significance test or merge gate.

## Benchmark and query correctness

- Use a fixture with enough scale and shape to expose the claimed cost, and verify that the benchmark measures the claimed operation.
- For set-based or bulk-query changes, inspect query cardinality, bind parameters, duplicate handling, ordering, empty inputs, and semantic equivalence with the prior path.
- Treat an N+1 reduction as complete only when remaining per-entity or per-relationship queries have a documented reason.
- Preserve transformation output, provenance, relationships, aspects, exclusions, and error behavior across performance optimizations.
- Keep benchmark registration explicit and use the existing catalog, scenario, fixture, and runner boundaries.

## Harness integrity

- Preserve deterministic fixture identity, immutable shared artifacts, semantic digest equality across comparison arms, per-worker timeouts, forced termination, and retained failure artifacts.
- Keep quick, comparison, integration, and credential-dependent weekly suites distinct; the quick harness remains credential-free.
- Validate workflow changes against the actual command and compiled-package topology documented by the package.
- Run `pnpm check` when a change file, package metadata, or release-facing benchmark contract changes.
- Add unit tests for resolution, identity, report, lifecycle, and failure paths when those components change.

```ts
await scenario.prepare();
const result = await measure(() => transformer.processChanges());
await scenario.finish(result);
```

## Maintainability

- Prefer benchmark abstractions that simplify registration, measurement, or validation.
- Place special-case scenario behavior in a fixture recipe, provider, catalog, or dedicated scenario abstraction.
- Use one clear lifecycle with explicit process ownership and cleanup.
