# Performance test agent guidance

Read `README.md` for commands, benchmark authoring, and report behavior. Read
`ARCHITECTURE.md` before changing quick-harness components, provider lifecycles,
timing boundaries, or the weekly regression contract. The repository-root
`AGENTS.md` also applies.

## Working in this package

Install workspace dependencies from the repository root, then run performance
commands from `packages/performance-tests`. Build the transformer prerequisite
before quick commands:

```sh
pnpm --dir ../transformer build:cjs
```

`pnpm test` runs the credential-dependent weekly suite. Use the quick scripts
documented in `README.md` for credential-free harness and benchmark work.

## Validation

For A/B comparison changes, run:

```sh
pnpm build
pnpm lint
pnpm build:quick-cli
pnpm test:quick-comparison
pnpm test:quick-unit
pnpm test:quick-harness
```

Also run a real isolated comparison smoke when changing process orchestration,
fixture materialization, transformer resolution, or lifecycle cleanup.

Each isolated A/B worker records wall time and peak RSS. Interpret these metrics independently; the informational threshold and workflow warning apply only to wall time. Peak RSS comes from Node's `process.resourceUsage().maxRSS` and covers the complete worker lifetime.
