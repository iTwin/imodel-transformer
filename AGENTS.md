# AGENTS.md

Guidance for AI agents working in `iTwin/imodel-transformer`. Read `packages/transformer/README.md` for package behavior, strict peer-dependency handling, and environment variables.

## Workspace

| Package                        | Purpose                                                                              |
| ------------------------------ | ------------------------------------------------------------------------------------ |
| `packages/transformer`         | Published `@itwin/imodel-transformer` library and its Vitest suite.                  |
| `packages/test-app`            | CLI and sample app for manual runs.                                                  |
| `packages/performance-tests`   | Mocha performance-regression suite. Run it explicitly; root `pnpm test` excludes it. |
| `packages/performance-scripts` | Performance profiling helpers.                                                       |

## Build and test

- In `packages/transformer`, use `pnpm build`, `pnpm test`, and `pnpm cover`. Vitest runs `src/test/**/*.test.ts` directly; `build` separately validates the compiled CommonJS output.
- `pnpm cover` uses Vitest's V8 provider, enforces the configured thresholds, and writes reports to `packages/transformer/coverage`.
- `extract-api` regenerates `common/api/*`. Never edit those files manually. Commit regenerated reports when a public API changes.
- `src/test/setupVitest.ts` starts and stops `IModelHost` and registers custom assertions for each test file. Tests run in a bounded pool of forked workers with worker-local output directories.
- `HubMock` comes from `@itwin/core-backend` internals.

### Edit transactions

Use the `withEditTxn` pattern already present in a test instead of adding a bare `db.saveChanges()`:

```ts
const id = withEditTxn(db, "insert PhysicalObject", (txn) => {
  return someId;
});
```

The edit-transaction migration is a breaking-change area tracked by #305 and #306. Flag semantic changes as major rather than silently folding them into unrelated work.

## Validation and release

- Use the Node version from `.github/workflows/ci.yml`; `package.json` defines the supported range.
- Package CI and release run in `.github/workflows/`. Documentation uses `.azure-pipelines/generate-docs.yaml` and the external `docs-build.yaml@itwinjs-core` template.
- Published behavior changes require a beachball change file. `pnpm check` validates it.
- Document minor and major changes in `docs/changehistory/NEXT_VERSION.md`, including migration steps for breaking changes.

## Error ownership

- For consumer-actionable transformer failures, throw `ITwinError` with `IModelTransformerErrorScope` and an `IModelTransformerError` key. Consumers branch on the scope and key, not the message.
- Preserve errors from core, backend, and database APIs unless deliberately translating them to a more specific transformer error. Preserve the original error as `cause` when translating.
- Use plain `Error` or an assertion for internal invariants and implementation bugs.
- Do not create `IModelError` instances for transformer-owned failures.
