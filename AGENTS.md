# AGENTS.md

Guidance for AI agents working in `iTwin/imodel-transformer`. For what the package does, versioning/strict peer-dep behavior, and env vars, read `packages/transformer/README.md` first — this file only covers things that aren't obvious from the README, `package.json`, or `CONTRIBUTING.md`.

## Layout

pnpm workspace, four packages under `packages/`:

| Package | Name | What it is |
|---|---|---|
| `transformer` | `@itwin/imodel-transformer` | The published library. All real code + tests live here. |
| `test-app` | `transformer-test-app` | CLI/sample app for manual runs. |
| `performance-tests` | `transformer-performance-tests` | Perf regression suite. **Not run by `pnpm test`.** |
| `performance-scripts` | — | Helper scripts; has no real tests. |

## Build & test gotchas

(Scripts themselves are in `package.json`; these are the non-obvious bits.)

- `pnpm test` runs every package **except** `transformer-performance-tests` — it's filtered out, run it explicitly if needed.
- Inside `packages/transformer`, `build` runs `tsc` → copy test assets → `extract-api`. Mocha runs the compiled `lib/cjs/**/*.test.js`, so **build before test** — editing `.ts` alone won't change what runs.
- `pnpm cover` (nyc) only produces real coverage for `transformer`; no artifact is persisted and CI does not publish coverage.
- API report: `extract-api` regenerates `common/api/*`. If a public API changes, commit the updated report or CI fails. Never hand-edit `common/api/*`.

## Test stack

Mocha + Chai (+ Sinon, chai-as-promised) + nyc. Tests are in `packages/transformer/src/test/`, mostly under `standalone/`. They compile to `lib/cjs/test/` and run from there.

iModelHub is mocked via `HubMock` from `@itwin/core-backend` internals.

### Edit transactions (active breaking-change area)

Newer tests wrap edits in `withEditTxn` (imported from `@itwin/core-backend`) instead of bare `db.saveChanges()`:

```ts
const id = withEditTxn(db, "insert PhysicalObject", (txn) => {
  // ...do inserts/updates against db...
  return someId;
});
```

A migration to require edit transactions is in progress (`#306` converted several test files; `#305`/`IModelTransformer.ts` related). When you touch or add tests, **match the `withEditTxn` pattern already used in the file** rather than reintroducing raw `saveChanges`. Treat anything around edit-txn semantics as a **major / breaking** change — flag it, don't silently "fix" it.

## Node version

Local and CI Node versions can differ. Treat the CI workflow (`.github/workflows/ci.yml`) as the source of truth for the version to run, and `package.json` `engines` for the supported range — match CI when running locally. Don't hardcode a version here.

## CI & docs

- Package CI/release: **GitHub Actions** (`.github/workflows/`). CI = build + lint + test on a core-version matrix.
- Docs: **Azure Pipelines** (`.azure-pipelines/generate-docs.yaml`), delegating to the shared `docs-build.yaml@itwinjs-core` job — part of the docs path lives outside this repo.

## Changes / PR hygiene

- Versioning/changelog is **beachball**. Every PR that changes published behavior needs a change file (`pnpm change`); `pnpm check` enforces it.
- Pick the change `type` deliberately: use **`major`** for breaking changes (e.g. the edit-txn requirement), not `patch`.

## Agent guardrails

- Default to one logical change per task. Don't refactor adjacent code or "modernize" tooling (e.g. swapping mocha→vitest) unless explicitly asked — those are tracked as separate work.
- Don't create branches, push, or open PRs unless the user explicitly asks. Draft locally first.
- Perf tests are intentionally excluded from `pnpm test`; don't wire them into the default path without being asked.
