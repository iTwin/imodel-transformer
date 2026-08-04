# Performance test agent guidance

This package contains the credential-free quick benchmark harness and the
credential-dependent weekly regression suite. Read `README.md` and
`ARCHITECTURE.md` before changing quick benchmark behavior. The repository-root
`AGENTS.md` also applies.

## Work from this directory

Install workspace dependencies once from the repository root. From
`packages/performance-tests`, build the transformer prerequisite before quick
commands:

```sh
pnpm --dir ../transformer build:cjs
pnpm build
pnpm test:quick-comparison
pnpm build:quick-cli
```

Use Vitest only. Do not add Mocha, Chai 4, `ts-node`, or their types.

| Command                       | Use                                                                 |
| ----------------------------- | ------------------------------------------------------------------- |
| `pnpm build`                  | Type-check this package                                             |
| `pnpm lint`                   | Lint all performance-test TypeScript                                |
| `pnpm test:quick-comparison`  | Run focused A/B orchestration and report tests                      |
| `pnpm test:quick-unit`        | Run all quick unit tests                                            |
| `pnpm test:quick-integration` | Run fixture, database, HubMock, runner, and cleanup tests           |
| `pnpm test:quick-harness`     | Run every quick unit and integration test                           |
| `pnpm build:quick-cli`        | Compile the native ESM quick CLIs to `test/quick/runtime/.compiled` |
| `pnpm test:quick`             | Execute the selected quick benchmark                                |
| `pnpm quick:compare`          | Build the quick CLI and coordinate a prepared local A/B run         |

Do not run `pnpm test` unless the credential-dependent weekly suite is
intended and its environment is configured.

## Quick A/B architecture

The candidate checkout owns the comparison coordinator, worker code, scenario,
fixture recipe, and one immutable fixture artifact. Each arm runs in its own
checkout with its own locked dependency installation and independently built
transformer package. Copy the candidate's compiled quick harness into the
baseline checkout so both arms run identical orchestration code.

The coordinator:

1. asks a fresh candidate worker to build one detached fixture artifact;
2. validates the artifact's SHA-256 content identity;
3. starts a fresh Node process and module graph for every warm-up and measured
   execution;
4. alternates baseline-first and candidate-first measured pairs;
5. proves each worker resolved the transformer below its assigned checkout;
6. compares fixture identity and semantic results before reporting medians.

Transformer version and compiled-content provenance are arm-specific. Never add
them to workload identity. The briefcase, changesets, recipe data, scenario,
fixture descriptor, Node version, and core-backend version must remain identical
across arms. Review lockfile differences before interpreting a result: expected
transformer dependency changes are part of the candidate, but unrelated
dependency drift makes the comparison unsuitable until it is removed.

The default policy is one warm-up plus three measured executions per arm. It is
alternating, not position-balanced, because an odd measured count necessarily
gives one arm one extra first position.

## Prepare a local comparison

Use two clean checkouts: the current candidate and a baseline at the intended
base revision. Do not compare two transformer builds from one checkout.

1. In both checkouts, install the lockfile with `pnpm install --frozen-lockfile`.
2. From the candidate's `packages/performance-tests` directory, run
   `pnpm --dir ../transformer build:cjs`, `pnpm build`,
   `pnpm test:quick-comparison`, and `pnpm build:quick-cli`.
3. Build the baseline transformer with
   `pnpm --dir <baseline-root>/packages/transformer build:cjs`.
4. Replace the baseline's
   `packages/performance-tests/test/quick/runtime/.compiled` directory with an
   exact copy of the candidate's compiled directory.
5. From this package in the candidate checkout, set:

```text
QUICK_PERF_BASELINE_ROOT=<absolute baseline repository root>
QUICK_PERF_CANDIDATE_ROOT=<absolute candidate repository root>
QUICK_PERF_BASELINE_REVISION=<baseline revision label>
QUICK_PERF_CANDIDATE_REVISION=<candidate revision label>
QUICK_PERF_COMPARISON_OUTPUT=<absolute output directory>
QUICK_PERF_SCENARIO=changeset-scanning
QUICK_PERF_COMPARISON_SAMPLES=3
QUICK_PERF_COMPARISON_THRESHOLD_PERCENT=5
QUICK_PERF_COMPARISON_WORKER_TIMEOUT_SECONDS=600
```

6. Run the already-compiled coordinator without rebuilding the candidate after
   the copy:

```sh
node test/quick/runtime/.compiled/quick/src/cli/comparisonCli.js
```

On Windows, use a short output path under `$env:TEMP`. If iModelJS reports that
its shared profile is locked, wait for the other process to finish and retry;
do not kill unrelated processes.

## Interpret output

`comparison.json` is the machine-readable report. `comparison.md` is appended
to the Actions run summary. `comparison-samples.jsonl` contains every warm-up
and measured record.

Positive percentage delta means the candidate is slower. The threshold and
status are informational only: they do not establish confidence, significance,
or a merge-blocking result. The pull-request workflow emits a warning annotation
when the status is `candidate-slower-than-threshold`, but the job remains
successful. Worker failures, timeouts, fixture mismatches, transformer
resolution failures, and semantic mismatches still fail the job.

## Change rules

- Reuse the current benchmark registry, recipe, provider, runner, and
  changeset-scanning APIs.
- Preserve one candidate-authored immutable workload for both arms.
- Preserve a fresh process per warm-up and measured execution.
- Keep the report understandable: arm medians, measured samples, percentage
  delta, and informational status.
- Do not add calibration pools, confidence intervals, bootstrap analysis, power
  analysis, sign tests, distribution diagnostics, execution fingerprints, or
  legacy scanner/oracle/ledger APIs.
- Add focused Vitest coverage for orchestration, identity, output, and failure
  behavior.
- Use `withEditTxn` in database tests instead of adding bare
  `db.saveChanges()` calls.
- Treat cleanup and `IModelHost.shutdown()` failures as failures; never silently
  preserve a partial fixture or report.

## Validation

For A/B changes, run:

```sh
pnpm --dir ../transformer build:cjs
pnpm build
pnpm lint
pnpm build:quick-cli
pnpm test:quick-comparison
pnpm test:quick-unit
pnpm test:quick-harness
```

Also run one real isolated comparison smoke when orchestration, fixture
materialization, transformer resolution, or process lifecycle changes.
