# Transformer Performance Tests

A package containing performance tests for the [`@itwin/imodel-transformer` library](../../README.md).

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the test categories, weekly suite
lifecycle, registration model, and extension guidance.

## Test categories

| Category                   | Entry points                                | Purpose                                                                     |
| -------------------------- | ------------------------------------------- | --------------------------------------------------------------------------- |
| Weekly regression          | `test/TransformerRegression.test.ts`        | Long-running comparisons against Hub and generated iModels                  |
| Weekly infrastructure unit | `test/unit/**/*.test.ts`                    | Registration, lifecycle, and cleanup behavior used by the weekly suite      |
| Quick benchmark            | `test/quick/QuickPerformance.test.ts`       | Credential-free, bounded local performance measurement                      |
| Quick harness unit         | `test/quick/tests/unit/**/*.test.ts`        | Resolution, catalogs, manifests, and statistics used by the quick benchmark |
| Quick harness integration  | `test/quick/tests/integration/**/*.test.ts` | Database-backed fixture, runner, reporting, and cleanup behavior            |

The quick harness units validate performance-test infrastructure; they do not
measure transformer performance. The quick benchmark and weekly regression suite
are separate Vitest entry points with separate runtime requirements. See
[ARCHITECTURE.md](./ARCHITECTURE.md) for their lifecycle and directory structure.

## Quick incremental performance

The quick suite is independent of the cloud-backed weekly regression suite. It
reconstructs a fresh local HubMock from the versioned
`balanced-incremental` recipe for every sample, establishes target provenance,
pushes eight real source changesets, and then times only
`IModelTransformer.process()` with `argsForProcessChanges`.

Reconstruction, verification, and reporting are outside benchmark timing but
are reported and count against the 15-minute end-to-end budget. The suite does
not use iModelHub credentials or download QA iModels.

Run these commands from `packages/performance-tests`:

| Command                               | Purpose                                                                  |
| ------------------------------------- | ------------------------------------------------------------------------ |
| `pnpm --dir ../transformer build:cjs` | Build the workspace transformer package required by the quick runtime    |
| `pnpm test:quick-unit`                | Run the quick harness unit tests                                         |
| `pnpm test:quick-integration`         | Run the database-backed quick harness integration tests                  |
| `pnpm test:quick-harness`             | Run all quick harness tests without executing the benchmark              |
| `pnpm quick:build-fixture`            | Compile the native ESM quick CLI and write the canonical recipe manifest |
| `pnpm quick:verify-fixture`           | Reconstruct the fixture twice and write a diagnostic report              |
| `pnpm test:quick`                     | Run one warm-up and one measured benchmark sample locally                |

The default scenario is `incremental-synchronization`, selected with
`QUICK_PERF_SCENARIO`. Unknown scenario names fail before fixture
reconstruction. The default run is sample 0 as a warm-up plus one measured
sample locally; the workflow runs eight measured samples. Each sample is a
fresh reconstruction, one timed scenario execution, and untimed verification
and cleanup. The warm-up follows the same lifecycle but is excluded from
summary timing statistics. `QUICK_PERF_SAMPLES` sets the positive integer
number of measured samples, and the manual workflow sets it to eight for
variance analysis. Reports are written under `test/quick/.quick-output/` unless
`QUICK_PERF_OUTPUT` is set and include `samples.jsonl`, `summary.json`, and
`summary.csv`. Every sample and report includes the scenario ID, and the
reporter rejects mixed-scenario sample sets.

The calibrated fixture contains 6,000 base elements, 12,000 aspects, 3,000
relationships, and 3,000 geometry-bearing elements. Its eight changesets apply
600 element inserts/updates/deletes, 600 aspect inserts/updates, 1,200 aspect
deletes, 300 relationship inserts/updates, 825 relationship deletes, and 150
geometry updates. This is 25 deterministic repetitions of one balanced content
unit, preserving the original scenario ratios.

`varianceStatus` requires coefficient of variation (standard deviation divided
by mean) and normalized MAD at or below 5%. CV can exceed the target when host
scheduling or background load delays one sample. An unstable manual run emits a
workflow warning and must not be used as regression evidence, but correctness
and the 15-minute budget remain hard gates. Variance does not currently fail the
manual workflow: six local calibration suites informed the final scale, and
three ratio-correct final suites on a shared workstation produced 1.38-1.48
second medians but only two met the CV threshold. A hard gate would therefore
have false-failed one of three final runs. The GitHub workflow now targets
`ubuntu-latest`, but no Linux reliability evidence exists until that workflow
runs. Revisit the failure policy after repeated measurements on the hosted
Linux runner or a dedicated performance agent.

See [Quick report interpretation](./ARCHITECTURE.md#quick-report-interpretation)
for the report identity fields, timing boundaries, statistical definitions, and
guidance on comparing runs.

The GitHub Actions workflow is manual-only. Select the branch with GitHub's
native workflow ref and the scenario with its dispatch input:

```sh
gh workflow run quick-performance.yml --ref <branch> \
  -f scenario=incremental-synchronization
```

GitHub can dispatch this workflow only after `quick-performance.yml` exists on
the repository's default branch, and the caller must have repository write
access. The `--ref` selects which committed branch revision runs after that
requirement is met; there is intentionally no custom branch input and no
automatic pull-request or push trigger.

`pnpm quick:build-fixture` writes the canonical recipe manifest.
`pnpm quick:verify-fixture` performs two fresh reconstructions (warm-up plus one
measured sample), checks their semantic digests, and writes a diagnostic report.

Here are tests we need but don't have:

- _Identity Transform_
  transform the entire contents of the iModel to an empty iModel seed
- _JSON Geometry Editing Transform_
  transform the iModel, editing geometry as we go using the json format
- _Binary Geometry Editing Transform_
  transform the iModel, editing geometry as we go using elementGeometryBuilderParams
- _Optimistically Locking Remote Target_
- _Pessimistically Locking Remote Target_
- _Processing Changes_
- _More Branching Stuff_

## Usage

1. Clone the repository.

2. Install dependencies:

   ```sh
   pnpm install
   ```

3. Create `.env` file using `template.env` template.

4. Run the serialized Vitest suite:

   ```sh
   pnpm test
   ```

5. Review `test/.output/report.csv`. This path is also the artifact contract used by
   the weekly Azure pipeline.
