# Performance Test Architecture

This package contains the transformer's performance regression tests and the
unit tests for their supporting infrastructure. Both use Vitest, but they have
different runtime requirements.

## Test categories

| Category                  | Location                                    | Purpose                                                                                       | External requirements                                   |
| ------------------------- | ------------------------------------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| Weekly regression         | `test/TransformerRegression.test.ts`        | Measure transformer implementations against selected Hub iModels and a generated local iModel | Hub credentials, OIDC configuration, and network access |
| Infrastructure unit       | `test/unit/**/*.test.ts`                    | Verify registration and cleanup behavior                                                      | None                                                    |
| Quick benchmark           | `test/quick/QuickPerformance.test.ts`       | Measure the selected scenario against a deterministic local fixture                           | None                                                    |
| Quick harness unit        | `test/quick/tests/unit/**/*.test.ts`        | Verify resolution, catalogs, manifests, and statistics                                        | None                                                    |
| Quick harness integration | `test/quick/tests/integration/**/*.test.ts` | Verify database-backed fixture, runner, reporting, and cleanup behavior                       | None                                                    |

The unit tests do not measure transformer performance. They validate code used
to construct and tear down the weekly or quick performance suites.

## Vitest configuration

`vitest.config.ts` includes both categories and uses one forked worker with file
parallelism disabled. The weekly tests share process-wide resources such as
`IModelHost`, authentication, downloaded briefcases, and the CSV reporter, so
they must run serially.

The weekly regression file opts out of test and hook timeouts because individual
transformations and downloads can run for hours. Infrastructure unit tests keep
Vitest's default timeouts so a broken unit test fails instead of hanging the
worker.

Environment loading is scoped to `TransformerRegression.test.ts`. Unit tests can
therefore run locally without a `.env` file by running
`pnpm exec vitest run test/unit`.

The repository's root `pnpm test` excludes this package. Run its tests explicitly
from `packages/performance-tests`.

## Quick suite architecture

The quick suite is independent of the weekly regression lifecycle. Its root
contains one generic benchmark entry point, `QuickPerformance.test.ts`, which
resolves the configured scenario and fixture before invoking the shared runner.
Adding a scenario normally does not require another performance test file.

- `src/framework/` coordinates resolution, samples, timed work, and lifecycle.
- `src/catalogs/` registers the selectable scenarios and fixtures.
- `src/reporting/` produces reports and computes descriptive statistics.
- `src/fixtures/` defines fixture descriptors, artifact persistence, provider
  lifecycles, recipes, and validation.
- `src/scenarios/` defines the behavior measured by the benchmark.
- `src/cli/` contains fixture build and verification commands.
- `src/support/` contains shared runtime support such as filesystem paths.
- `assets/` contains non-TypeScript fixture inputs such as schemas.
- `tests/unit/` contains fast infrastructure tests.
- `tests/integration/` contains database- and host-backed infrastructure tests.
- `tests/support/` contains helpers used only by infrastructure tests.

The fixture lifecycle has two stages. When the topology supports it, stage 1
builds and validates one immutable artifact. Stage 2 creates a pristine sample
copy, runs one scenario, validates the result, and disposes every resource.
Topologies that require a live local Hub rebuild their deterministic dataset per
sample instead.

The quick fixture commands compile `test/quick/src/cli/fixtureCli.ts` and its
runtime graph to native ESM under `test/quick/runtime/.compiled/`. The
runtime-only `"type": "module"` boundary keeps that output separate from the
package's TypeScript source graph.

## Quick report interpretation

Every report has three files:

- `samples.jsonl` contains one record per warm-up or measured sample.
- `summary.json` is the structured aggregate used by workflow reporting.
- `summary.csv` is a compact aggregate for spreadsheet or dashboard import.

Only `IModelTransformer.process()` contributes to each sample's
`wallMilliseconds`. Fixture reconstruction, semantic verification, and teardown
are untimed phases with their own measurements. `fixtureBuildMilliseconds` is a
single job-level value because stage 1 runs at most once. `jobMilliseconds`
measures the end-to-end benchmark run used by the budget assertion.

The warm-up follows the complete sample lifecycle but is excluded from the wall
statistics below. Reconstruction, verification, and teardown summaries include
the warm-up because all samples consume job time.

| Field                    | Meaning                                                                                           |
| ------------------------ | ------------------------------------------------------------------------------------------------- |
| `median`                 | Middle measured wall time; the primary typical-run value                                          |
| `p90`, `p95`             | Linearly interpolated upper-percentile wall times                                                 |
| `mad`                    | Median absolute distance from the median, in milliseconds                                         |
| `normalizedMad`          | MAD divided by the median, making dispersion comparable across run durations                      |
| `coefficientOfVariation` | Population standard deviation divided by the mean                                                 |
| `unstableSamples`        | Measured sample indexes more than 15% away from the median                                        |
| `varianceStatus`         | `stable` only when at least eight measured samples have both CV and normalized MAD at or below 5% |

`insufficient-samples` means there are fewer than eight measured samples.
`unstable` means the run completed correctly but environmental noise makes it
unsuitable as regression evidence. Variance is currently advisory; fixture
correctness and the scenario's end-to-end budget remain hard gates.

Compare two reports only when `reportSchemaVersion`, `scenarioId`, `fixtureId`,
`fixtureVersion`, `fixtureRecipeHash`, and every `fixtureGenerator` version
match. The reporter rejects mixed fixture identities within one report, but a
consumer comparing separate reports must enforce the same rule.

## Weekly regression lifecycle

The weekly suite has two phases.

### Collection

Before running any tests, Vitest imports `TransformerRegression.test.ts` to
discover them. During this setup step, the module:

1. Load environment configuration and authenticate.
2. Start `IModelHost` with Hub access.
3. Discover and filter the configured Hub iModels.
4. Add the generated local iModel.
5. Load the built-in and optional comparison transformer modules.
6. Build the supported test-case/module combinations.

If collection fails after authentication or host startup begins, the worker
lifecycle attempts to shut down every initialized resource before preserving
and rethrowing the original error.

### Execution

For each selected iModel, the suite:

1. Download or generate a local `.bim` source database and record its report
   metadata.
2. Opens a fresh read-only source database for each test.
3. Runs every supported test-case/module combination.
4. Closes the source database after each test.

The raw-insert comparison runs after the per-iModel transform cases. Once all
tests finish, the suite exports `test/.output/report.csv`, shuts down
`IModelHost`, and signs out. Cleanup tasks are all attempted even if an earlier
task fails.

## Test registration

`RegressionTestRegistration.ts` creates the execution matrix; it does not run
tests or store results.

Each test case names the factory function it requires from
`TestTransformerModule`. Each loaded transformer module is paired only with the
test cases it supports. `TransformerRegression.test.ts` consumes those
definitions and registers the corresponding Vitest tests.

The module name is the human-readable identifier used in test names and report
entries. The module object contains the implementation that the test executes.
Additional implementations can be loaded through `EXTRA_TRANSFORMERS`.

## Inputs and authentication

`template.env` documents the weekly suite's environment variables. The important
input controls are:

- `ITWIN_IDS`: iTwins from which test iModels are discovered.
- `IMODEL_IDS`: specific iModels to include, or `*` for every iModel in the
  configured iTwins.
- `EXTRA_TRANSFORMERS`: optional module paths for comparison implementations.
- `LOG_LEVEL`: verbosity for the iTwin logger.

CI uses headless authentication. Local weekly runs use the CLI authorization
client and still require the OIDC and Hub configuration described in
`template.env`. Never commit a populated `.env` file.

## Reporting

Test cases report measurements through the callback in `TestCaseContext`.
`@itwin/perf-tools` combines those measurements with iModel and branch metadata,
then writes `test/.output/report.csv`.

That path and CSV format are the artifact contract consumed by the hosted weekly
performance pipeline. Changes to either require coordinated pipeline validation.

## Extending the package

- Put credential-free tests of support code under `test/unit`.
- Add a weekly performance case under `test/cases`, declare its required
  transformer factory in `TestTransformerModule`, and add it to `testCasesMap`.
- Add a built-in transformer implementation under `test/transformers`, or load a
  comparison implementation with `EXTRA_TRANSFORMERS`.
- Give future performance-test categories their own entry file, setup, timeout,
  and reporting policy. Do not make Hub credentials or unlimited timeouts global
  merely because the weekly suite needs them.
