# Transformer Performance Tests

This package contains performance tests for
[`@itwin/imodel-transformer`](../../README.md). It has two independent suites:

| Suite             | Entry point                           | Purpose                                                           | Requirements                                            |
| ----------------- | ------------------------------------- | ----------------------------------------------------------------- | ------------------------------------------------------- |
| Quick performance | `test/quick/QuickPerformance.test.ts` | Run a bounded, deterministic transformer benchmark                | Local machine only                                      |
| Weekly regression | `test/TransformerRegression.test.ts`  | Run long-lived transformer comparisons against configured iModels | Hub credentials, OIDC configuration, and network access |

The quick infrastructure tests are separate from both performance suites:

| Tests                 | Location                                    | Purpose                                                                      |
| --------------------- | ------------------------------------------- | ---------------------------------------------------------------------------- |
| Quick unit            | `test/quick/tests/unit/**/*.test.ts`        | Validate catalogs, fixture descriptors, resolution, and statistics           |
| Quick integration     | `test/quick/tests/integration/**/*.test.ts` | Validate database, HubMock, fixture artifact, runner, and cleanup lifecycles |
| Weekly infrastructure | `test/unit/**/*.test.ts`                    | Validate registration and cleanup used by the weekly suite                   |

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the quick suite's component model,
provider lifecycles, execution diagram, timing boundaries, and report format.

## Quick performance terminology

The quick suite has one generic Vitest entry point. A run is assembled from these
parts:

| Term         | Meaning                                                                                                                                                                                                             |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Scenario** | The performance test: the transformer behavior being measured, such as applying source changes incrementally to an existing target iModel.                                                                          |
| **Recipe**   | The deterministic specification for the generated source iModel: EC schemas, initial element/aspect/relationship distribution, geometry, random seed, and source changesets.                                        |
| **Provider** | The form and lifecycle of the iModel data supplied to the scenario. A provider can supply live source and target `BriefcaseDb`s backed by `HubMock`, or a detached source `BriefcaseDb` with local changeset files. |
| **Fixture**  | A named configuration that combines a recipe with a provider topology, expected content distribution, version, and reproducibility identity.                                                                        |
| **Catalog**  | The set of scenario and fixture IDs that users can select through environment variables or workflow inputs.                                                                                                         |
| **Harness**  | The catalogs, runner, fixture infrastructure, validation, reporting, and their unit/integration tests. Harness tests do not measure transformer performance.                                                        |

The provider creates and owns the source, target, Hub, and changeset resources.
The scenario uses those resources to construct `IModelTransformer`, choose its
options, and select the operation measured.

The current benchmark resolves to:

```text
incremental-synchronization scenario
  + balanced-incremental fixture
      + balanced-incremental recipe
      + source-and-empty-target topology
      + liveHubProvider
```

That provider supplies source and target `BriefcaseDb`s backed by a running
`HubMock`. The timed operation is `IModelTransformer.process()` configured to
process source changes into the existing target.

The package also contains a `source-only` fixture backed by
`detachedBriefcaseProvider`. It supplies a read-only source `BriefcaseDb` and
local changeset files without a running Hub during scenario execution. No
performance scenario in this layer currently consumes that fixture; its artifact
lifecycle is covered by integration tests for use by future source-only
scenarios.

## Running the quick suite

Install the workspace dependencies from the repository root:

```sh
pnpm install
```

Build the CommonJS transformer package before running quick commands:

```sh
pnpm --dir packages/transformer build:cjs
```

Then run commands from `packages/performance-tests`:

| Command                       | Purpose                                                                                               |
| ----------------------------- | ----------------------------------------------------------------------------------------------------- |
| `pnpm build`                  | Type-check the performance-test package                                                               |
| `pnpm lint`                   | Lint the performance-test sources                                                                     |
| `pnpm test:quick-unit`        | Run the quick infrastructure unit tests                                                               |
| `pnpm test:quick-integration` | Run the database- and HubMock-backed integration tests                                                |
| `pnpm test:quick-harness`     | Run all quick unit and integration tests; does not run the benchmark                                  |
| `pnpm test:quick`             | Run the selected performance scenario                                                                 |
| `pnpm quick:build-fixture`    | Compile the native ESM fixture CLI, initialize its output, and write the selected fixture descriptor  |
| `pnpm quick:verify-fixture`   | Run one warm-up plus one measured sample, verify deterministic results, and write a diagnostic report |

The local benchmark default is one warm-up followed by one measured sample:

```sh
pnpm test:quick
```

### Selecting a scenario and fixture

| Environment variable  | Meaning                                                                                | Default                                 |
| --------------------- | -------------------------------------------------------------------------------------- | --------------------------------------- |
| `QUICK_PERF_SCENARIO` | Scenario catalog ID                                                                    | `incremental-synchronization`           |
| `QUICK_PERF_FIXTURE`  | Fixture catalog ID; must satisfy the selected scenario's required topology and claims  | The scenario's `defaultFixtureId`       |
| `QUICK_PERF_SAMPLES`  | Positive integer number of measured samples; the runner always adds one warm-up        | `1` locally; `8` in the workflow        |
| `QUICK_PERF_OUTPUT`   | Report and working directory below `test/quick/.quick-output` or a temporary directory | `test/quick/.quick-output/<fixture-id>` |

Example in a POSIX shell:

```sh
QUICK_PERF_SCENARIO=incremental-synchronization \
QUICK_PERF_FIXTURE=balanced-incremental \
QUICK_PERF_SAMPLES=3 \
pnpm test:quick
```

Example in PowerShell:

```powershell
$env:QUICK_PERF_SCENARIO = "incremental-synchronization"
$env:QUICK_PERF_FIXTURE = "balanced-incremental"
$env:QUICK_PERF_SAMPLES = "3"
pnpm test:quick
```

On Windows, use a short `QUICK_PERF_OUTPUT` path under `$env:TEMP` when the
repository is nested deeply enough to approach the legacy path-length limit.

Each run writes:

- `samples.jsonl`: one record for the warm-up and each measured sample.
- `summary.json`: structured aggregate and reliability classification.
- `summary.csv`: compact aggregate for spreadsheet or dashboard ingestion.

## Running the manual workflow

`.github/workflows/quick-performance.yml` is manual-only. It runs eight measured
samples on `ubuntu-latest` and publishes the report files:

```sh
gh workflow run quick-performance.yml --ref <branch> \
  -f scenario=incremental-synchronization
```

GitHub can dispatch the workflow only after the workflow file exists on the
repository's default branch. The caller must have repository write access.

## Adding quick performance coverage

### Add a scenario

Add a scenario when the transformer operation being measured changes. Examples
include a full transformation into an empty target, incremental synchronization,
or transformation with geometry remapping.

1. Add the implementation under `test/quick/src/scenarios/`.
2. Export a `BenchmarkScenarioDefinition` with:
   - A stable catalog `id`.
   - A compatible `defaultFixtureId`.
   - The required fixture topology and scenario claims.
   - A factory that creates the scenario for one prepared dataset.
3. Put only the transformer operation being measured in `measure()`.
4. Put semantic/provenance validation in `finish()` and resource release in
   `abort()`.
5. Register the definition in
   `test/quick/src/catalogs/ScenarioCatalog.ts`.
6. Add the scenario ID to the workflow dispatch choices when it should be
   selectable in GitHub Actions.
7. Add focused unit tests for resolution and integration tests for its database
   lifecycle.

Do not add another performance test file. `QuickPerformance.test.ts` runs every
catalog scenario selected through configuration.

### Add a recipe

Add a recipe when the generated source iModel's schema, content distribution, or
change mix changes.

1. Add the generation logic under `test/quick/src/fixtures/recipes/`.
2. Implement `FixtureRecipe`:
   - `createSeed()` creates the source iModel seed and imports required EC schemas.
   - `applySourceChangesets()` performs deterministic source edits and pushes the
     expected changesets.
3. Register the recipe in `FixtureRecipe.ts`.
4. Store reusable schema inputs under `test/quick/assets/schemas/`.
5. Add a fixture descriptor to `FixtureCatalog.ts` that references the recipe,
   declares the expected distribution, and includes every generation input in
   its identity hash.
6. Add integration coverage proving that repeated construction produces the
   expected distribution and semantic digest.

A recipe describes iModel contents, not how those contents are delivered to a
scenario. Reuse the same recipe when only the provider topology changes.

### Add a fixture

Add a fixture descriptor when a recipe needs a selectable scale, topology,
version, or content identity.

1. Add the descriptor to `FixtureCatalog.ts`.
2. Select an existing recipe by ID.
3. Select a supported topology. The topology determines the provider.
4. Declare the expected base content and operation counts.
5. Include recipe source, schemas, dependency lockfile, seed, topology, and
   generator versions in the recipe hash.
6. Advertise only scenario claims that the fixture actually supports.

Changing fixture contents requires a descriptor version or identity change so
reports from different generated iModels cannot be compared accidentally.

### Add a provider

Add a provider only when a scenario needs a new physical iModel topology or
lifecycle. For example, a future provider could supply local source and target
`SnapshotDb`s without using `HubMock`.

1. Add the topology to `FixtureTopology` and its prepared-dataset shape to
   `FixtureProvider.ts`.
2. Implement the two-stage `FixtureProvider` lifecycle under
   `test/quick/src/fixtures/providers/`.
3. Update `getFixtureProvider()` to select it.
4. Add fixture descriptors and scenario capability checks for the topology.
5. Add integration tests covering database ownership, pristine sample creation,
   failure cleanup, and deterministic results.

Providers own database construction, copying, opening, and disposal. Recipes
must not start or stop `IModelHost` or `HubMock`. Scenarios own transformer
construction and options.

## Running the weekly regression suite

The weekly suite is credential-dependent and intentionally excluded from the
repository root `pnpm test`.

1. Copy `template.env` to `.env` and configure the required Hub and OIDC values.
2. Run the serialized Vitest suite:

   ```sh
   pnpm test
   ```

3. Review `test/.output/report.csv`.

The CSV path and format are consumed by the hosted weekly performance pipeline.
See [Weekly regression architecture](./ARCHITECTURE.md#weekly-regression-architecture)
before changing its lifecycle or report contract.
