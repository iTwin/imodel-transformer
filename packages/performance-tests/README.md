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

| Term                   | Meaning                                                                                                                                                                                                                |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Scenario**           | The performance test: the transformer behavior being measured, such as applying source changes incrementally to an existing target iModel.                                                                             |
| **Recipe**             | The typed specification for an iModel workload: schemas and identity inputs, distribution, construction logic, and optional validation.                                                                                |
| **Configured fixture** | A named immutable invocation of a recipe with explicit parameters, topology, seed, version, label, and scenario claims.                                                                                                |
| **Fixture descriptor** | The serializable artifact/report manifest generated from a configured fixture. Infrastructure derives distribution, generator versions, and the recipe hash.                                                           |
| **Provider**           | The form and lifecycle of the iModel data supplied to the scenario. A provider can supply live source and target `BriefcaseDb`s, a detached source `BriefcaseDb`, or a standalone source plus fresh standalone target. |
| **Registration**       | One cohesive contribution containing a scenario and the configured fixtures that scenario supports.                                                                                                                    |
| **Harness**            | The registry, runner, fixture infrastructure, validation, reporting, and their unit/integration tests. Harness tests do not measure transformer performance.                                                           |

The provider creates and owns the source, target, Hub, and changeset resources.
The scenario uses those resources to construct `IModelTransformer`, choose its
options, and select the operation measured.

The current benchmark resolves to:

```text
incremental-synchronization scenario
  + balanced-incremental configured fixture
      + balanced-incremental recipe at scale 25
      + generated fixture descriptor
      + source-and-empty-target topology
      + liveHubProvider
```

That provider supplies source and target `BriefcaseDb`s backed by a running
`HubMock`. The timed operation is `IModelTransformer.process()` configured to
process source changes into the existing target.

The `large-base-incremental-synchronization` scenario measures the same
operation against the `large-base-incremental` fixture. Its recipe derives the
workload from a positive integer `scale`: at the registered `scale: 25`, it has
50,000 unchanged base elements in one flat model with a 50-element delta (25
inserts, 25 updates) across 2 changesets. Increasing the scale multiplies both
the base and delta while preserving the 1,000:1 base-to-change ratio and the
two-changeset schedule. Incremental synchronization should scale with
changeset size, so the extreme ratio exposes any cost proportional to the
unchanged base that `balanced-incremental`'s proportional churn dilutes. The
scale is part of the fixture parameters and therefore produces a distinct
fixture descriptor and artifact identity.

The package also contains a `source-only` fixture backed by
`detachedBriefcaseProvider`. It supplies a read-only source `BriefcaseDb` and
local changeset files without a running Hub during scenario execution. No
performance scenario in this layer currently consumes that fixture; its artifact
lifecycle is covered by integration tests for use by future source-only
scenarios.

The `standalone-full-transformation` scenario uses
`standalone-source-and-empty-target`. It prepares one immutable source
`SnapshotDb`, then gives every warm-up and measured sample a private read-only
source copy and a newly-created empty target. Untimed `prepare()` imports
schemas, `measure()` contains only `IModelTransformer.process()`, and
`finish()` computes the target output-shape digest used for A/B comparability.
Two configured fixtures support it: `standalone-full-transform` (element-heavy,
no relationships) and `relationship-heavy-transform` (5,000 elements with
30,000 `ElementGroupsMembers` relationships, exercising the relationship export
path including federation-guid lookups).

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
| `pnpm test:quick-comparison`  | Run focused isolated A/B orchestration and reporting tests                                            |
| `pnpm test:quick-integration` | Run the database- and HubMock-backed integration tests                                                |
| `pnpm test:quick-harness`     | Run all quick unit and integration tests; does not run the benchmark                                  |
| `pnpm test:quick`             | Run the selected performance scenario                                                                 |
| `pnpm quick:build-fixture`    | Compile the native ESM fixture CLI, initialize its output, and write the selected fixture descriptor  |
| `pnpm quick:verify-fixture`   | Run one warm-up plus one measured sample, verify deterministic results, and write a diagnostic report |
| `pnpm quick:compare`          | Compile and run the A/B coordinator against prepared baseline and candidate checkouts                 |

The local benchmark default is one warm-up followed by one measured sample:

```sh
pnpm test:quick
```

### Selecting a scenario and fixture

| Environment variable        | Meaning                                                                                | Default                                 |
| --------------------------- | -------------------------------------------------------------------------------------- | --------------------------------------- |
| `QUICK_PERF_SCENARIO`       | Scenario catalog ID                                                                    | `incremental-synchronization`           |
| `QUICK_PERF_FIXTURE`        | Fixture catalog ID; must satisfy the selected scenario's required topology and claims  | The scenario's `defaultFixtureId`       |
| `QUICK_PERF_SAMPLES`        | Positive integer number of measured samples; the runner always adds one warm-up        | `1` locally; `8` in the workflow        |
| `QUICK_PERF_OUTPUT`         | Report and working directory below `test/quick/.quick-output` or a temporary directory | `test/quick/.quick-output/<fixture-id>` |
| `QUICK_PERF_STANDALONE_BIM` | Absolute path to an existing standalone `.bim` copied into the selected artifact       | Generated recipe source                 |

Registered fixtures per scenario:

| Scenario ID                      | Fixture IDs                                                 | Default                     |
| -------------------------------- | ----------------------------------------------------------- | --------------------------- |
| `incremental-synchronization`    | `balanced-incremental`                                      | `balanced-incremental`      |
| `standalone-full-transformation` | `standalone-full-transform`, `relationship-heavy-transform` | `standalone-full-transform` |
| `schema-processing`              | `schema-processing-large`                                   | `schema-processing-large`   |
| `changeset-scanning`             | `update-heavy-scan`                                         | `update-heavy-scan`         |

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

Run the generated standalone full-transform workload in a POSIX shell:

```sh
QUICK_PERF_SCENARIO=standalone-full-transformation \
QUICK_PERF_SAMPLES=3 \
pnpm test:quick
```

Use an existing standalone BIM in a POSIX shell:

```sh
QUICK_PERF_SCENARIO=standalone-full-transformation \
QUICK_PERF_STANDALONE_BIM=/absolute/path/source.bim \
pnpm test:quick
```

The equivalent PowerShell command is:

```powershell
$env:QUICK_PERF_SCENARIO = "standalone-full-transformation"
$env:QUICK_PERF_STANDALONE_BIM = "C:\iModels\source.bim"
pnpm test:quick
```

`QUICK_PERF_STANDALONE_BIM` is valid only for a
`standalone-source-and-empty-target` fixture. The path must be absolute, and the
file must exist, have a `.bim` extension, and open as a standalone `SnapshotDb`;
briefcases and invalid
or unsupported databases are rejected. It must also be outside
`QUICK_PERF_OUTPUT` and other harness-managed artifact directories. The harness never opens the user's file
for benchmarking and never mutates it. Stage one copies it into the immutable
artifact, records its basename, byte length, and SHA-256, and combines that
identity with the configured recipe hash. The recipe still selects the
fixture's topology, claims, and scenario contract, but external bytes replace
recipe generation and generated-distribution validation.

Only the artifact copy is consumed after stage one. Isolated A/B workers do not
reopen the original file. The copied bytes contribute to the artifact content
hash, and the external identity is included in sample and comparison JSON, so
both arms must consume the same source. Portability remains limited by the
installed `@itwin/core-backend`: a BIM using an incompatible profile, schema,
encryption, or native format cannot be opened. Paths are not reported or hashed;
the artifact is relocatable after it is built.

On Windows, use a short `QUICK_PERF_OUTPUT` path under `$env:TEMP` when the
repository is nested deeply enough to approach the legacy path-length limit.

Each run writes:

- `samples.jsonl`: one record for the warm-up and each measured sample.
- `summary.json`: structured aggregate and reliability classification.
- `summary.csv`: compact aggregate for spreadsheet or dashboard ingestion.

## Pull request A/B comparison

`.github/workflows/quick-performance-comparison.yml` compares a pull request's
head SHA with its base SHA. Both checkouts build their own transformer package.
The candidate's compiled quick harness and test-utils runtime are copied into both
checkouts. A dedicated baseline worker authors one immutable fixture artifact
before timing begins, and every warm-up and measured worker copies from those exact
bytes. For incremental synchronization, that artifact includes prepared source and
target briefcases, their version-zero seeds, and both local-hub changeset timelines.
The target's initial full transformation and provenance therefore come from the
baseline transformer, modeling the common upgrade path in which candidate code
processes changes against baseline-created state. The artifact manifest hashes
every workload file, and each worker revalidates the hash before restoring its
private hub and briefcases. For standalone full transformation, the artifact
contains one standalone source and creates no target until a worker materializes
its private sample.

Each execution still gets a fresh Node process and module graph. Before running,
the worker proves that Node resolved `@itwin/imodel-transformer` to the entry
point below its assigned baseline or candidate checkout and records that build's
version and complete compiled-output content hash. Transformer build provenance is intentionally separate
from fixture identity, so an expected baseline/candidate transformer difference
does not masquerade as a workload mismatch.

Before any execution, one baseline worker builds the immutable fixture artifact.
Its manifest records a SHA-256 over every captured briefcase, seed, changeset,
props, and optional recipe-data file. Every warm-up and measured worker validates
that hash and materializes a private copy from the same artifact, so fresh process
isolation never regenerates the workload or repeats the initial full transform.

The initial policy runs one warm-up and three measured
`incremental-synchronization` executions per arm. The coordinator orders the
warm-up candidate/baseline, then alternates baseline/candidate and
candidate/baseline measured pairs. An odd measured-sample count cannot give both
arms the same number of first positions, so this policy is alternating rather
than position-balanced.

The comparison fails when a worker fails or times out, arm configuration
differs, or semantic digests differ. Each worker has a configurable timeout and
is terminated, then force-killed after a short grace period if it hangs. A
performance delta never fails the job.

For standalone full transformation, the semantic digest is a target output-shape
digest containing entity counts by class. It keeps A/B arms comparable without
duplicating source-to-target correctness assertions from the transformer test suite.

Successful runs publish:

- `comparison.json`: baseline and candidate medians, percentage delta, raw measured wall times and peak worker RSS values, arm transformer versions, baseline fixture-authoring revision and transformer version, shared fixture content hash, execution order, and informational threshold status.
- `comparison.md`: the same small result set for the Actions job summary.
- `comparison-samples.jsonl`: all warm-up and measured sample records with arm
  and revision labels.

The five-percent threshold is a visible informational label only. Three
measured samples do not establish statistical confidence or merge-blocking
significance. When the candidate median exceeds that threshold, the pull-request
workflow emits a GitHub warning annotation and remains successful.

The coordinator accepts `QUICK_PERF_SCENARIO`, `QUICK_PERF_FIXTURE`,
`QUICK_PERF_COMPARISON_SAMPLES`, and
`QUICK_PERF_COMPARISON_THRESHOLD_PERCENT`, and
`QUICK_PERF_STANDALONE_BIM` for the standalone topology.
`QUICK_PERF_COMPARISON_WORKER_TIMEOUT_SECONDS` sets the positive per-process timeout and defaults to 600 seconds. Every isolated worker reports its peak RSS through Node's `process.resourceUsage().maxRSS`, covering the complete worker lifetime including setup and teardown; `rssDeltaBytes` continues to cover only the scenario endpoints. Wall time and peak RSS are reported together but must be interpreted independently, and the informational threshold applies only to wall time. `QUICK_PERF_BASELINE_ROOT` is required; candidate and revision paths are set by the workflow.

## Running the manual workflow

`.github/workflows/quick-performance.yml` is manual-only. It runs eight measured
samples on `ubuntu-latest` and publishes the report files:

```sh
gh workflow run quick-performance.yml --ref <branch> \
  -f scenario=incremental-synchronization
```

GitHub can dispatch the workflow only after the workflow file exists on the
repository's default branch. The caller must have repository write access.
Merged scenarios remain discoverable in the `scenario` choice input. To run a
scenario that exists only on a feature branch, set the optional free-form
`scenario_override` input; it takes precedence:

```sh
gh workflow run quick-performance.yml --ref <branch> \
  -f scenario_override=my-feature-scenario
```

## Adding quick performance coverage

Add a scenario when the transformer operation being measured changes. Add a
recipe when the generated schema, content distribution, or change mix changes.
A scenario that reuses a registered fixture needs only its scenario module and
one registry entry.

For a custom fixture, co-locate its typed recipe and configured invocations under
`test/quick/src/fixtures/recipes/`, then include them in the scenario's benchmark
registration:

```ts
interface WorkloadParameters {
  readonly scale: number;
}

const workloadRecipe = defineFixtureRecipe<WorkloadParameters, RecipeState>({
  id: "workload",
  identity: {
    implementationFiles: [
      quickPath("src", "fixtures", "recipes", "workload.ts"),
    ],
    schemaFiles: [quickPath("assets", "schemas", "Workload.ecschema.xml")],
  },
  distribution: ({ scale }) => expectedDistribution(scale),
  createSeed: async (fileName, context) => buildSeed(fileName, context),
  applySourceChangesets: async (db, token, context, state) =>
    applyChanges(db, token, context, state),
  validate: async (db, context) => validateWorkload(db, context),
});

const workloadFixture = configureFixture(workloadRecipe, {
  id: "workload-medium",
  version: 1,
  label: "workload medium",
  scenarioClaims: ["full transformation"],
  topology: "source-only",
  seed: 328,
  parameters: { scale: 25 },
});

export const workloadBenchmark = defineBenchmark({
  scenario: workloadScenario,
  fixtures: [workloadFixture],
});
```

Add the registration import and one array entry in
`test/quick/src/catalogs/BenchmarkRegistry.ts`. Registration is explicit so the
compiled Node CLI has predictable imports; no provider, reporter, workflow, or
bespoke catalog test changes are needed.

The three fixture authoring stages are intentionally distinct:

1. A **recipe** owns typed workload logic and declared identity files.
2. A **configured fixture** invokes that recipe at a named scale/topology.
3. A **fixture descriptor** is generated for artifacts and reports.

The generated recipe hash includes fixture metadata, parameters, derived
distribution, seed, topology, declared implementation/schema files,
`pnpm-lock.yaml`, Node, and core backend versions. Transformer provenance is
reported separately because it is the intentional variable in an A/B run. Identity file
contents are newline-normalized so the same commit has the same identity across
platforms. Declare every helper file whose implementation affects generation.
Validation is optional and runs only when the recipe supplies it. Recipes remain
an imperative escape hatch; there is no required fixture DSL.

Keep only the transformer operation being measured in `measure()`. Put scenario
result/provenance checks in `finish()` and resource release in `abort()`. Do not
add another performance test file: `QuickPerformance.test.ts` runs the selected
registered scenario.

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
