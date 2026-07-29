# Presentation Performance Tests

A package containing performance tests for the [`@itwin/imodel-transformer` library](../../README.md).

## Tests

### Quick incremental performance

The quick suite is independent of the cloud-backed weekly regression suite. It
reconstructs a fresh local HubMock from the versioned
`balanced-incremental` recipe for every sample, establishes target provenance,
pushes eight real source changesets, and then times only
`IModelTransformer.process()` with `argsForProcessChanges`.

Reconstruction, verification, and reporting are outside benchmark timing but
are reported and count against the 15-minute end-to-end budget. The suite does
not use iModelHub credentials or download QA iModels.

Build the transformer package before running the TypeScript suite:

```sh
pnpm --dir ../transformer build:cjs
pnpm test:quick
```

The default scenario is `incremental-synchronization`, selected with
`QUICK_PERF_SCENARIO`. Unknown scenario names fail before fixture
reconstruction. The default run is sample 0 as a warm-up plus eight measured
samples. Each sample is a fresh reconstruction, one timed scenario execution,
and untimed verification and cleanup. The warm-up follows the same lifecycle
but is excluded from summary timing statistics. Set `QUICK_PERF_SAMPLES` only
for local diagnostics. Reports are written under `test/quick/.quick-output/`
unless `QUICK_PERF_OUTPUT` is set and include `samples.jsonl`, `summary.json`,
and `summary.csv`. Every sample and report includes the scenario ID, and the
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

The GitHub Actions workflow is manual-only. Select the branch with GitHub's
native workflow ref and the scenario with its dispatch input:

```sh
gh workflow run quick-performance.yml --ref <branch> \
  -f scenario=incremental-synchronization
```

GitHub can dispatch this workflow only after `quick-performance.yml` exists on
the repository's default branch. The `--ref` selects which committed branch
revision runs after that requirement is met; there is intentionally no custom
branch input and no automatic pull-request or push trigger.

### Quick A/A calibration and A/B comparison

`quick-performance-comparison.yml` is a separate manual-only workflow for
`changeset-scanning`. It does not use the weekly Hub-backed regression suite and
never creates a merge-blocking performance gate. Each independent job produces
exactly one `PairObservation`: the declared `AB` or `BA` order runs one isolated
process per arm, each process performs one excluded warm-up plus three measured
scenario executions, and each arm's three measurements are median-collapsed
before computing one log-ratio. This is eight scenario executions per job; the
within-process samples are never treated as three independent pairs.

Calibration mode builds one selected `calibration_ref` for both labeled arms and
runs three independent jobs in `AB`, `BA`, `AB` order. The aggregation job
rejects fixture, recipe, workload, environment, execution-policy, build, or
fingerprint mismatches. Three matching observations from three jobs establish
the initial pool. An absolute A/A band at or below 5% is target quality, between
5% and 10% is marginal, and 10% or greater is unresolvable. All three remain
informational; the practical effect and equivalence threshold is 10%.

```sh
gh workflow run quick-performance-comparison.yml --ref main \
  -f mode=calibrate-a-a \
  -f scenario=changeset-scanning \
  -f calibration_ref=main
```

Comparison mode checks out and builds baseline and candidate separately. The
top-layer harness loads `ChangedInstanceIds` from each explicit built package in
its own child process, so the baseline ref does not need to contain the
comparison harness. Supply the prior calibration workflow run ID and artifact;
leave `candidate_ref` blank to use the workflow ref selected by `--ref`.

```sh
gh workflow run quick-performance-comparison.yml --ref main \
  -f mode=compare-a-b \
  -f scenario=changeset-scanning \
  -f baseline_ref=main \
  -f candidate_ref=my-candidate \
  -f pair_order=AB \
  -f calibration_run_id=123456789 \
  -f calibration_artifact=QuickPerformanceCalibration \
  -f calibration_file=calibration.json
```

The workflow publishes raw arm samples, the pair observation (including discard
reasons), the calibration pool and band, summary JSON, and Markdown used for the
job summary. A/B rejects a missing calibration or any calibration fingerprint
or hosted-runner class mismatch instead of borrowing a nearby result.

GitHub can dispatch this workflow only after
`quick-performance-comparison.yml` reaches the repository's default branch.
Before then, local smoke runs can exercise process orchestration but cannot
create hosted calibration evidence. The CLI's `prepare-fixture --smoke true`
uses an explicitly labeled reduced fixture; its different identity prevents it
from being accepted as calibration for the full fixture.

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

4. Run:

   ```sh
   pnpm test
   ```

<!-- FIXME: output csv -->

6. Review results like:

```sh
pnpm exec process-results < report.jsonl
```
