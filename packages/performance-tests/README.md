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

The default is one warm-up plus eight measured samples. Set
`QUICK_PERF_SAMPLES` only for local diagnostics. Reports are written under
`test/quick/.quick-output/` unless `QUICK_PERF_OUTPUT` is set and include
`samples.jsonl`, `summary.json`, and `summary.csv`.

The calibrated fixture contains 6,000 base elements, 12,000 aspects, 3,000
relationships, and 3,000 geometry-bearing elements. Its eight changesets apply
600 element inserts/updates/deletes, 600 aspect inserts/updates, 1,200 aspect
deletes, 300 relationship inserts/updates, 825 relationship deletes, and 150
geometry updates. This is 25 deterministic repetitions of one balanced content
unit, preserving the original scenario ratios.

`varianceStatus` requires coefficient of variation and normalized MAD at or
below 5%. An unstable manual run emits a workflow warning and must not be used
as regression evidence. It does not currently fail the manual workflow: six
local calibration suites informed the final scale, and three ratio-correct final
suites on a shared workstation produced 1.38-1.48 second medians but only two
met the CV threshold. A hard gate would therefore have false-failed one of three
final runs. Revisit the failure policy after repeated measurements on the target
Windows runner or a dedicated performance agent.

`pnpm quick:build-fixture` writes the canonical recipe manifest.
`pnpm quick:verify-fixture` performs two fresh reconstructions (warm-up plus one
measured sample), checks their semantic digests, and writes a diagnostic report.

Here are tests we need but don't have:

- *Identity Transform*
  transform the entire contents of the iModel to an empty iModel seed
- *JSON Geometry Editing Transform*
  transform the iModel, editing geometry as we go using the json format
- *Binary Geometry Editing Transform*
  transform the iModel, editing geometry as we go using elementGeometryBuilderParams
- *Optimistically Locking Remote Target*
- *Pessimistically Locking Remote Target*
- *Processing Changes*
- *More Branching Stuff*


## Usage

1. Clone the repository.

2. Install dependencies:

   ```sh
   pnpm install
   ```

3. Create `.env` file using `template.env` template.

5. Run:

   ```sh
   pnpm test
   ```

<!-- FIXME: output csv -->
6. Review results like:

```sh
pnpm exec process-results < report.jsonl
```
