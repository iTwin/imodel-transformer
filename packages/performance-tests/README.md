# Presentation Performance Tests

A package containing performance tests for the [`@itwin/imodel-transformer` library](../../README.md).

## Tests

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

