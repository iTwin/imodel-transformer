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

<!-- FIXME: this isn't true yet-->
All tests run in isolation on every iModel in these projects:

- https://qa-connect-imodelhubwebsite.bentley.com/Context/892aa2c9-5be8-4865-9f37-7d4c7e75ebbf
- https://qa-connect-imodelhubwebsite.bentley.com/Context/523a1365-2c85-4383-9e4c-f9ec25d0e107
- https://qa-connect-imodelhubwebsite.bentley.com/Context/bef34215-1046-4bf1-b3be-a30ae52fefb6

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

