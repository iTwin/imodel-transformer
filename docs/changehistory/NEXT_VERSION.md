# Next release notes

## Breaking changes: EditTxn-based constructors

`IModelTransformer`, `IModelImporter`, and `TemplateModelCloner` constructors now require an explicit [`EditTxn`](https://www.itwinjs.org/reference/core-backend/imodels/edittxn/) from `@itwin/core-backend`. This aligns the transformer with the iTwin.js platform's move toward explicit edit transactions and eliminates the possibility of mismatched db/txn references.

### `IModelTransformer`

The constructor now takes a single `IModelTransformArgs` object as its first argument, with an optional `IModelTransformOptions` second argument.

**Before:**

```ts
const transformer = new IModelTransformer(sourceDb, targetDb, options);
```

**After:**

```ts
const editTxn = new EditTxn(targetDb, "my transformation");
editTxn.start();
const transformer = new IModelTransformer(
  { source: sourceDb, target: editTxn },
  options
);
await transformer.process();
editTxn.end(); // saves changes; use end("abandon") to roll back
```

The `target` field accepts either:
- An `EditTxn` — the transformer creates a default `IModelImporter` internally (most common).
- A pre-configured `IModelImporter` — for custom import behavior.

The target `IModelDb` is derived from `editTxn.iModel` (or `importer.targetDb`).

### `IModelImporter`

The `targetDb` parameter has been removed. The importer now derives it from the `EditTxn`.

**Before:**

```ts
const importer = new IModelImporter(targetDb, editTxn, options);
```

**After:**

```ts
const importer = new IModelImporter(editTxn, options);
// importer.targetDb === editTxn.iModel
```

A new public `editTxn` getter is also available on `IModelImporter`.

### `TemplateModelCloner`

Since template cloning is always an in-place operation (source and target are the same iModel), the constructor now only requires an `EditTxn`.

**Before:**

```ts
const cloner = new TemplateModelCloner(sourceDb);
// or
const cloner = new TemplateModelCloner(sourceDb, targetDb);
```

**After:**

```ts
const editTxn = new EditTxn(db, "clone templates");
editTxn.start();
const cloner = new TemplateModelCloner(editTxn);
```

### `initializeBranchProvenance`

The `editTxn` property has been removed from `ProvenanceInitArgs`. The function now creates, manages, and commits its own `EditTxn` internally. Callers no longer need to create or end a transaction.

**Before:**

```ts
const editTxn = new EditTxn(branchDb, "init provenance");
editTxn.start();
await initializeBranchProvenance({ master, branch: branchDb, editTxn });
editTxn.saveChanges();
editTxn.end();
```

**After:**

```ts
await initializeBranchProvenance({ master, branch: branchDb });
// Changes are committed internally.
```

### EditTxn lifecycle contract

The caller is responsible for the `EditTxn` lifecycle when using `IModelTransformer` and `IModelImporter`:

1. Ensure the `EditTxn` is started before constructing the transformer/importer. This does **not** need to be a freshly created `EditTxn` — you can reuse an existing active transaction if you were already making edits to the target iModel.
2. Call `process()` (or other transformation methods).
3. Call `editTxn.end()` to save, or `editTxn.end("abandon")` to roll back.

The transformer will call `saveChanges()` internally during `processChanges` (via `ProcessChangesOptions.saveTargetChanges`), but **never** ends the transaction — that responsibility belongs to the caller.
