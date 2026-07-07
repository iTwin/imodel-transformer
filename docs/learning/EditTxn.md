# EditTxn usage with imodel-transformer

This document describes how `EditTxn` integrates with the transformer, importer, and related APIs.

## Overview

An [`EditTxn`](https://www.itwinjs.org/reference/core-backend/imodels/edittxn/) represents an explicit edit transaction on an `IModelDb`. Starting in v3, the transformer and importer require a caller-managed `EditTxn` instead of accepting a raw `IModelDb` for write operations.

## Lifecycle

The caller owns the `EditTxn` lifecycle:

```ts
const editTxn = new EditTxn(targetDb, "description");
editTxn.start();

// Use with transformer or importer
const transformer = new IModelTransformer({ source: sourceDb, target: editTxn });
await transformer.process();

editTxn.end();          // saves and commits
// or: editTxn.end("abandon")  // rolls back all changes
```

### Key rules

- **Start before constructing.** The transformer validates `editTxn.isActive` at construction time and throws if not started.
- **Reuse is fine.** You do not need a freshly created `EditTxn` — if you already have an active transaction from prior edits on the same db, pass it directly.
- **Only one active per db.** `EditTxn` enforces exclusivity — calling `start()` on a second `EditTxn` for the same db throws.
- **Caller ends the txn.** The transformer calls `saveChanges()` internally during processing but **never** calls `end()`. That responsibility belongs to the caller.

### Error handling

Wrap transformation calls in try/catch to ensure the transaction is properly ended on failure:

```ts
const editTxn = new EditTxn(targetDb, "transform");
editTxn.start();
try {
  const transformer = new IModelTransformer({ source: sourceDb, target: editTxn });
  await transformer.process();
  editTxn.end(); // save on success
} catch (err) {
  editTxn.end("abandon"); // roll back on error
  throw err;
}
```

## Reverse synchronization

Reverse sync writes provenance back to the **source** db, so it requires a second `EditTxn`:

```ts
const sourceEditTxn = new EditTxn(sourceDb, "reverse sync provenance");
sourceEditTxn.start();
const targetEditTxn = new EditTxn(targetDb, "reverse sync");
targetEditTxn.start();

const transformer = new IModelTransformer(
  { source: sourceDb, target: targetEditTxn },
  { sourceEditTxn, isReverseSynchronization: true }
);
await transformer.process();

sourceEditTxn.end();
targetEditTxn.end();
```

Without `sourceEditTxn`, the transformer throws at runtime: *"A reverse synchronization requires a sourceEditTxn..."*.

## IModelImporter

The importer takes a single `EditTxn` and derives `targetDb` from `editTxn.iModel`:

```ts
const editTxn = new EditTxn(targetDb, "import");
editTxn.start();
const importer = new IModelImporter(editTxn, options);
// importer.targetDb === editTxn.iModel
```

When passing a custom importer to the transformer, the transformer uses the importer's `editTxn`:

```ts
const importer = new IModelImporter(editTxn, { autoExtendProjectExtents: false });
const transformer = new IModelTransformer({ source: sourceDb, target: importer });
```

## TemplateModelCloner

`TemplateModelCloner` is always an in-place operation — source and target are the same iModel. It takes a single `EditTxn`:

```ts
const editTxn = new EditTxn(db, "clone templates");
editTxn.start();
const cloner = new TemplateModelCloner(editTxn);
const idMap = await cloner.placeTemplate3d(templateModelId, targetModelId, placement);
editTxn.end();
```

The cloner reads template definitions from `editTxn.iModel` and writes instantiated elements back to the same db.

## initializeBranchProvenance

This function fully owns its own `EditTxn` — callers do not pass or manage one:

```ts
await initializeBranchProvenance({ master: masterDb, branch: branchDb });
// Transaction is created, committed, and ended internally.
```
