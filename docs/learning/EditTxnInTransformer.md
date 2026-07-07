# EditTxn in imodel-transformer

> For general `EditTxn` concepts (lifecycle, `withEditTxn`, migration from implicit writes, common failure modes), see the [iTwin.js EditTxn learning page](https://www.itwinjs.org/learning/backend/edittxn/).

This document covers transformer-specific patterns only.

## IModelTransformer

The transformer requires a **started** `EditTxn` for the target db. Pass it directly or via a pre-configured `IModelImporter`:

```ts
// Direct — transformer creates a default importer internally
const transformer = new IModelTransformer({ source: sourceDb, target: editTxn });

// Via custom importer
const importer = new IModelImporter(editTxn, importOptions);
const transformer = new IModelTransformer({ source: sourceDb, target: importer });
```

The transformer **never** calls `editTxn.end()` — the caller owns that. It does call `saveChanges()` internally during `processChanges` (controlled by `ProcessChangesOptions.saveTargetChanges`).

## Reverse synchronization

Reverse sync writes provenance back to the **source** (branch) db, so it requires two `EditTxn`s — one for the target (master) and one for the source (branch):

```ts
const masterEditTxn = new EditTxn(masterDb, "reverse sync");
masterEditTxn.start();
const branchEditTxn = new EditTxn(branchDb, "reverse sync provenance");
branchEditTxn.start();

// source = branch, target = master; transformer auto-detects reverse sync from provenance
const transformer = new IModelTransformer(
  { source: branchDb, target: masterEditTxn },
  { sourceEditTxn: branchEditTxn }
);
await transformer.process();

masterEditTxn.end();
branchEditTxn.end();
```

The transformer detects reverse sync automatically based on provenance direction. Without `sourceEditTxn`, it throws at runtime.

## IModelImporter

The importer takes a single `EditTxn` and derives `targetDb` from it:

```ts
const importer = new IModelImporter(editTxn, options);
// importer.targetDb === editTxn.iModel
```

## TemplateModelCloner

Always an in-place operation (source === target). Takes a single `EditTxn`:

```ts
const cloner = new TemplateModelCloner(editTxn);
const idMap = await cloner.placeTemplate3d(templateModelId, targetModelId, placement);
```

## initializeBranchProvenance

Owns its own `EditTxn` internally — callers do not pass or manage one:

```ts
await initializeBranchProvenance({ master: masterDb, branch: branchDb });
```
