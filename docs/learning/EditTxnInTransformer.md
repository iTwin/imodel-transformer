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
  { sourceEditTxn: branchEditTxn, argsForProcessChanges: {} }
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

## Error handling: when to use try/catch

The transformer **never** calls `editTxn.end()` — the caller owns that. Since `end()` defaults to `"save"`, the simplest correct usage is:

```ts
const editTxn = createStartedEditTxn(targetDb);
const transformer = new IModelTransformer({ source: sourceDb, target: editTxn });
await transformer.process();
editTxn.end(); // defaults to "save"
```

If `process()` throws, the error propagates and the transaction is cleaned up when the db closes. This is fine for most cases, including tests and short-lived databases.

### When try/catch IS needed: rollback on failure

Use try/catch/finally when you need to **explicitly abandon** a failed transaction — typically in production code where a partial save would cause problems (e.g. before pushing to iModelHub):

```ts
const editTxn = createStartedEditTxn(targetDb);
let succeeded = false;
try {
  const transformer = new IModelTransformer({ source: sourceDb, target: editTxn });
  await transformer.process();
  succeeded = true;
} finally {
  editTxn.end(succeeded ? "save" : "abandon");
}
```

This ensures the target db is not left in a partially written state before a push.

### Reverse sync: two transactions

Reverse sync manages two `EditTxn`s. In production, end both in a finally block:

```ts
const masterEditTxn = createStartedEditTxn(masterDb);
const branchEditTxn = createStartedEditTxn(branchDb);
let succeeded = false;
try {
  const transformer = new IModelTransformer(
    { source: branchDb, target: masterEditTxn },
    { sourceEditTxn: branchEditTxn, argsForProcessChanges: {} }
  );
  await transformer.process();
  succeeded = true;
} finally {
  masterEditTxn.end(succeeded ? "save" : "abandon");
  branchEditTxn.end(succeeded ? "save" : "abandon");
}
```

For tests or cases where abandoning is unnecessary, you can simply call `end()` on both after `process()`.

### Summary

| Pattern | Try/catch needed? | Why |
|---|---|---|
| `createStartedEditTxn` + transformer (tests / discardable dbs) | No | Just call `end()` after; db close handles cleanup on failure |
| `createStartedEditTxn` + transformer (production / before push) | Recommended | Use `end("abandon")` on failure to avoid pushing partial state |
| Reverse sync (production) | Recommended | Both transactions should be abandoned together on failure |
| `initializeBranchProvenance` | No | Manages its own transaction internally |
