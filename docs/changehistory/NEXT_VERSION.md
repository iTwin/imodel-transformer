# Next release notes

## Breaking change: `IModelTransformer` no longer extends `IModelExportHandler`

`IModelTransformer` now registers a dedicated internal `IModelExportHandler` with its exporter instead of acting as the export handler itself. Existing transformer subclass overrides such as `shouldExportElement()`, `onExportElement()`, `onProgress()`, and `onSkipElement()` continue to be invoked with the same behavior.

Code that treated an `IModelTransformer` instance as an `IModelExportHandler` must use a separate handler for direct `IModelExporter` workflows:

```ts
const transformer = new IModelTransformer({
  source: sourceDb,
  target: targetEditTxn,
});

// The transformer configures its own internal export handler.
await transformer.process();

// Direct exporter workflows require their own handler.
const exporter = new IModelExporter(sourceDb);
exporter.registerHandler(myExportHandler);
await exporter.exportAll();
```

The `IModelTransformer.exporter` property and support for passing a preconfigured exporter as `IModelTransformArgs.source` are unchanged.

## Breaking change: transformer errors now have stable identifiers

Errors detected and owned by `@itwin/imodel-transformer` now use `ITwinError` with scope `@itwin/imodel-transformer` and a key from `IModelTransformerError`. These errors previously used a mix of `IModelError` and plain `Error`.

| Previous type | `IModelTransformerError` keys                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `IModelError` | `ExportChangesRequiresBriefcase`, `InvalidModelId`, `TargetClassNotFound`, `ElementIdRequired`, `RelationshipIdRequired`, `InvalidSubCategory`, `GeolocationUnavailable`, `GeographicCoordinateSystemUnavailable`, `GeographicCoordinateSystemMismatch`, `DanglingReference`, `RootSubjectNotProcessable`, `ParentModelRequired`, `DependencyMappingMissing`, `ProvenanceSchemaUnsupported`, `ProvenanceScopeConflict`                                     |
| `Error`       | `SchemaLoadFailed`, `ExportHandlerNotRegistered`, `ChangedInstanceMetadataMissing`, `InvalidEntityReference`, `ImporterOptionMismatch`, `InvalidCode`, `ElementIdNotPreservable`, `SynchronizationRangeInvalid`, `EditTxnNotActive`, `ChangesetIndexUnavailable`, `RelationshipClassNotFound`, `SourceEditTxnRequired`, `SynchronizationVersionMissing`, `RelationshipProvenanceNotFound`, `SynchronizationTypeNotDetermined`, `DependencyVersionMismatch` |

`NoChangesets` is a new failure condition rather than a migration from an existing thrown error. It is covered separately below.

Consumers that check `instanceof IModelError`, inspect `errorNumber`, or compare an error message for a transformer-owned condition must switch to `ITwinError.isError`. Check both the exported `IModelTransformerErrorScope` and the expected enum member:

```ts
import { ITwinError } from "@itwin/core-bentley";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "@itwin/imodel-transformer";

try {
  await transformer.process();
} catch (error) {
  if (
    ITwinError.isError(
      error,
      IModelTransformerErrorScope,
      IModelTransformerError.DanglingReference
    )
  ) {
    // Correct the source reference or choose a different policy before retrying.
    return;
  }
  throw error;
}
```

Errors originating from iTwin.js core, the backend, or the database retain their original type and status. When the transformer translates an upstream failure into a package-owned condition, the new error retains the upstream error as `cause`.

See [Error handling in imodel-transformer](../learning/transformer/error-handling.md) for the complete ownership and handling rules.

## Breaking change: `exportChanges()` no longer falls back to `exportAll()`

`IModelExporter.exportChanges()` no longer calls `exportAll()` when the source briefcase has no changesets and no custom changes. It now throws an `ITwinError` with scope `@itwin/imodel-transformer` and key `no-changesets`. `IModelTransformer.process()` propagates this error when `argsForProcessChanges` is specified, before finalizing the transformation or updating its synchronization version.

This makes change processing predictable and prevents configuration errors from silently running a full export or completing an empty transformation. Briefcases with changesets, including changesets without relevant instance changes, and workflows that supply custom changes are unaffected.

If the source briefcase has no changesets and you intend to transform all content, call the explicit full-processing API instead. For `IModelTransformer`, omitting `argsForProcessChanges` makes `process()` call `processAll()` and transform all content; supplying it makes `process()` call `processChanges()` for incremental processing.

```ts
// Direct exporter usage
await exporter.exportAll();

// IModelTransformer usage: omit argsForProcessChanges
const transformer = new IModelTransformer({
  source: sourceDb,
  target: targetEditTxn,
});
await transformer.process();
```

## Breaking changes: EditTxn-based constructors

`IModelTransformer`, `IModelImporter`, and `TemplateModelCloner` constructors now require an explicit [`EditTxn`](https://www.itwinjs.org/reference/core-backend/imodels/edittxn/) from `@itwin/core-backend`from the target iModel. This aligns the transformer with the iTwin.js platform's move toward explicit edit transactions and eliminates the possibility of mismatched db/txn references.

For detailed usage patterns and lifecycle guidance, see the [EditTxn in Transformer learning doc](../learning/EditTxnInTransformer.md).

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

#### Reverse sync

Reverse synchronization now requires a `sourceEditTxn` in `IModelTransformOptions`. Without it, the transformer throws at runtime.

**Before:**

```ts
// source = branch, target = master; reverse sync auto-detected from provenance
const transformer = new IModelTransformer(branchDb, masterDb, {
  argsForProcessChanges: {},
});
```

**After:**

```ts
// sourceEditTxn needed so provenance can be written back to the branch
const transformer = new IModelTransformer(
  { source: branchDb, target: masterEditTxn },
  { sourceEditTxn: branchEditTxn, argsForProcessChanges: {} }
);
```

### `IModelImporter`

The `targetDb` parameter has been removed. The importer now derives it from the `EditTxn`.

**Before:**

```ts
const importer = new IModelImporter(targetDb, options);
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
```

**After:**

```ts
const editTxn = new EditTxn(db, "clone templates");
editTxn.start();
const cloner = new TemplateModelCloner(editTxn);
await cloner.placeTemplate3d(templateModelId, targetModelId, placement);
editTxn.end();
```

> **Note:** The previous optional `targetDb` parameter (which allowed cross-db cloning) has been removed. `TemplateModelCloner` now only supports in-place cloning within `editTxn.iModel`, which was always the documented intent. If you previously passed a separate `targetDb`, use `IModelTransformer` directly instead.

### `initializeBranchProvenance`

No changes to the call signature. The function now uses an `EditTxn` internally, but this is transparent to callers:

```ts
await initializeBranchProvenance({ master, branch: branchDb });
// No migration needed — works the same as before.
```

## Breaking changes: `IModelTransformer` provenance APIs reorganized

As part of [the decomposition of `IModelTransformer`](https://github.com/iTwin/imodel-transformer/pull/295), synchronization direction resolution and provenance management were moved into focused internal classes. Most commonly used `IModelTransformer` APIs remain available, including `initElementProvenance()`, `getSynchronizationVersion()`, `tryGetProvenanceScopeAspect()`, `initScopeProvenance()`, and `updateSynchronizationVersion()`.

The following APIs were removed from `IModelTransformer`:

- `determineSyncType()`
- `noEsaSyncDirectionErrorMessage`
- `getProvenanceSourceDb()`
- `forEachTrackedElement()`
- `initElementProvenanceOptions()`
- `initRelationshipProvenanceOptions()`
- `queryScopeExternalSourceAspect()`

Subclasses that need the extracted provenance functionality can use the protected `_provenanceManager`. To determine synchronization direction, use `getIsForwardSynchronization()` or `getIsReverseSynchronization()`.

## Breaking changes: Many synchronous methods are now asynchronous

As part of the upgrade to iTwin.js 5.0, a large number of previously synchronous methods across the public API now return `Promise` and must be `await`ed. If you override any of these methods in a subclass, your override must also be declared `async` (or return a `Promise`).

Affected classes and methods:

#### IModelExportHandler

- `onDeleteElement`
- `onDeleteModel`
- `onDeleteRelationship`
- `onExportCodeSpec`
- `onExportElement`
- `onExportElementMultiAspects`
- `onExportElementUniqueAspect`
- `onExportFont`
- `onExportModel`
- `onExportRelationship`
- `onSkipElement`
- `shouldExportCodeSpec`
- `shouldExportElement`
- `shouldExportElementAspect`
- `shouldExportRelationship`
- `shouldExportSchema`

#### IModelExporter

- `shouldExportElement`

#### IModelImporter

- `deleteElement`
- `deleteModel`
- `deleteRelationship`
- `importElement`
- `importElementMultiAspects`
- `importElementUniqueAspect`
- `importModel`
- `importRelationship`
- `onDeleteElement`
- `onDeleteElementAspect`
- `onDeleteModel`
- `onDeleteRelationship`
- `onInsertElement`
- `onInsertElementAspect`
- `onInsertModel`
- `onInsertRelationship`
- `onProgress`
- `onUpdateElement`
- `onUpdateElementAspect`
- `onUpdateModel`
- `onUpdateRelationship`

#### IModelTransformer

- `completePartiallyCommittedAspects`
- `completePartiallyCommittedElements`
- `getIsForwardSynchronization`
- `getIsReverseSynchronization`
- `getProvenanceDb`
- `getSynchronizationVersion`
- `initElementProvenance`
- `initScopeProvenance`
- `onDeleteElement`
- `onDeleteModel`
- `onDeleteRelationship`
- `onExportCodeSpec`
- `onExportElement`
- `onExportElementMultiAspects`
- `onExportElementUniqueAspect`
- `onExportFont`
- `onExportModel`
- `onExportRelationship`
- `onTransformElement`
- `onTransformElementAspect`
- `shouldDetectDeletes`
- `shouldExportCodeSpec`
- `shouldExportElement`
- `shouldExportElementAspect`
- `shouldExportRelationship`
- `shouldExportSchema`
- `tryGetProvenanceScopeAspect`

#### TemplateModelCloner

- `onTransformElement`

### Synchronization property accessors replaced with async methods

The `isForwardSynchronization` and `isReverseSynchronization` getters on `IModelTransformer` have been replaced with async methods `getIsForwardSynchronization()` and `getIsReverseSynchronization()`.

### Requires iTwin.js 5.0 peer dependencies

This version requires iTwin.js 5.0 packages as peer dependencies.

## How to migrate

### 1. Add `async` to overrides

If you override any of the above methods, add the `async` keyword:

```ts
// Before (v1)
protected onInsertElement(elementProps: ElementProps): Id64String {
  // custom logic
  return super.onInsertElement(elementProps);
}

// After (v2)
protected async onInsertElement(elementProps: ElementProps): Promise<Id64String> {
  // custom logic
  return super.onInsertElement(elementProps);
}
```

### 2. Await calls to these methods

Any direct calls to the now-async methods must be awaited:

```ts
// Before (v1)
importer.importElement(elementProps);

// After (v2)
await importer.importElement(elementProps);
```

### 3. Synchronization property accessors replaced with async methods

The `isForwardSynchronization` and `isReverseSynchronization` getters have been replaced with async methods:

```ts
// Before (v1)
if (transformer.isForwardSynchronization) { ... }

// After (v2)
if (await transformer.getIsForwardSynchronization()) { ... }
```
