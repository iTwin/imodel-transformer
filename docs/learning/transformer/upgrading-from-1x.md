# Upgrading from imodel-transformer 1.x to 2.0

<!--
AGENT INSTRUCTIONS: migrate a codebase from @itwin/imodel-transformer 1.x to 2.x.
This block is hidden from rendered docs. Humans: the visible guide below has the same content.

Goal: the consumer compiles and passes its tests on @itwin/imodel-transformer 2.x with unchanged intent. Apply mechanical rewrites; report judgment calls instead of guessing.

1. Inventory before editing. Search the consumer for each pattern and record every hit with file and line:
   - Constructors: `new IModelTransformer(`, `new IModelImporter(`, `new TemplateModelCloner(`, `new IModelExporter(`.
   - Removed or renamed members: `onDeleteElement(`, `isForwardSynchronization`, `isReverseSynchronization`, `provenanceDb`, `provenanceSourceDb`, `synchronizationVersion`, `determineSyncType`, `noEsaSyncDirectionErrorMessage`, `forEachTrackedElement`, `initElementProvenanceOptions`, `initRelationshipProvenanceOptions`, `queryScopeExternalSourceAspect`, `detectElementDeletes`, `detectRelationshipDeletes`, `hasDefinitionContainerDeletionFeature`, `alignECEFLocations`, `ecefTransform`, `calculateEcefTransform(`, `IModelCloneContext`, `ChangedECInstance`, `addCustomAspectChange(`, `exportSchemas(`, `exportRelationship(`.
   - Deep imports: `@itwin/imodel-transformer/lib`, and any import path under the package other than the root and `/schema-processing`.
   - Error checks near transformer calls: `instanceof IModelError`, `errorNumber`, `.message` comparisons, `IModelStatus`.
   - Logging config containing the strings `core-backend.IModelExporter`, `core-backend.IModelImporter`, `core-backend.IModelTransformer`, `core-backend.IModelCloneContext`.
   - Writes and saves on the target or source IModelDb: `saveChanges(`, `abandonChanges(`.
   - Overrides of any method in the "Affected classes and methods" lists below, in subclasses of IModelExportHandler, IModelExporter, IModelImporter, IModelTransformer, or TemplateModelCloner.
   - Environment variable `SUGGEST_TRANSFORMER_VERSIONS`.
2. Dependencies: set iTwin.js core and ecschema packages to one 5.x version satisfying ^5.13.0, all equal. Add `@itwin/ecschema-editing` and `@itwin/ecschema-locaters` only if the code imports `@itwin/imodel-transformer/schema-processing`. Require Node ^22.12.0 or ^24.18.0.
3. Mechanical rewrites. Apply each exactly as the reference section below shows:
   - Constructors: see "EditTxn constructors". The caller creates and starts the EditTxn before construction and ends it after processing: `end("save")` on success and `end("abandon")` on failure, in a finally block. Reverse synchronization passes a started `sourceEditTxn` in options and ends both transactions with the same mode.
   - Async: make every override in the affected lists `async` returning `Promise`, and `await` every call to them. Replace `forEach` callbacks that call them with `for...of` plus `await`. Do not leave a floating promise.
   - Getters to methods: `x.isForwardSynchronization` becomes `await x.getIsForwardSynchronization()`, same for reverse; `x.provenanceDb` becomes `await x.getProvenanceDb()`; protected `this.synchronizationVersion` becomes `await this.getSynchronizationVersion()`.
   - `onDeleteElement(id)` overrides become `onDeleteElements(ids: ReadonlySet<Id64String>)` looping over the set, then `await super.onDeleteElements(ids)`. In an IModelImporter subclass, finish work that reads the elements before calling super.
   - `alignECEFLocations` becomes `tryAlignGeolocation`. `calculateEcefTransform(src, target)` becomes `calculateEcefTransform()`, which may return undefined.
   - `context` typed as IModelCloneContext becomes `IModelTransformContext`.
   - `ChangedInstanceIds.addChange` input: produce `ChangeInstance` values with `ChangesetReader` and `PartialChangeUnifier` as shown in "ChangedInstanceIds.addChange".
   - `addCustomAspectChange("Deleted", ...)` must pass the owning element IDs as the third argument.
   - Error checks for transformer-owned failures become `ITwinError.isError(error, IModelTransformerErrorScope, IModelTransformerError.<Key>)`. Map each old check to a key using the table in "Errors". Keep checks for errors from core, backend, or database unchanged.
   - Logger category strings: replace the `core-backend.` prefix with `imodel-transformer.` for the four categories above, or use the `TransformerLoggerCategory` members.
   - Deep imports: import from `@itwin/imodel-transformer` or `@itwin/imodel-transformer/schema-processing`.
   - Remove uses of `SUGGEST_TRANSFORMER_VERSIONS`.
4. Judgment calls. Do not auto-fix these; list each with file, line, and a recommended change for the human:
   - Overrides of `IModelExporter.exportSchemas()` meant to change transformer schema discovery: they no longer apply. Recommend `enumerateSchemas()` or a `SchemaProcessingStrategy`.
   - Overrides of `IModelExporter.exportRelationship()`: bulk export no longer calls it. Recommend `exportRelationshipInstance()`.
   - Removed static provenance helpers and `provenanceSourceDb`, `detectElementDeletes`, `detectRelationshipDeletes`, `determineSyncType`, `hasDefinitionContainerDeletionFeature`: no drop-in replacement. Recommend `getIsForwardSynchronization()`, `getIsReverseSynchronization()`, or the protected `_provenanceManager`, and explain what behavior would change.
   - Code that relied on `exportChanges()` falling back to `exportAll()` when there are no changesets: it now throws `NoChangesets`. Recommend an explicit `exportAll()` or `process()` without `argsForProcessChanges`.
   - Custom ElementAspect strategies, aspect cleanup code, custom schema import or dynamic schema merging, and retry loops around deletion: 2.x does this work. Recommend deletion and name the 2.x feature that replaces it.
   - Code that counts aspect write hooks, saves or reports progress per element assuming its aspects were processed with it, or assumes `onExportElement` sees unchanged elements during change processing.
   - Element filters that can accept an element while rejecting its category or another required element: these now throw `DependencyMappingMissing`.
   - Setup writes that run while the transformation's EditTxn would be active: only one EditTxn can be active per iModel.
5. Validate: type check, run the consumer's tests, and search again for every inventory pattern; each remaining hit must be intentional and explained. Report: files changed, rewrites applied by category, judgment calls with recommendations, and test results. Do not change public behavior of the consumer, suppress errors, or catch and ignore ITwinError to make tests pass.
-->

This guide takes code written for `@itwin/imodel-transformer` 1.x to 2.0. Work through the steps in order: the first five get your code compiling, and the last two remove code 2.0 makes unnecessary and catch the behavior changes the compiler can't. The steps follow a production service's migration from 1.x. Each step links to its reference section below.

## Step 1: Update Node and iTwin.js

2.0 requires Node `^22.12.0 || ^24.18.0` and iTwin.js `^5.13.0` peer dependencies. 1.2.0 required Node `^18.0.0` and iTwin.js `^4.3.5`. See [Node and peer dependencies](#node-and-peer-dependencies).

## Step 2: Own the target and source transactions

`IModelTransformer`, `IModelImporter`, and `TemplateModelCloner` require a started `EditTxn` on the target iModel instead of an `IModelDb`. Reverse synchronization also requires a started `sourceEditTxn`. The caller ends each transaction: save after success and abandon after failure.

Decide where each transaction starts and ends before changing call sites:

- Only one `EditTxn` can be active for an iModel, and `start()` throws if unsaved changes are present. Finish setup writes, such as creating the target scope element or seeding definitions, in their own transactions and end them before the transformation's transaction starts.
- End the transformation's transaction in a `finally` block, with `"save"` after success and `"abandon"` after failure, so a failed run never saves partial changes. End both transactions of a reverse synchronization with the same mode.
- Replace your own `saveChanges()` calls on the target `IModelDb`, such as periodic saves during a long run, with `editTxn.saveChanges()`. Push saved changes as a separate step after the transaction ends.

See [EditTxn constructors](#edittxn-constructors) and [EditTxn in imodel-transformer](../EditTxnInTransformer.md).

## Step 3: Make overrides and calls async

Await every call to a method that now returns a `Promise`, and make every override of one `async`. Watch loops: replace `forEach` callbacks that call these methods with `for...of` and `await`, so calls run in order and errors propagate. See [Async methods](#async-methods).

## Step 4: Replace removed and renamed APIs

- Element deletion: `onDeleteElement` is removed from `IModelExportHandler`, `IModelTransformer`, and `IModelImporter`; override `onDeleteElements` instead. See [Element deletion](#element-deletion).
- `IModelTransformer.context` is typed as `IModelTransformContext`, and several provenance APIs and getters were removed or became async methods. See [Context and provenance APIs](#context-and-provenance-apis) and [Removed and renamed APIs](#removed-and-renamed-apis).
- `alignECEFLocations` is replaced by `tryAlignGeolocation`. See [Geolocation alignment](#geolocation-alignment).
- `ChangedInstanceIds.addChange` takes a `ChangeInstance` instead of `ChangedECInstance`. See [ChangedInstanceIds.addChange](#changedinstanceidsaddchange).
- The package `exports` map removes undocumented deep imports, such as paths under `lib/cjs`. Import from `@itwin/imodel-transformer` or `@itwin/imodel-transformer/schema-processing`. See [Schema processing and package exports](#schema-processing-and-package-exports).
- `TransformerLoggerCategory` values changed from `core-backend.*` to `imodel-transformer.*`. See [Logger categories](#logger-categories).

## Step 5: Update error handling

Replace `instanceof IModelError`, `errorNumber`, and message checks for transformer-owned failures with `ITwinError.isError` and an `IModelTransformerError` key. `exportChanges()`, and `process()` with `argsForProcessChanges`, now throw `no-changesets` instead of running a full export when the source has no changesets and no custom changes. See [Errors](#errors) and [exportChanges() without changesets](#exportchanges-without-changesets).

## Step 6: Delete code that 2.0 makes obsolete

2.0 takes over work that 1.x consumers often did themselves. Look for these and remove them:

- Custom ElementAspect export strategies or importer shims that worked around delete-and-reinsert aspect processing. `IModelExporter` no longer accepts an aspect-processing strategy, and the importer now reconciles aspects itself. See [ElementAspects](#elementaspects).
- Custom schema enumeration, ordering, import, or dynamic-schema merging. Use `processSchemas({ strategy })` with `NewerVersionSchemaImportStrategy` or `DynamicSchemaUnionStrategy`, and subclass a strategy for custom policy. See [Schema processing in a transformation](./schema-processing.md).
- Per-element deletion overrides that retried or skipped elements still in use. The importer now keeps them with a warning. See [Elements that are still in use](./element-deletion.md#elements-that-are-still-in-use).
- Local patches for defects fixed since 1.2.0, such as changes stored in overflow tables or models deleted after their partition element was remapped. The 2.0 release notes list the fixes.

## Step 7: Re-test for behavior changes

Run your tests against real data and check these, because none of them fails to compile:

- **Filters.** A filter that accepts elements must also accept their categories and other required elements, or the transform throws `DependencyMappingMissing`. See [Element filtering and required elements](#element-filtering-and-required-elements).
- **ElementAspect callbacks.** Aspect callbacks no longer run beside their owning element's callback, and `onInsertElementAspect`, `onUpdateElementAspect`, and `onDeleteElementAspect` run only for actual writes, so a rerun with no source changes calls none of them. Code that saves or reports progress per element and assumes its aspects were processed with it, or that counts aspect hooks, must change. See [ElementAspects](#elementaspects).
- **Shared target elements.** When elements from two sources map to the same target element, each transformation reconciles that element's aspects against its own source only, so it deletes the aspects the other source added. See [Processing ElementAspects](./element-aspect-processing.md).
- **Exporter subclasses.** `exportRelationships()` no longer calls `exportRelationship()` per relationship, and overriding `exportElement` or `exportChildElements` opts out of the faster traversal. See [IModelExporter subclasses](#imodelexporter-subclasses).
- **Change processing.** Unchanged elements are no longer exported during change processing, and filters run on unchanged ancestors only when a changed descendant is reached. See [Incremental exports](./index.md#incremental-exports).
- **Deletion.** Elements still in use are kept with a warning, and a failed native deletion throws `ElementBulkDeleteFailed`. See [Deleting elements](./element-deletion.md).

## Reference

### Node and peer dependencies

| Requirement                | 1.2.0     | 2.0                      |
| -------------------------- | --------- | ------------------------ |
| Node                       | `^18.0.0` | `^22.12.0 \|\| ^24.18.0` |
| iTwin.js peer dependencies | `^4.3.5`  | `^5.13.0`                |

The iTwin.js peer dependencies are `@itwin/core-backend`, `@itwin/core-bentley`, `@itwin/core-common`, `@itwin/core-geometry`, `@itwin/core-quantity`, and `@itwin/ecschema-metadata`. `@itwin/ecschema-editing` and `@itwin/ecschema-locaters` are optional and are needed only for the `schema-processing` subpath; see [Schema processing and package exports](#schema-processing-and-package-exports).

The `SUGGEST_TRANSFORMER_VERSIONS` environment variable, which looked up compatible transformer versions when the peer dependency check failed, and the README version-selection script are removed. `TRANSFORMER_NO_STRICT_DEP_CHECK` is unchanged.

### EditTxn constructors

`IModelTransformer`, `IModelImporter`, and `TemplateModelCloner` constructors now require an explicit [`EditTxn`](https://www.itwinjs.org/reference/core-backend/imodels/edittxn/) from `@itwin/core-backend` for the target iModel. This aligns the transformer with the iTwin.js platform's move toward explicit edit transactions and eliminates the possibility of mismatched db/txn references.

For detailed usage patterns and lifecycle guidance, see the [EditTxn in Transformer learning doc](../EditTxnInTransformer.md).

#### `IModelTransformer`

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

#### `IModelImporter`

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

#### `TemplateModelCloner`

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

#### `initializeBranchProvenance`

No changes to the call signature. The function now uses an `EditTxn` internally, but this is transparent to callers:

```ts
await initializeBranchProvenance({ master, branch: branchDb });
// No migration needed — works the same as before.
```

### Async methods

Many previously synchronous methods across the public API now return `Promise` and must be `await`ed. If you override any of these methods in a subclass, your override must also be declared `async` (or return a `Promise`).

#### 1. Add `async` to overrides

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

#### 2. Await calls to these methods

```ts
// Before (v1)
importer.importElement(elementProps);

// After (v2)
await importer.importElement(elementProps);
```

#### 3. Replace the synchronization property accessors

The `isForwardSynchronization` and `isReverseSynchronization` getters on `IModelTransformer` have been replaced with async methods `getIsForwardSynchronization()` and `getIsReverseSynchronization()`:

```ts
// Before (v1)
if (transformer.isForwardSynchronization) { ... }

// After (v2)
if (await transformer.getIsForwardSynchronization()) { ... }
```

#### Affected classes and methods

`onDeleteElement` is not listed because it was removed; see [Element deletion](#element-deletion).

##### IModelExportHandler

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

##### IModelExporter

- `shouldExportElement`

##### IModelImporter

- `deleteElement`
- `deleteModel`
- `deleteRelationship`
- `importElement`
- `importElementMultiAspects`
- `importElementUniqueAspect`
- `importModel`
- `importRelationship`
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

##### IModelTransformer

- `completePartiallyCommittedAspects`
- `completePartiallyCommittedElements`
- `getIsForwardSynchronization`
- `getIsReverseSynchronization`
- `getProvenanceDb`
- `getSynchronizationVersion`
- `initElementProvenance`
- `initScopeProvenance`
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
- `updateSynchronizationVersion`

##### TemplateModelCloner

- `onTransformElement`

### Element deletion

Incremental synchronization now deletes elements in one batch. `IModelExporter.exportChanges()` passes all deleted source IDs to `IModelExportHandler.onDeleteElements()`, `IModelTransformer` maps them to target IDs, and `IModelImporter.deleteElements()` deletes the target elements through the native bulk-delete API. Children, sub-model contents, and elements whose code is scoped by a deleted element are still deleted with it.

These callbacks have been removed:

- `IModelExportHandler.onDeleteElement()`
- `IModelTransformer.onDeleteElement()`
- the protected `IModelImporter.onDeleteElement()` hook

Move per-element logic to `onDeleteElements(elementIds: ReadonlySet<Id64String>)`, which receives every deleted ID:

```ts
public override async onDeleteElements(
  sourceElementIds: ReadonlySet<Id64String>
): Promise<void> {
  for (const sourceElementId of sourceElementIds)
    this.recordDeletion(sourceElementId);
  await super.onDeleteElements(sourceElementIds);
}
```

A custom importer's override receives target IDs. `super.onDeleteElements()` does the deletion, so finish any work that reads the elements before calling it:

```ts
protected override async onDeleteElements(
  targetElementIds: ReadonlySet<Id64String>
): Promise<void> {
  for (const targetElementId of targetElementIds)
    this.insertDeleteAuditRecord(targetElementId);
  await super.onDeleteElements(targetElementIds);
}
```

Calls to the public `IModelImporter.deleteElement(elementId)` don't need to change.

If an element outside the batch still uses an element in it, such as through its category or code scope, the importer keeps the used element, deletes the rest, and logs a warning. See [Elements that are still in use](./element-deletion.md#elements-that-are-still-in-use).

If a native deletion call fails anyway, for example because geometry outside the batch uses a geometry part in it, or because of a reference from a domain schema, `IModelImporter.deleteElements()` throws an `ElementBulkDeleteError` with scope `IModelTransformerErrorScope` and key `IModelTransformerError.ElementBulkDeleteFailed`. Its `status`, `sqlDeleteStatus`, and `failedIds` describe the failed call. Deletions from that call and earlier ones are still pending in the caller's target transaction, so abandon the transaction before fixing the dependency and retrying.

See [Deleting elements](./element-deletion.md) for details and an error-handling example.

### Errors

Errors detected and owned by `@itwin/imodel-transformer` now use `ITwinError` with scope `@itwin/imodel-transformer` and a key from `IModelTransformerError`. These errors previously used a mix of `IModelError` and plain `Error`.

| Previous type | `IModelTransformerError` keys                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `IModelError` | `ExportChangesRequiresBriefcase`, `InvalidModelId`, `TargetClassNotFound`, `ElementIdRequired`, `RelationshipIdRequired`, `InvalidSubCategory`, `GeolocationUnavailable`, `GeographicCoordinateSystemUnavailable`, `GeographicCoordinateSystemMismatch`, `DanglingReference`, `RootSubjectNotProcessable`, `ParentModelRequired`, `DependencyMappingMissing`, `ProvenanceSchemaUnsupported`, `ProvenanceScopeConflict`                                     |
| `Error`       | `SchemaLoadFailed`, `ExportHandlerNotRegistered`, `ChangedInstanceMetadataMissing`, `InvalidEntityReference`, `ImporterOptionMismatch`, `InvalidCode`, `ElementIdNotPreservable`, `SynchronizationRangeInvalid`, `EditTxnNotActive`, `ChangesetIndexUnavailable`, `RelationshipClassNotFound`, `SourceEditTxnRequired`, `SynchronizationVersionMissing`, `RelationshipProvenanceNotFound`, `SynchronizationTypeNotDetermined`, `DependencyVersionMismatch` |

These keys are new failure conditions rather than migrations from an existing thrown error: `NoChangesets` (see [exportChanges() without changesets](#exportchanges-without-changesets)), `ElementBulkDeleteFailed` (see [Element deletion](#element-deletion)), `AspectOwnerRequired` (see [ElementAspects](#elementaspects)), `SchemaConflict` and `SchemaDependencyCycle` (see [Schema processing in a transformation](./schema-processing.md#handle-schema-processing-errors)), and `InvalidRelationshipData`, thrown when a relationship query returns malformed instance data.

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

See [Error handling in imodel-transformer](./error-handling.md) for the complete ownership and handling rules.

### exportChanges() without changesets

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

### Context and provenance APIs

#### `IModelTransformer.context`

`IModelTransformer.context` is now typed as `IModelTransformContext`. The interface supports target lookup, explicit mappings for elements, element aspects, element classes, and CodeSpecs, and SubCategory filtering. Cloning operations, source and target database access, native resource management, context persistence, and other `IModelCloneContext` implementation details are no longer accessible through the public property.

Continue to obtain the context from an `IModelTransformer`:

```ts
import type { IModelTransformContext } from "@itwin/imodel-transformer";

const context: IModelTransformContext = transformer.context;
const targetElementId = context.findTargetElementId(sourceElementId);
```

Tests should mock `IModelTransformContext` instead of constructing or stubbing `IModelCloneContext` directly.

#### Provenance APIs

As part of [the decomposition of `IModelTransformer`](https://github.com/iTwin/imodel-transformer/pull/295), synchronization direction resolution and provenance management were moved into focused internal classes. Most commonly used `IModelTransformer` APIs remain available, including `initElementProvenance()`, `getSynchronizationVersion()`, `tryGetProvenanceScopeAspect()`, `initScopeProvenance()`, and `updateSynchronizationVersion()`, but they are now async.

Several provenance APIs were removed or became async methods; see [Removed and renamed APIs](#removed-and-renamed-apis). Subclasses that need the extracted provenance functionality can use the protected `_provenanceManager`. To determine synchronization direction, use `getIsForwardSynchronization()` or `getIsReverseSynchronization()`.

### Removed and renamed APIs

| 1.x API                                                                                                                                           | 2.0 replacement                                                                                                                                              |
| ------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `new IModelTransformer(source, target, options)`                                                                                                  | `new IModelTransformer({ source, target }, options)`, where `target` is an `EditTxn` or `IModelImporter`. See [EditTxn constructors](#edittxn-constructors). |
| `new IModelImporter(targetDb, options)`                                                                                                           | `new IModelImporter(editTxn, options)`                                                                                                                       |
| `new TemplateModelCloner(sourceDb, targetDb?)`                                                                                                    | `new TemplateModelCloner(editTxn)`. Cross-iModel cloning is removed.                                                                                         |
| `new IModelExporter(sourceDb, elementAspectsStrategy?)`                                                                                           | `new IModelExporter(sourceDb)`. See [ElementAspects](#elementaspects).                                                                                       |
| `IModelTransformer.context: IModelCloneContext`                                                                                                   | `IModelTransformContext`. See [Context and provenance APIs](#context-and-provenance-apis).                                                                   |
| `isForwardSynchronization`, `isReverseSynchronization` getters                                                                                    | `await getIsForwardSynchronization()`, `await getIsReverseSynchronization()`                                                                                 |
| `provenanceDb` getter                                                                                                                             | `await getProvenanceDb()`                                                                                                                                    |
| `provenanceSourceDb` getter                                                                                                                       | Removed.                                                                                                                                                     |
| Protected `synchronizationVersion` getter                                                                                                         | Protected `await getSynchronizationVersion()`                                                                                                                |
| Static `determineSyncType()` and `noEsaSyncDirectionErrorMessage`                                                                                 | Removed. Use `getIsForwardSynchronization()` or `getIsReverseSynchronization()`.                                                                             |
| Static `forEachTrackedElement()`, `initElementProvenanceOptions()`, `initRelationshipProvenanceOptions()`, and `queryScopeExternalSourceAspect()` | Removed. Subclasses can use the protected `_provenanceManager`.                                                                                              |
| `detectElementDeletes()`, `detectRelationshipDeletes()`                                                                                           | Removed. Both were deprecated in 1.x.                                                                                                                        |
| Protected `hasDefinitionContainerDeletionFeature` getter                                                                                          | Removed.                                                                                                                                                     |
| `onDeleteElement()` on `IModelExportHandler`, `IModelTransformer`, and `IModelImporter`                                                           | `onDeleteElements()`. See [Element deletion](#element-deletion).                                                                                             |
| `IModelTransformOptions.alignECEFLocations`, `ecefTransform`, `calculateEcefTransform(srcDb, targetDb)`                                           | `tryAlignGeolocation` and `calculateEcefTransform()`. See [Geolocation alignment](#geolocation-alignment).                                                   |
| `ChangedInstanceIds.addChange(change: ChangedECInstance)`                                                                                         | `addChange(change: ChangeInstance)`. See [ChangedInstanceIds.addChange](#changedinstanceidsaddchange).                                                       |
| Deep imports such as `@itwin/imodel-transformer/lib/cjs/...`                                                                                      | Import from `@itwin/imodel-transformer` or `@itwin/imodel-transformer/schema-processing`.                                                                    |
| `SUGGEST_TRANSFORMER_VERSIONS` environment variable                                                                                               | Removed. See [Node and peer dependencies](#node-and-peer-dependencies).                                                                                      |

`getProvenanceSourceDb()`, added in earlier 2.0 development builds, is also removed.

### Geolocation alignment

`IModelTransformOptions.alignECEFLocations` is replaced by `tryAlignGeolocation`. Like `alignECEFLocations`, it is off by default and aligns iModels whose ECEF locations differ. It also aligns iModels that have a geographic coordinate system with the same CRS but different `additionalTransform`s, and throws `ITwinError` when the iModels can't be aligned.

The public `ecefTransform` property is removed. `calculateEcefTransform()` no longer takes arguments; it uses the transformer's source and target iModels, returns `Transform | undefined`, and throws `GeolocationUnavailable` when either ECEF location is missing.

See [Aligning geolocation](./index.md#aligning-geolocation) for the alignment rules and errors.

### Logger categories

`TransformerLoggerCategory` values now use the `imodel-transformer` prefix instead of `core-backend`. Code that refers to the enum members needs no change. Logging configuration that uses the strings must be updated:

| Member               | 1.x value                         | 2.0 value                               |
| -------------------- | --------------------------------- | --------------------------------------- |
| `IModelExporter`     | `core-backend.IModelExporter`     | `imodel-transformer.IModelExporter`     |
| `IModelImporter`     | `core-backend.IModelImporter`     | `imodel-transformer.IModelImporter`     |
| `IModelTransformer`  | `core-backend.IModelTransformer`  | `imodel-transformer.IModelTransformer`  |
| `IModelCloneContext` | `core-backend.IModelCloneContext` | `imodel-transformer.IModelCloneContext` |

A new `ECReferenceTypesCache` member uses `imodel-transformer.ECReferenceTypesCache`.

### IModelExporter subclasses

`IModelExporter.exportRelationships()` now reads relationships with one streaming query and no longer calls `exportRelationship()` once per relationship. Both paths call the new protected `exportRelationshipInstance()`, which applies exclusions and calls the handler. Move per-relationship logic from an `exportRelationship()` override to `exportRelationshipInstance()`, or to the handler's `shouldExportRelationship()` or `onExportRelationship()`.

Subclasses that override `exportElement` or `exportChildElements` keep the previous per-element traversal, so those overrides continue to receive every element, but they don't get the faster traversal described in [Incremental exports](./index.md#incremental-exports).

### ChangedInstanceIds.addChange

`ChangedInstanceIds.addChange` now accepts a [`ChangeInstance`](https://www.itwinjs.org/reference/core-backend/ecdb/changeinstance/) from the new iTwin.js changeset APIs instead of the deprecated `ChangedECInstance`.

If you call `addChange` directly, use `ChangesetReader` and `PartialChangeUnifier` to produce the input:

```ts
using reader = ChangesetReader.openFile({ db, fileName: changesetPath });
using unifier = new PartialChangeUnifier();

while (reader.step()) {
  unifier.appendFrom(reader);
}

for (const change of unifier.instances) {
  await changedInstanceIds.addChange(change);
}
```

### Element filtering and required elements

A changed element can require an unchanged element that has no mapping in the target, such as its category. `IModelTransformer` does not insert unchanged elements, so it now throws `ITwinError` with key `DependencyMappingMissing` when it cannot map one. Previously, a required parent that was missing from the target could make change processing recurse until the process ran out of memory.

Full transforms such as `processAll()` and change processing also throw this error when an accepted element requires an element that the filter rejects, directly or through an ancestor. Previously, native cloning copied such a category into the target anyway, with a new FederationGuid. A filter that accepts elements must now also accept their categories.

See [Filtering and required elements](./index.md#filtering-and-required-elements) for when this error occurs and how to fix it.

### Schema processing and package exports

`IModelExporter.enumerateSchemas()` is now the schema-discovery extension point used by both `exportSchemas()` and transformer schema processing. Overrides of `exportSchemas()` continue to affect direct exporter calls but no longer control transformer schema discovery. Override `enumerateSchemas()` instead.

Applications using the optional schema-processing subpath must provide compatible `@itwin/ecschema-editing` and `@itwin/ecschema-locaters` peer dependencies. The new package `exports` map exposes the root package, `schema-processing`, and `package.json`; undocumented deep imports are no longer supported.

See [Schema processing in a transformation](./schema-processing.md) for strategy selection and extension points.

### ElementAspects

In 2.x, ElementAspects are exported separately from element callbacks using bounded, owner-scoped groups. The constructor no longer accepts an aspect-processing selector, and the previous implementation that exported aspects beside their owning elements is removed.

Existing `IModelExportHandler` callbacks and `shouldExportElementAspect` remain available. `IModelExporter` also continues to support `excludeElementAspectClass`. These callbacks retain their filtering and export roles, but aspect callbacks are no longer guaranteed to run next to the callback for their owning element.

For each accepted owner, the transformer reconciles replaceable target aspects with the current source aspects; unchanged aspects keep their IDs, changed ones are updated, new ones inserted, and the rest deleted; see [Processing ElementAspects](./element-aspect-processing.md). Excluded aspect classes and transformer provenance aspects are preserved, with one exception: an included unique aspect replaces an excluded unique aspect of a base or derived class on the same element, because iModels store them in one slot.

#### Custom deleted aspect changes need the owner ID

Custom inserted or updated aspect changes infer the owner while the source aspect exists. Custom deleted or missing aspects require the owning element ID and throw when it is omitted:

```ts
changedInstanceIds.addCustomAspectChange(
  "Deleted",
  deletedAspectId,
  owningElementId
);
```

For the processing entry points, Exporter/Transformer/Importer boundaries, workflow diagram, filtering, batching, and custom-change examples, see the [Processing ElementAspects learning guide](./element-aspect-processing.md).

#### Direct `IModelImporter` aspect calls match the exact class

`IModelImporter.importElementUniqueAspect` and `importElementMultiAspects` read existing target aspects with `getAspects`, which also returns aspects of derived classes. In 1.x, the importer used those derived-class aspects as matches. That could overwrite a derived-class aspect with base-class properties, or delete it as a surplus aspect. The importer now matches, updates, and deletes only existing aspects of exactly the class named by `classFullName`.

| Call                                                                                                             | Before                                                                                                                                                    | Now                                                                                                         |
| ---------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `importElementUniqueAspect` for class `A` when the owner has a unique aspect of derived class `B`                | Treated the `B` aspect as the match and returned its ID. If properties differed, called `onUpdateElementAspect` with the `B` aspect ID and the `A` props. | Deletes the `B` aspect through `onDeleteElementAspect`, then inserts the `A` aspect and returns its new ID. |
| `importElementUniqueAspect` for class `B` when the owner has a unique aspect of base class `A`                   | Inserted `B`, leaving both aspects.                                                                                                                       | Deletes the `A` aspect through `onDeleteElementAspect`, then inserts the `B` aspect.                        |
| `importElementMultiAspects` with aspects of class `A` when the owner also has multi-aspects of derived class `B` | Matched `A` and `B` target aspects in order, so a `B` aspect could be updated with `A` properties or deleted as surplus.                                  | Matches only `A` target aspects. `B` aspects are changed only by props whose `classFullName` is `B`.        |

The unique-aspect deletion happens because iModel unique-aspect writes treat a class and its base or derived classes as one slot for an element. Inserting `A` replaces an existing `B`, and deleting `A` also deletes `B`. Deleting the related aspect first runs your deletion hook and keeps later deletes from removing the new aspect.

To migrate code that calls these methods directly:

- Pass each aspect with its concrete `classFullName`. Props read from an iModel with `getAspects` or `getAspect` already have it.
- If you relied on a base-class `importElementMultiAspects` call deleting derived-class aspects, include those aspects in the call under their own class, or delete them with `EditTxn.deleteAspect`.
- If you track IDs returned by `importElementUniqueAspect`, expect a new ID when an owner's unique aspect changes between a base and a derived class, and expect `onDeleteElementAspect` to run for the aspect it replaces.

`IModelTransformer` already passes concrete classes, so transforms need no changes. See [Calling the importer directly](./element-aspect-processing.md#calling-the-importer-directly).
