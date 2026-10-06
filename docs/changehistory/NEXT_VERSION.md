# Next release notes

## Unchanged ElementAspects are no longer rewritten

Full, change, and subset transforms no longer delete and reinsert every replaceable ElementAspect of each accepted owner. The importer now matches each source aspect to an existing target aspect of the same class on the same owner, updates it only if its properties differ, inserts aspects that are new, and deletes target aspects that no longer have a source counterpart. That includes aspects removed from the source and aspect classes that became empty for an owner. A rerun with no source changes writes no aspects, and unchanged aspects keep their target IDs. The exception is a target aspect with a value for a property that only the target schema has: each rerun updates it, keeping its ID and that value. Aspects that a source schema change moves to another class get new target IDs once.

`IModelImporter.onInsertElementAspect`, `onUpdateElementAspect`, and `onDeleteElementAspect` now run only for actual inserts, updates, and deletes. Code that counted these hooks during a rerun will see fewer calls.

`IModelImporter.importElementUniqueAspect` and `importElementMultiAspects` now match existing target aspects of the exact class only. Code that calls them directly may see different results; see [Direct `IModelImporter` aspect calls match the exact class](#direct-imodelimporter-aspect-calls-match-the-exact-class).

With `includeSourceProvenance`, cloned source `ExternalSourceAspect`s whose scope maps to the target scope element are no longer mistaken for the transformer's own provenance. Previously they were inserted again on every run, which duplicated them. Only `Scope` aspects, and `Element` and `Relationship` aspects scoped to a target scope element, are treated as transformer provenance. That includes the provenance of other target scopes: when one element is shared by several scopes, such as one source imported in parts, a transformation into one scope keeps the other scopes' provenance. Cloned provenance from another transformation is reused only by a source aspect with the same kind, scope, and identifier, and isn't deleted when the source deletes it. Cloned source aspects with the same kind and scope as the current scope's provenance still can't be told apart from it, so they are still inserted again on every run.

See [Processing ElementAspects](../learning/transformer/element-aspect-processing.md) for details.

## Context-based provenance resolution for incremental deletions

`IModelTransformer.process()` now resolves guidless incremental element
deletions from its transformation context, which is populated from matching
federation GUIDs and current-scope element provenance before changes are
processed. This removes the additional per-deletion provenance query while
preserving changeset order, recreation handling, federation-GUID-first
resolution, scope isolation, relationship deletion behavior, and database
error propagation.

ElementAspect deletions, identified by their ECClass, no longer enter element
deletion handling. Aspects are synchronized through their owning elements, and
their IDs are not element IDs, so looking them up as element deletions only
added work and could match an unrelated element with the same numeric ID.

Deletion processing now also honors a valid context remap supplied by
`addCustomChanges()` after provenance initialization. A conflicting remap made
before `process()` can still be replaced while the context is initialized from
the current scope. Context mappings are not target-existence checks, so custom
remaps must identify a valid target element. If duplicate current-scope element
provenance exists for one source identifier, the earliest
`ExternalSourceAspect` remains authoritative.

## Set-based element hierarchy traversal in full exports

`IModelExporter` now discovers element hierarchies during full exports (`exportAll()`, `exportModelContents()`, `exportChildElements()`) with a single streamed recursive ECSQL query per traversal root instead of one `queryChildren()` round trip per visited element. Observable export behavior is unchanged for root order, sibling order (ECInstanceId ascending), depth-first pre-order, element filtering, subtree suppression, and exporter callbacks. The streamed loop yields while consuming every result row, including descendants skipped inside rejected subtrees, so large exports remain responsive.

Changes-mode exports can use the separate sparse changed-element traversal described below. The legacy per-element path remains the fallback for incompatible cases, including subclasses that override `exportElement` or `exportChildElements`, so subclass dispatch semantics are preserved.

The streamed traversal relies on SQLite's documented recursive-CTE queue behavior — an `ORDER BY` inside the recursive member turns the queue into a priority queue, yielding depth-first pre-order — via ECSQL `WITH RECURSIVE`, which is supported across the package's supported `@itwin/core-backend` range. Exporter-level traversal tests exercise the resulting order through the production query.

## Faster element traversal during change processing

`IModelExporter.exportChanges()` now finds elements marked as inserted or updated, elements excluded by ID, and the parents needed to reach them in one query. It visits only those paths instead of checking every element in each changed model. `IModelTransformer.process()` uses the same behavior when `argsForProcessChanges` is set. This reduces traversal work when changes affect a small part of a large iModel. Model discovery and other export phases are unchanged.

Existing export callbacks keep the same arguments and parent-before-child order. Unchanged ancestors are not exported. When the exporter reaches a changed element, it calls `shouldExportElement` once for each unchanged ancestor that has not been checked yet, starting at the top. If an ancestor is rejected, `onSkipElement` is called for it and its descendants are skipped, as in a full export. Unchanged elements excluded by ID still trigger `onSkipElement`, and modeled elements continue through the existing model filters. Custom `IModelExporter` subclasses that override `exportElement` or `exportChildElements` use the previous traversal so those overrides continue to receive every element.

A changed element can require an unchanged element that has no mapping in the target, such as its category. `IModelTransformer` does not insert unchanged elements, so it now throws `ITwinError` with key `DependencyMappingMissing` when it cannot map one. Previously, a required parent that was missing from the target could make change processing recurse until the process ran out of memory. It also throws this error when a changed required element is not exported because the filter rejects it or one of its ancestors; previously the import failed later with a less specific error. See [Filtering during change processing](../learning/transformer/index.md#filtering-during-change-processing) for when this error occurs and how to fix it.

See [Incremental exports](../learning/transformer/index.md#incremental-exports) for callback and customization details.

## Breaking change: batched incremental element deletion

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

If an element outside the batch still uses an element in it through a reference that the importer checks, such as its category, its code scope, or a view definition's display style, the importer keeps the used element, the elements it needs, and the elements that contain it, deletes the rest, and logs a warning listing up to ten kept elements. Later syncs don't try to delete kept elements again. Per-element deletion also kept definitions that were still in use, but without a warning.

If a native deletion call fails anyway, for example because geometry outside the batch uses a geometry part in it, or because of a reference from a domain schema, `IModelImporter.deleteElements()` throws an `ElementBulkDeleteError` with scope `IModelTransformerErrorScope` and key `IModelTransformerError.ElementBulkDeleteFailed`. Its `status`, `sqlDeleteStatus`, and `failedIds` describe the failed call. Deletions from that call and earlier ones are still pending in the caller's target transaction, so abandon the transaction before fixing the dependency and retrying.

See [Deleting elements](../learning/transformer/element-deletion.md) for details and an error-handling example.

Batched deletion is 8 to 10 times faster than per-element deletion. A batch that deletes both a category and elements that use it takes two native calls, because core refuses to delete a category still used by an element in the same call. Together with the reference checks, that costs 14% to 20% compared with one unchecked call, which throws and leaves the categories behind. Other batches cost 3% to 7% more. These medians come from interleaved runs of `IModelImporter` deletion on core-backend 5.13.0 with 100,000 physical elements and 100 spatial categories, measured before later planning changes that add about 40 ms to a batch like this:

| Requested elements              | Per-element deletion (2.0.0-dev.50)       | One native call without reference checks      | Batched deletion with reference checks |
| ------------------------------- | ----------------------------------------- | --------------------------------------------- | -------------------------------------- |
| Elements, then their categories | 19.4 s                                    | 1.96 s, throws, and the 100 categories remain | 2.35 s                                 |
| Categories, then their elements | 19.3 s, silently keeps all 100 categories | 1.91 s, throws, and the 100 categories remain | 2.18 s                                 |
| Elements only                   | 21.0 s                                    | 1.97 s                                        | 2.03 s                                 |

## Schema-processing strategies

`IModelTransformer.processSchemas()` now accepts a `SchemaProcessingStrategy`. Calls without options use `NewerVersionSchemaImportStrategy`, which preserves the existing newer-version selection and schema hooks. `DynamicSchemaUnionStrategy`, imported from `@itwin/imodel-transformer/schema-processing`, is available for iModels that may contain different compatible additions to the same schema marked with `CoreCustomAttributes.DynamicSchema`. See [Schema processing in a transformation](../learning/transformer/schema-processing.md) for strategy selection, compatibility rules, extension points, and failure handling.

Package-owned schema conflicts and dependency cycles use `IModelTransformerErrorScope` with the `SchemaConflict` and `SchemaDependencyCycle` keys. Upstream and custom failures retain their original error contract.

`IModelExporter.enumerateSchemas()` is now the schema-discovery extension point used by both `exportSchemas()` and transformer schema processing. Overrides of `exportSchemas()` continue to affect direct exporter calls but no longer control transformer schema discovery.

Applications using the optional schema-processing subpath must provide compatible `@itwin/ecschema-editing` and `@itwin/ecschema-locaters` peer dependencies. The new package `exports` map exposes the root package, `schema-processing`, and `package.json`; undocumented deep imports are no longer supported.

## Breaking change: `IModelTransformer.context` exposes a supported transformation contract

`IModelTransformer.context` is now typed as `IModelTransformContext`. The interface supports target lookup, explicit mappings for elements, element aspects, element classes, and CodeSpecs, and SubCategory filtering. Cloning operations, source and target database access, native resource management, context persistence, and other `IModelCloneContext` implementation details are no longer accessible through the public property.

Continue to obtain the context from an `IModelTransformer`:

```ts
import type { IModelTransformContext } from "@itwin/imodel-transformer";

const context: IModelTransformContext = transformer.context;
const targetElementId = context.findTargetElementId(sourceElementId);
```

Tests should mock `IModelTransformContext` instead of constructing or stubbing `IModelCloneContext` directly.

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

## Breaking changes: `ChangedInstanceIds.addChange`

`ChangedInstanceIds.addChange` now accepts a
[`ChangeInstance`](https://www.itwinjs.org/reference/core-backend/ecdb/changeinstance/)
from the new iTwin.js changeset APIs instead of the deprecated
`ChangedECInstance`.

If you call `addChange` directly, use `ChangesetReader` and
`PartialChangeUnifier` to produce the input:

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

The transformer uses the same reader and unifier to collect changed instance
IDs and deletion metadata in one pass per selected changeset file.

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

## Breaking changes: ElementAspect processing

In 2.x, ElementAspects are exported separately from element callbacks using bounded, owner-scoped groups. The constructor no longer accepts an aspect-processing selector, and the previous implementation that exported aspects beside their owning elements is removed.

Existing `IModelExportHandler` callbacks and `shouldExportElementAspect` remain available. `IModelExporter` also continues to support `excludeElementAspectClass`. These callbacks retain their filtering and export roles, but aspect callbacks are no longer guaranteed to run next to the callback for their owning element.

For each accepted owner, the transformer reconciles replaceable target aspects with the current source aspects. It keeps unchanged aspects, updates changed ones, inserts new ones, and deletes the rest. Excluded aspect classes and transformer provenance aspects are preserved, with one exception: an included unique aspect replaces an excluded unique aspect of a base or derived class on the same element, because iModels store them in one slot. Custom inserted or updated aspect changes infer the owner while the source aspect exists. Custom deleted or missing aspects require the owning element ID and throw when it is omitted:

```ts
changedInstanceIds.addCustomAspectChange(
  "Deleted",
  deletedAspectId,
  owningElementId
);
```

For the processing entry points, Exporter/Transformer/Importer boundaries, workflow diagram, filtering, batching, and custom-change examples, see the [Processing ElementAspects learning guide](../learning/transformer/element-aspect-processing.md).

### Direct `IModelImporter` aspect calls match the exact class

`IModelImporter.importElementUniqueAspect` and `importElementMultiAspects` read existing target aspects with `getAspects`, which also returns aspects of derived classes. Before this release, the importer used those derived-class aspects as matches. That could overwrite a derived-class aspect with base-class properties, or delete it as a surplus aspect. The importer now matches, updates, and deletes only existing aspects of exactly the class named by `classFullName`.

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

`IModelTransformer` already passes concrete classes, so transforms need no changes. See [Calling the importer directly](../learning/transformer/element-aspect-processing.md#calling-the-importer-directly).
