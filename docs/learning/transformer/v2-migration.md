# Migrating to @itwin/imodel-transformer 2.0

## Overview

Version 2.0 of `@itwin/imodel-transformer` upgrades to iTwin.js 5.0 and introduces several breaking changes. The most impactful change is that many previously synchronous methods are now asynchronous.

## Methods that became async

A large number of methods across the public API now return `Promise` and must be `await`ed. If you override any of these methods in a subclass, your override must also be declared `async` (or return a `Promise`).

### IModelExportHandler

The following handler callbacks are now async:

| Method | Notes |
|--------|-------|
| `onDeleteElement` | |
| `onDeleteModel` | |
| `onDeleteRelationship` | |
| `onExportCodeSpec` | |
| `onExportElement` | |
| `onExportElementMultiAspects` | |
| `onExportElementUniqueAspect` | |
| `onExportFont` | |
| `onExportModel` | |
| `onExportRelationship` | |
| `onSkipElement` | |
| `shouldExportCodeSpec` | |
| `shouldExportElement` | |
| `shouldExportElementAspect` | |
| `shouldExportRelationship` | |
| `shouldExportSchema` | |

### IModelExporter

| Method | Notes |
|--------|-------|
| `exportFontByFontFamilyDescriptor` | New method, async from the start |
| `exportFontByFontProps` | New method, async from the start |
| `shouldExportElement` | Was sync, now async |

### IModelImporter

All import and delete operations, as well as their corresponding `on*` callbacks, are now async:

| Method | Notes |
|--------|-------|
| `importElement` | |
| `importModel` | |
| `importRelationship` | |
| `importElementMultiAspects` | |
| `importElementUniqueAspect` | |
| `deleteElement` | |
| `deleteModel` | |
| `deleteRelationship` | |
| `onInsertElement` | |
| `onInsertModel` | |
| `onInsertRelationship` | |
| `onInsertElementAspect` | |
| `onUpdateElement` | |
| `onUpdateModel` | |
| `onUpdateRelationship` | |
| `onUpdateElementAspect` | |
| `onDeleteElement` | |
| `onDeleteModel` | |
| `onDeleteRelationship` | |
| `onDeleteElementAspect` | |
| `onProgress` | |

### IModelTransformer

| Method | Notes |
|--------|-------|
| `onTransformElement` | |
| `onTransformElementAspect` | |
| `onExportCodeSpec` | |
| `onExportElement` | |
| `onExportElementMultiAspects` | |
| `onExportElementUniqueAspect` | |
| `onExportFont` | |
| `onExportModel` | |
| `onExportRelationship` | |
| `onDeleteElement` | |
| `onDeleteModel` | |
| `onDeleteRelationship` | |
| `shouldExportCodeSpec` | |
| `shouldExportElement` | |
| `shouldExportElementAspect` | |
| `shouldExportRelationship` | |
| `shouldExportSchema` | |
| `shouldDetectDeletes` | |
| `completePartiallyCommittedElements` | |
| `completePartiallyCommittedAspects` | |
| `initScopeProvenance` | |
| `initElementProvenance` | |
| `tryGetProvenanceScopeAspect` | |
| `getSynchronizationVersion` | |
| `getIsForwardSynchronization` | Replaces the `isForwardSynchronization` getter |
| `getIsReverseSynchronization` | Replaces the `isReverseSynchronization` getter |
| `getProvenanceDb` | |

### TemplateModelCloner

| Method | Notes |
|--------|-------|
| `onTransformElement` | |

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

## Other breaking changes

- Requires iTwin.js 5.0 peer dependencies.
