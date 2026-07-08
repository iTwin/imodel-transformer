# Next release notes

<!--
  Hand-author the release notes for the NEXT minor or major release of @itwin/imodel-transformer here.
  On the next minor/major "Publish NPM packages" run this file is archived to
  docs/changehistory/<version>.md; the authored prose here is included in the GitHub Release body
  alongside a link to the full CHANGELOG, then this file is reset to this template.
  Patch (and dev/prerelease) publishes ignore this file.
-->

## Breaking changes

### Many synchronous methods are now asynchronous

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
