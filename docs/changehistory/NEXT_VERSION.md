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

- **IModelExportHandler** — `onDeleteElement`, `onDeleteModel`, `onDeleteRelationship`, `onExportCodeSpec`, `onExportElement`, `onExportElementMultiAspects`, `onExportElementUniqueAspect`, `onExportFont`, `onExportModel`, `onExportRelationship`, `onSkipElement`, `shouldExportCodeSpec`, `shouldExportElement`, `shouldExportElementAspect`, `shouldExportRelationship`, `shouldExportSchema`
- **IModelExporter** — `shouldExportElement`
- **IModelImporter** — `importElement`, `importModel`, `importRelationship`, `importElementMultiAspects`, `importElementUniqueAspect`, `deleteElement`, `deleteModel`, `deleteRelationship`, `onInsertElement`, `onInsertModel`, `onInsertRelationship`, `onInsertElementAspect`, `onUpdateElement`, `onUpdateModel`, `onUpdateRelationship`, `onUpdateElementAspect`, `onDeleteElement`, `onDeleteModel`, `onDeleteRelationship`, `onDeleteElementAspect`, `onProgress`
- **IModelTransformer** — `onTransformElement`, `onTransformElementAspect`, `onExportCodeSpec`, `onExportElement`, `onExportElementMultiAspects`, `onExportElementUniqueAspect`, `onExportFont`, `onExportModel`, `onExportRelationship`, `onDeleteElement`, `onDeleteModel`, `onDeleteRelationship`, `shouldExportCodeSpec`, `shouldExportElement`, `shouldExportElementAspect`, `shouldExportRelationship`, `shouldExportSchema`, `shouldDetectDeletes`, `completePartiallyCommittedElements`, `completePartiallyCommittedAspects`, `initScopeProvenance`, `initElementProvenance`, `tryGetProvenanceScopeAspect`, `getSynchronizationVersion`, `getIsForwardSynchronization`, `getIsReverseSynchronization`, `getProvenanceDb`
- **TemplateModelCloner** — `onTransformElement`

### Synchronization property accessors replaced with async methods

The `isForwardSynchronization` and `isReverseSynchronization` getters on `IModelTransformer` have been replaced with async methods `getIsForwardSynchronization()` and `getIsReverseSynchronization()`.

### Requires iTwin.js 5.0 peer dependencies

This version requires iTwin.js 5.0 packages as peer dependencies.

---

See [v2 Migration Guide](../learning/transformer/v2-migration.md) for detailed migration instructions and code examples.
