# @itwin/imodel-transformer 2.0

Every consumer upgrading from 1.x must change code: constructors take an `EditTxn`, many methods are asynchronous, several APIs were removed or renamed, and transformer-owned errors are `ITwinError`s with stable keys. Code that compiles after those changes can still see different results, mainly in ElementAspect writes, element deletion, and change-mode traversal.

See [Upgrading from imodel-transformer 1.x to 2.0](../learning/transformer/upgrading-from-1x.md) for step-by-step migration instructions, code samples, and the full list of removed and renamed APIs.

## Upgrade checklist

Each item links to its section of the upgrade guide.

- **Node and iTwin.js.** Node `^22.12.0 || ^24.18.0` and iTwin.js `^5.14.2` peer dependencies. See [Node and peer dependencies](../learning/transformer/upgrading-from-1x.md#node-and-peer-dependencies).
- **Constructors take an `EditTxn`.** `IModelTransformer`, `IModelImporter`, and `TemplateModelCloner` require a started `EditTxn` on the target iModel; reverse sync also requires a `sourceEditTxn`. See [EditTxn constructors](../learning/transformer/upgrading-from-1x.md#edittxn-constructors).
- **Many methods are async.** Await calls to them and make overrides `async`. The synchronization, provenance, and synchronization-version getters are now async methods. See [Async methods](../learning/transformer/upgrading-from-1x.md#async-methods).
- **Removed and renamed APIs.** Including the static provenance helpers, `detectElementDeletes`, `detectRelationshipDeletes`, and `alignECEFLocations`. See [Removed and renamed APIs](../learning/transformer/upgrading-from-1x.md#removed-and-renamed-apis).
- **Element deletion callbacks are batched.** `onDeleteElement` is removed; override `onDeleteElements` instead. See [Element deletion](../learning/transformer/upgrading-from-1x.md#element-deletion).
- **Errors are `ITwinError` with stable keys.** Replace `instanceof IModelError`, `errorNumber`, and message checks with `ITwinError.isError`. See [Errors](../learning/transformer/upgrading-from-1x.md#errors).
- **`exportChanges()` no longer falls back to `exportAll()`.** A source with no changesets and no custom changes now throws `no-changesets`. See [exportChanges() without changesets](../learning/transformer/upgrading-from-1x.md#exportchanges-without-changesets).
- **`IModelTransformer.context` is an `IModelTransformContext`.** See [Context and provenance APIs](../learning/transformer/upgrading-from-1x.md#context-and-provenance-apis).
- **`ChangedInstanceIds.addChange` takes a `ChangeInstance`.** See [ChangedInstanceIds.addChange](../learning/transformer/upgrading-from-1x.md#changedinstanceidsaddchange).
- **Package exports map.** Deep imports are no longer supported, and `exportSchemas()` overrides no longer control transformer schema discovery. See [Schema processing and package exports](../learning/transformer/upgrading-from-1x.md#schema-processing-and-package-exports).
- **Logger categories.** `TransformerLoggerCategory` values use the `imodel-transformer.` prefix instead of `core-backend.`. See [Logger categories](../learning/transformer/upgrading-from-1x.md#logger-categories).
- **Filters must accept required elements.** Full and change transforms throw `DependencyMappingMissing` when an accepted element requires an element the filter rejects, such as its category. See [Element filtering and required elements](../learning/transformer/upgrading-from-1x.md#element-filtering-and-required-elements).
- **ElementAspect processing.** Aspect callbacks no longer run beside their owning element's callback, direct `IModelImporter` aspect calls match the exact class, and custom deleted aspect changes need the owning element ID. See [ElementAspects](../learning/transformer/upgrading-from-1x.md#elementaspects).
- **IModelExporter subclasses.** `exportRelationships()` no longer calls `exportRelationship()` per relationship. See [IModelExporter subclasses](../learning/transformer/upgrading-from-1x.md#imodelexporter-subclasses).

## Behavior changes

Compared with 1.2.0, these change results without any code change.

### ElementAspects removed in the source are deleted in the target

1.2.0 only touched the aspect classes that the source still exported for an element. A unique aspect removed from the source, a multi-aspect class the source no longer had, and aspects rejected by `shouldExportElementAspect` all stayed in the target. 2.0 reconciles each accepted element's target aspects with its current source aspects and deletes target aspects that have no source counterpart, calling `IModelImporter.onDeleteElementAspect` for each. Aspects of excluded classes and transformer provenance aspects are kept.

Aspects match by exact class. 1.2.0 also matched aspects of derived classes. Aspect callbacks no longer run right after the callback for their owning element, because the exporter processes aspects in batches of owners.

See [ElementAspects](../learning/transformer/upgrading-from-1x.md#elementaspects) for the API changes and [Processing ElementAspects](../learning/transformer/element-aspect-processing.md) for details.

### Shared target elements lose more of another source's aspects

When elements from two source iModels map to the same target element, such as a `SpatialCategory` matched by its code, each transformation reconciles that element's aspects against its own source only. 1.2.0 already overwrote or deleted the other source's aspects of a class both sources had. 2.0 also deletes the other source's aspects of classes this source doesn't have, and a source with no aspects on that element deletes all of them except aspects of excluded classes and transformer provenance. Change processing does the same when the changes include that source element or its aspects.

### Cloned source provenance is no longer duplicated

With `includeSourceProvenance`, 1.2.0 inserted cloned source `ExternalSourceAspect`s whose scope maps to the target scope element again on every run. 2.0 treats only `Scope` aspects, and `Element` and `Relationship` aspects scoped to a target scope element, as transformer provenance, and reconciles other cloned aspects like any other aspect. A transformation into one scope keeps other scopes' provenance on a shared element, such as when one source is imported in parts. Cloned provenance from another transformation is reused only by a source aspect with the same kind, scope, and identifier, and isn't deleted when the source deletes it. Cloned source aspects with the same kind and scope as the current scope's provenance still can't be told apart from it, so they are still inserted again on every run.

### Elements still in use are kept during deletion

1.2.0 deleted each element's tree separately. Core kept definition elements that were still in use, without a warning, and no other references were checked in advance. 2.0 deletes the elements of a change set in batches. If an element outside the batch still uses an element in it through a reference that the importer checks, such as its category, its code scope, or a view definition's display style, the importer keeps the used element, the elements it needs, and the elements that contain it, deletes the rest, and logs a warning listing up to ten kept elements. Later syncs don't try to delete kept elements again. See [Element deletion](../learning/transformer/upgrading-from-1x.md#element-deletion) and [Elements that are still in use](../learning/transformer/element-deletion.md#elements-that-are-still-in-use).

### Change-mode traversal visits only changed paths

In 1.2.0, `IModelExporter.exportChanges()` visited every element of each changed model, loading it and calling `shouldExportElement` and `onExportElement` for it. 2.0, and `IModelTransformer.process()` when `argsForProcessChanges` is set, visit only changed elements and the parents needed to reach them. Export callbacks keep the same arguments and parent-before-child order. Unchanged ancestors are not exported. When the exporter reaches a changed element, it calls `shouldExportElement` once for each unchanged ancestor that has not been checked yet, starting at the top. If an ancestor is rejected, `onSkipElement` is called for it and its descendants are skipped, as in a full export. Unchanged elements excluded by ID still trigger `onSkipElement`, and modeled elements continue through the existing model filters. Custom `IModelExporter` subclasses that override `exportElement` or `exportChildElements` keep the 1.2.0 traversal so those overrides continue to receive every element.

See [Incremental exports](../learning/transformer/index.md#incremental-exports) for callback and customization details.

### Deletions use remaps from `addCustomChanges()`

1.2.0 found the target of each deleted source element with its own provenance or FederationGuid query and replaced any remap already in the transformation context. 2.0 resolves deletions from the context, so a remap that `addCustomChanges()` supplies is used. Context mappings are not target-existence checks, so a custom remap must identify a valid target element.

## New features and hooks

### Schema-processing strategies

`IModelTransformer.processSchemas()` now accepts a `SchemaProcessingStrategy`. Calls without options use `NewerVersionSchemaImportStrategy`, which preserves the existing newer-version selection and schema hooks. `DynamicSchemaUnionStrategy`, imported from `@itwin/imodel-transformer/schema-processing`, is available for iModels that may contain different compatible additions to the same schema marked with `CoreCustomAttributes.DynamicSchema`. See [Schema processing in a transformation](../learning/transformer/schema-processing.md) for strategy selection, compatibility rules, extension points, and failure handling.

Package-owned schema conflicts and dependency cycles use `IModelTransformerErrorScope` with the `SchemaConflict` and `SchemaDependencyCycle` keys. Upstream and custom failures retain their original error contract.

The breaking changes that come with this feature are in [Schema processing and package exports](../learning/transformer/upgrading-from-1x.md#schema-processing-and-package-exports).

### New and changed hooks

| Hook                                                                                                          | Use it to                                                                                                                                                                                        |
| ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `IModelExportHandler.onDeleteElements(elementIds)` and `IModelTransformer.onDeleteElements(sourceElementIds)` | Handle every deleted source element of a change set at once. Replaces `onDeleteElement`.                                                                                                         |
| Protected `IModelImporter.onDeleteElements(targetElementIds)`                                                 | Run work before or after the importer deletes a batch of target elements.                                                                                                                        |
| `IModelImporter.deleteElements(elementIds)`                                                                   | Delete a set of target elements in one batch, keeping elements still in use.                                                                                                                     |
| Protected `IModelExporter.exportRelationshipInstance(relationship, isUpdate, sourceFedGuid?, targetFedGuid?)` | Customize export of each relationship. Both `exportRelationship()` and the bulk `exportRelationships()` call it.                                                                                 |
| `IModelExportHandler.onExportRelationship(relationship, isUpdate, sourceFedGuid?, targetFedGuid?)`            | Use the FederationGuids of the relationship's endpoints, when the exporter has them.                                                                                                             |
| `IModelExporter.enumerateSchemas()`                                                                           | Customize which schemas both `exportSchemas()` and transformer schema processing see.                                                                                                            |
| `processSchemas({ strategy })` with a `SchemaProcessingStrategy`                                              | Choose or subclass how source schemas are imported into the target.                                                                                                                              |
| `IModelTransformer.context` as `IModelTransformContext`                                                       | Look up and remap elements, aspects, element classes, and CodeSpecs, and filter SubCategories, through a supported interface.                                                                    |
| `ChangedInstanceIds.addCustomAspectChange(changeType, ids, elementIds?)`                                      | Add custom aspect changes with their owning elements. Required for deleted aspects.                                                                                                              |
| `IModelTransformOptions.tryAlignGeolocation`                                                                  | Align spatial elements between iModels with different ECEF locations or `additionalTransform`s. See [Geolocation alignment](../learning/transformer/upgrading-from-1x.md#geolocation-alignment). |
| `IModelTransformerError` and `IModelTransformerErrorScope`                                                    | Branch on a stable key for each transformer-owned failure. See [Errors](../learning/transformer/upgrading-from-1x.md#errors).                                                                    |

### Other additions

- `IModelImporter.editTxn` returns the importer's transaction.
- `IModelExporter.exportFontByFontFamilyDescriptor()` and `exportFontByFontProps()` export a font by its family descriptor or props.
- `IModelTransformer.calculateTransformFromHelmertTransforms()` and the static `convertHelmertToTransform()` expose the transform that `tryAlignGeolocation` uses for iModels with a geographic coordinate system. See [Geolocation alignment](../learning/transformer/upgrading-from-1x.md#geolocation-alignment).

## Improvements

### Performance

- **Full exports.** `IModelExporter` discovers element hierarchies in `exportAll()`, `exportModelContents()`, and `exportChildElements()` with one streamed recursive ECSQL query per traversal root instead of one `queryChildren()` round trip per visited element. Root order, sibling order (ECInstanceId ascending), depth-first pre-order, element filtering, subtree suppression, and exporter callbacks are unchanged. `IModelExporter` subclasses that override `exportElement` or `exportChildElements` keep the previous traversal.
- **Change processing.** `exportChanges()` finds changed elements, elements excluded by ID, and the parents needed to reach them in one query, and visits only those paths instead of checking every element in each changed model. This reduces traversal work when changes affect a small part of a large iModel. Model discovery and other export phases are unchanged. See [Change-mode traversal visits only changed paths](#change-mode-traversal-visits-only-changed-paths).
- **Element deletion.** Batched deletion is 8 to 10 times faster than per-element deletion. See [Element deletion](../learning/transformer/upgrading-from-1x.md#element-deletion).
- **Incremental deletions.** Element deletions without a FederationGuid are resolved from the transformation context instead of one provenance query per deletion.
- **Relationships.** Relationship export reads relationships and their endpoint FederationGuids with one bulk query instead of separate queries per relationship. See [IModelExporter subclasses](../learning/transformer/upgrading-from-1x.md#imodelexporter-subclasses).
- **Reference and Code lookups.** With `danglingReferencesBehavior: "reject"`, the transformer caches reference existence checks for the run and skips them for entities it has already exported. Finding existing target elements by Code skips empty codes, which can never match.

### Fixes since 1.2.0

If you worked around one of these in 1.x, remove the workaround.

- Change processing handles changes whose data is stored in an overflow table. ([#304](https://github.com/iTwin/imodel-transformer/pull/304))
- A model is not deleted when its partition element was remapped or recreated. ([#305](https://github.com/iTwin/imodel-transformer/pull/305))
- Unchanged elements no longer cause an infinite loop during synchronization. ([#270](https://github.com/iTwin/imodel-transformer/pull/270))
- Change processing no longer recurses until it runs out of memory when it reaches a required parent that is missing from the target; it throws `DependencyMappingMissing`. ([#440](https://github.com/iTwin/imodel-transformer/pull/440))
- Element codes are preserved when updating identity-mapped elements within the same iModel, such as in-place Category updates. ([#367](https://github.com/iTwin/imodel-transformer/pull/367))
- Export change options and root-skipping traversal are preserved during synchronization. ([#330](https://github.com/iTwin/imodel-transformer/pull/330))
- `importElementMultiAspects` returns IDs in input order after deleting surplus multi-aspects. ([#332](https://github.com/iTwin/imodel-transformer/pull/332))
