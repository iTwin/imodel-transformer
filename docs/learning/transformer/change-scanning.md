# Supplying source changes

When [IModelTransformer]($transformer) processes changes, it reads them from the source iModel's changesets: it downloads the changesets selected for processing and reads them with `ChangesetReader`. An application that already has these changes, for example from a service that reads each changeset as it is pushed, can supply them instead by overriding the protected [IModelTransformer.scanChanges]($transformer) method. The transformer then downloads only the changesets that the override passes back to the default implementation.

`scanChanges` is a beta API.

## When the transformer calls `scanChanges`

The transformer calls `scanChanges` once, when it initializes to process changes: that is, when [IModelTransformOptions.argsForProcessChanges]($transformer) is set, the source is a briefcase, and changesets are left to process after the skipped ones are removed.

It doesn't call `scanChanges` when the exporter already has changed instance IDs, for example when `argsForProcessChanges.changedInstanceIds` is set. The transformer then still downloads and reads the changesets to find the records it needs for deleted instances.

## Input: changeset ranges

`scanChanges(ranges, changedInstanceIds)` receives:

- `ranges`: ordered, inclusive `[first, last]` changeset index ranges to process. They already exclude changesets that the synchronization must skip, such as changesets pushed by a previous synchronization in the other direction. For example, if the synchronization starts at changeset 2, ends at changeset 5, and must skip changeset 3, `ranges` is `[[2, 2], [4, 5]]`. Cover exactly these changesets: don't scan the skipped ones, and don't scan every changeset since the last synchronization. See [the known risk in skipping changesets](./branching-imodels.md#known-risk-pushes-by-other-users-during-a-synchronization), which applies to overrides too.
- `changedInstanceIds`: the [ChangedInstanceIds]($transformer) to add changes to. It defaults to a new, empty instance. Pass the same instance to every call to `super.scanChanges()`.

## Output: changes and deletion records

`scanChanges` returns a [ChangeScanResult]($transformer) with two parts.

`changedInstanceIds` holds the changes to export. Add each change with [ChangedInstanceIds.addChangeRecord]($transformer), passing a [ChangeRecord]($transformer) with the instance's `id`, its `ecClassId` in the source iModel, the operation `op`, and, for an aspect, the `aspectOwnerElementId` of its owning element. The class ID decides whether the change is to an element, model, aspect, relationship, or CodeSpec; a change whose class ID is none of these is ignored without a warning. Add changes in the order they were made, across all ranges: an insert followed by a delete of the same instance cancels out, and an update followed by a delete becomes a delete. For a deleted aspect, give the owning element ID, because the source iModel no longer has the aspect to read it from.

`deletionBatches` holds an ordered list of [DeletionBatch]($transformer) objects. The transformer uses them to find the target instance of each deleted source instance, because the deleted instance can no longer be read from the source iModel. Each batch lists deletions of one kind per array. Every record has the deleted instance's `ecInstanceId` and `ecClassId`, from [DeletionRecordBase]($transformer); the table lists the other fields:

| Array                   | Record                                             | Fields used to find the target                                                             |
| ----------------------- | -------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `elements`              | [ElementDeletionRecord]($transformer)              | `federationGuid`, when the element had one                                                 |
| `models`                | [ModelDeletionRecord]($transformer)                | none beyond the instance ID                                                                |
| `relationships`         | [RelationshipDeletionRecord]($transformer)         | `sourceECInstanceId` and `targetECInstanceId`, with `ecClassId` for the relationship class |
| `externalSourceAspects` | [ExternalSourceAspectDeletionRecord]($transformer) | `elementId`, `scopeId`, `kind`, `identifier`                                               |

Relationships are those derived from `BisCore:ElementRefersToElements`, excluding `BisCore:ElementDrivesElement`. ExternalSourceAspects are listed only when their scope, kind, and identifier are all set. A deleted ExternalSourceAspect helps only when the source iModel holds this transformation's provenance, as in a reverse synchronization: if its scope is the transformation's [target scope element](./branching-imodels.md#targetscopeelement) and its kind is `Element`, its `identifier` is used as the target ID of its owning element. An empty array means nothing of that kind was deleted.

The result must describe every change in the ranges, as the default implementation does. Missing data isn't detected; it changes the result:

- A change missing from `changedInstanceIds`, including a CodeSpec change, isn't exported.
- A relationship deletion without a matching record in `relationships` is skipped with a warning.
- An element or model deletion whose target can't be found through its record, its ExternalSourceAspect records, or the transformation's provenance is ignored.

## Example

The following transformer reads changes from a cache and passes each range the cache doesn't have to the default implementation. `IModelDb` comes from `@itwin/core-backend`; the other types come from `@itwin/imodel-transformer`.

```ts
[[include:ChangeScanning.cached-changes-transformer]]
```

## Deletion batches

A batch can cover one changeset, one range, or all ranges. The default implementation returns one batch per range.

Batch size matters when an element that has no federation GUID is deleted. Its target is then found through the ExternalSourceAspect that recorded its provenance. If that aspect was deleted, the transformer can use a deleted aspect only from the same batch. For example, a user deletes a provenance aspect on a branch in one changeset and its element in the next. A reverse synchronization then deletes the element's counterpart in the master only when both changesets are in the same batch. This handling is best effort: if a synchronization runs between the two changesets, the deleted aspect is no longer available when the element's deletion is processed, and the counterpart isn't deleted.

Changeset boundaries depend on when users push, so prefer larger batches: return one batch per range, as the default implementation does, unless your source only has finer-grained records.
