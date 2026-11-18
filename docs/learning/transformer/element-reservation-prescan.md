# Element reservation pre-scan (prototype)

> Status: prototype for [#418](https://github.com/iTwin/imodel-transformer/issues/418). It is an alternative to the plan, reserve, and replay design in [#473](https://github.com/iTwin/imodel-transformer/pull/473). Its behavior can change.

A target iModel that uses server-based element reservations (SchemaSync without the schema lock) rejects an insert of an element with a federation GUID unless that element was reserved first. Reserving one element at a time takes the shared write lock for every request, so the transformer needs to reserve all new elements of a run in one batch, before it inserts any of them.

The pre-scan finds those elements by exporting the run's selection once without importing anything. It then submits one `reserveElements` call and runs the transformation unchanged. The backend applies each reserved ID when the transformation inserts the element.

```ts
// Nothing to configure: the pre-scan runs when the target needs reservations.
const transformer = new IModelTransformer({
  source: sourceDb,
  target: editTxn,
});
await transformer.process();
```

## When the pre-scan runs

The target decides. `IModelTransformer.process()` runs the pre-scan only when the target's `reservations.isServerBased` is true and the target does not hold the schema lock. A process that holds the schema lock, such as a service that synchronizes schemas, never needs reservations and never pays for the scan. There is no option.

The pre-scan runs after `initialize()` and before anything is exported. It never runs when the source and target are the same iModel, or when `wasSourceIModelCopiedToTarget` or `preserveElementIdsForFiltering` is set. The other `process*` entry points are not covered.

Tests and performance measurements can set the internal `IModelTransformer.forceElementReservationPreScan` to run the scan on a local target. It still submits reservations only when the target needs them.

## Identity comes from the source

A reservation names the element's class, federation GUID, and code. The backend rejects an insert whose class or code differs from its reservation with `reservation-conflict`.

The pre-scan derives that identity from the source element, without cloning it and without running transformer hooks:

- the class, after `remapElementClass` rules;
- the source federation GUID;
- the source code value, with the target CodeSpec (after `remapCodeSpec`) and the mapped or matched code scope.

`onTransformElement`, `cloneElement`, and class `onCloned` overrides do not run during the scan. They run once, in the transformation, as without reservations. **A transformation into a target that needs reservations must not change an element's class, federation GUID, or code in those hooks.** If it does, the insert fails with `reservation-conflict` or `reservation-not-found`.

This is an accepted constraint, not a gap to close:

- Services that change identity in their hooks synchronize schemas and hold the schema lock, so they never reserve.
- Applications that transform into a target without the schema lock are not expected to rename or reclassify elements.
- The reservation API may later accept a federation GUID alone. A GUID-only reservation cannot be invalidated by a class or code change, and reserving an element that ends up matched is harmless.

The scan loads only the identity columns of each element when `shouldExportElement`, `hasElementChanged`, and the exporter's `shouldExportElement` are not overridden. One cached SQLite statement reads the class, model, parent, federation GUID, code, and category. A class whose constructor validates other properties, such as a ViewDefinition, falls back to a full load. An overridden filter may read any property, so it always gets a full element.

## Why the pre-scan depends on `initialize()`

The pre-scan is not an independent pass over the source. It reads the same state the transformation reads, and that state exists only after `initialize()`:

| State from `initialize()`                                  | Used by the pre-scan for                                                       |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------ |
| Scope provenance and changeset ranges                      | Which changesets define the selection in change processing                     |
| `exporter.sourceDbChanges` (`ChangedInstanceIds`)          | The candidate set and `hasElementChanged`                                      |
| Exporter initialization and custom changes                 | Elements added through `addCustomChanges()`                                    |
| Element mappings from provenance (`forEachTrackedElement`) | Matching elements that the target already has, including in reverse sync       |
| The clone context's remaps                                 | Element, class, and CodeSpec remaps that change the identity the target stores |

## Selection

The pre-scan calls `IModelExporter.scanElements()`, which runs the element traversal that `processAll()` or `processChanges()` would run, with the pre-scan as the export handler:

- the same `shouldExportElement` filter and exporter exclusions;
- in change processing, the same sparse changed-element traversal, with only inserted and updated elements and the ancestors needed to reach them;
- the same required-reference visits that `IModelTransformer.onExportElement` makes before transforming an element, so a changed category or parent that a changed element requires is classified before the element.

The scan does not export aspects, relationships, or deletions, and does not change the exporter's change data. It loads geometry only when `shouldExportElement`, `hasElementChanged`, or the exporter's `shouldExportElement` is overridden and the transformation loads geometry, so those filters get the same elements in both passes. Nothing is cloned, so the scan never writes categories, GeometryParts, or SubCategories to the target.

## Classification

For each element the traversal accepts, the pre-scan finds the target element the transformation would update, in the transformation's own order:

1. an existing mapping in the clone context, which includes every element tracked in provenance;
2. a target element with the same federation GUID;
3. a target element with the same code, if the code scope is mapped or matched and the code is not empty. A match of a different class clears the code, as the transformation does. The lookup also finds an element that the same run inserts earlier, as the transformation's lookup does once that element is inserted; a new code scope is compared by its federation GUID.

| Result                       | Reservation                       |
| ---------------------------- | --------------------------------- |
| Match found                  | None. The transformation updates. |
| No match, federation GUID    | `{ federationGuid, class, code }` |
| No match, no federation GUID | None. A normal insert.            |

A source update without a target match is an insert. A source insert that already matches is not reserved. A Category's default SubCategory is never reserved, because the backend inserts it with the Category.

### Forward and reverse synchronization

What a sync inserts is not known up front, so the pre-scan asks the same questions the transformation will ask:

- **Full transformation into an empty target:** every accepted element is an insert.
- **Forward sync (processChanges):** the candidates are the elements inserted or updated in the source changesets since the last sync. Elements synchronized before are mapped from provenance and are not reserved. New source elements are reserved unless the target already has them by federation GUID or code.
- **Reverse sync:** provenance maps branch elements back to their master elements. Elements created in the branch have no mapping and are reserved if the master does not have them.

## Code scopes that are new elements

When a parent and its coded children are both new, the transformation inserts the children with the parent's new target ID as their code scope. That ID does not exist when the reservations are made, and a reservation with a different scope string is a `reservation-conflict`.

The pre-scan reserves those children with the parent's federation GUID as the code scope. Before the insert, `onExportElement` replaces only the code scope with that federation GUID, so the insert matches its reservation. The backend resolves the GUID scope to the parent's target ID, so the stored code is the same as without reservations. Only the reservation row keeps the GUID form. `ElementReservationPreScan.test.ts` covers this case with the real reservation backend.

This is the prototype's current choice and may change. Known consequences:

- If a run reserves a child but fails before inserting it, and the parent was inserted, the next run reserves the child with the parent's numeric ID. The leftover GUID-scoped reservation then makes the whole batch fail with `reservation-conflict`.
- The reserved-code index compares the scope as text, so a GUID-scoped reservation and a numeric-scoped one for the same parent do not detect a duplicate code.

A new scope without a federation GUID cannot be named in a reservation. The pre-scan counts its coded children in `unreservableScopes` and reserves them with an empty code, so their inserts fail with `reservation-conflict` when reservations are submitted. Reserving them correctly needs reservations in dependency levels, which this prototype does not implement.

A code scope that the traversal reaches after the element is unmapped at insert. The transformation inserts an empty code and fills it in later, so the pre-scan reserves an empty code.

## Limits

- Transform hooks must not change class, federation GUID, or code (see [Identity comes from the source](#identity-comes-from-the-source)). Nothing checks this; a mismatch fails at insert.
- `shouldExportElement` and `hasElementChanged` run twice: once in the pre-scan and again in the transformation.
- The source `Code` object trims the code value while the native clone keeps it, so a value with leading or trailing whitespace can conflict.
- A code match is checked against the target before the run. If the source renames element A's code and a new element B takes A's old code, the scan matches B to A and does not reserve B.
- A missing CodeSpec is imported into the target during the scan, as the transformation's clone would import it.
- Reservations are not released if the transformation fails after the batch is submitted.
- The batch is not split. Planning state is held in memory as `Map` objects.
- Change processing builds the changed-element traversal twice: once for the pre-scan and once for the transformation.
- Server-based reservations require iTwin.js 5.15, which fixes ECSQL `IdSet` rejecting reserved IDs.

## Comparison with plan, reserve, and replay (#473)

|                       | Pre-scan                                            | Plan, reserve, replay (#473)                                                         |
| --------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------------ |
| Transformation        | Unchanged, except the new-scope code scope override | Exporter events recorded and replayed through a swapped handler                      |
| Hooks                 | Run once; filters run twice                         | All hooks run twice, the first time with an identity-only view when the model is new |
| Identity              | From the source; hooks must not change it           | From the hooks; `plan-changed` when replay differs from the plan                     |
| New exporter callback | Not affected                                        | Must be added to the recording                                                       |
| Matching              | Same order, separate implementation                 | Separate planner                                                                     |
| Geometry              | Not loaded, unless a custom filter needs it         | Loaded during planning                                                               |
| Runs when             | The target needs reservations                       | Always                                                                               |

## Measured cost

These measurements predate the current design: the scan ran on every target, so the numbers are the cost a reservation target pays. Other targets now skip the scan. The `identity` columns measure the scan as it is now, which derives identity from the source; `transform` measured a mode that ran the hooks and has since been removed.

The pre-scan's local cost is measured without a reservation server. On a local target, the scan runs in full and only `reserveElements` is skipped, so the comparison isolates the pre-scan from network time.

Each variant branch differs from the prototype only in the option's default. All runs use the quick performance A/B workflow with 40 samples per arm against `main`:

- `off`: the 5.15.0-dev.10 version pin alone;
- `identity`: the pin plus the pre-scan in identity mode;
- `transform`: the pin plus the pre-scan in transform mode.

The table shows the change in median wall time, candidate against `main`, within the same workflow run. Absolute times differ between runners, so only deltas within a run are comparable.

| Fixture                                       | `off`  | `identity`                                                                      | `transform`                                                                     |
| --------------------------------------------- | ------ | ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| balanced-incremental                          | +0.02% | [+5.15%](https://github.com/iTwin/imodel-transformer/actions/runs/39488162255)  | [+6.74%](https://github.com/iTwin/imodel-transformer/actions/runs/39488118768)  |
| hierarchical-incremental                      | +0.32% | [+20.79%](https://github.com/iTwin/imodel-transformer/actions/runs/39488170829) | [+24.28%](https://github.com/iTwin/imodel-transformer/actions/runs/39488128448) |
| large-base-incremental                        | −0.06% | [+2.50%](https://github.com/iTwin/imodel-transformer/actions/runs/39488178756)  | [+2.89%](https://github.com/iTwin/imodel-transformer/actions/runs/39488136774)  |
| realistic-building-transform (3,440 elements) | +0.41% | [+10.02%](https://github.com/iTwin/imodel-transformer/actions/runs/39488195588) | [+8.21%](https://github.com/iTwin/imodel-transformer/actions/runs/39488153454)  |
| standalone-full-transform (10,000 elements)   | +0.86% | [+19.30%](https://github.com/iTwin/imodel-transformer/actions/runs/39488187359) | [+19.26%](https://github.com/iTwin/imodel-transformer/actions/runs/39488145229) |

- The version pin alone costs under 1%, so the `identity` and `transform` deltas are the pre-scan's cost.
- A full transformation costs about 50 µs per element, or about 0.5 s for 10,000 elements.
- Incremental runs cost 2.5–24%. Hierarchical-incremental is the most expensive relative to its short run, because change processing builds the changed-element traversal twice.
- Peak RSS stayed within 5 MB of `main`, except standalone-full-transform, where the workflow reported +78 MB in both modes. A local run of the same shape measured +11 MB (176 MB to 187 MB) with no heap growth. The difference is not explained.

For comparison, #473 estimated 1.2–1.9× before optimizations. The pre-scan measured 1.03–1.24×.

### After identity-only loads

With identity-only loads, the CI quick-performance overhead against `main` dropped as follows. These runs used revision `79feacef`, where the `"auto"` default ran the scan on every target and skipped only the `reserveElements` call on a target without server-based reservations, so the scan ran in every fixture. The current default skips the scan on such targets.

| Fixture                      | Before (`identity`) | After |
| ---------------------------- | ------------------- | ----- |
| balanced-incremental         | +5.2%               | +2.0% |
| hierarchical-incremental     | +20.8%              | +8.7% |
| large-base-incremental       | +2.5%               | +1.5% |
| realistic-building-transform | +10.0%              | +2.9% |
| standalone-full-transform    | +19.3%              | +7.4% |
