# Element reservation pre-scan (prototype)

> Status: alpha prototype for [#418](https://github.com/iTwin/imodel-transformer/issues/418). It is an alternative to the plan, reserve, and replay design in [#473](https://github.com/iTwin/imodel-transformer/pull/473). The option and its behavior can change.

A target iModel that uses server-based element reservations (SchemaSync without the schema lock) rejects an insert of an element with a federation GUID unless that element was reserved first. Reserving one element at a time takes the shared write lock for every request, so the transformer needs to reserve all new elements of a run in one batch, before it inserts any of them.

The pre-scan finds those elements by exporting the run's selection once without importing anything. It then submits one `reserveElements` call and runs the transformation unchanged. The backend applies each reserved ID when the transformation inserts the element.

```ts
// The pre-scan runs by default when the target uses server-based reservations.
const transformer = new IModelTransformer({
  source: sourceDb,
  target: editTxn,
});
await transformer.process();
```

## When the pre-scan runs

`elementReservationPreScan` controls the pre-scan:

| Value              | Behavior                                                                                                           |
| ------------------ | ------------------------------------------------------------------------------------------------------------------ |
| `"auto"` (default) | Runs only when the target's `reservations.isServerBased` is true and the target does not hold the schema lock.     |
| `"on"`             | Always runs. It submits reservations only on a target that uses server-based reservations without the schema lock. |
| `"off"`            | Never runs. A target that requires reservations then rejects new elements that have a federation GUID.             |

Other targets pay nothing by default. `"on"` is useful to measure the scan on a local target.

`IModelTransformer.process()` runs the pre-scan after `initialize()` and before it exports anything. It never runs when the source and target are the same iModel, or when `wasSourceIModelCopiedToTarget` or `preserveElementIdsForFiltering` is set. The other `process*` entry points are not covered.

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
- the same required-reference visits that `IModelTransformer.preExportElement` makes, so a changed category or parent that a changed element requires is classified before the element.

The scan does not export aspects, relationships, or deletions, and does not change the exporter's change data. It loads geometry only when `shouldExportElement`, `hasElementChanged`, or the exporter's `shouldExportElement` is overridden and the transformation loads geometry, so those filters get the same elements in both passes.

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

## Identity modes

The reservation must name the class and code the transformation inserts. The backend rejects an insert whose class or code differs from its reservation with `reservation-conflict`. The pre-scan picks one of two internal modes; the mode is not an option.

| Mode          | Identity                                                                                                                                                                                              |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `"identity"`  | Derived from the source: the remapped class, the source federation GUID, and the source code with the target CodeSpec and scope. No hooks run.                                                        |
| `"transform"` | Calls `onTransformElement`. When the element has no geometry stream and its model and category are already mapped, the hooks get a full clone. Otherwise they get an identity-only clone (see below). |

The pre-scan uses `"transform"` when a subclass overrides `onTransformElement` or the clone context overrides `cloneElement`, and `"identity"` otherwise. The base transform and clone change identity only in ways that `"identity"` derives itself. `"identity"` would be wrong for a hook that changes an element's class or code, which is why the mode is not exposed.

In `"identity"` mode, the scan loads only the identity columns of each element when `shouldExportElement`, `hasElementChanged`, and the exporter's `shouldExportElement` are not overridden. One cached SQLite statement reads the class, model, parent, federation GUID, code, and category. A class whose constructor validates other properties, such as a ViewDefinition, falls back to a full load. An overridden filter may read any property, so it always gets a full element.

The native clone writes to the target in two cases: it imports an unmapped category, and it clones the geometry stream by source ID, which imports unmapped GeometryParts and SubCategories. Either import would happen before the reservations are made and would bypass the export filter. So `"transform"` never runs the native clone for a geometric element or a GeometryPart, or for an element whose category is not mapped.

### Identity-only clones

The scan cannot run the native clone for:

- an element whose model or category is new in the same run: the native clone needs the target model, and nothing new is mapped until the transformation inserts it. This covers every element of a full transformation into an empty target;
- a geometric element or GeometryPart, even in a sync where its model and category are mapped, because cloning its geometry stream can write to the target.

For those elements, `IModelCloneContext.cloneElement` returns an identity-only clone instead of running the native clone. The clone has only `classFullName`, `federationGuid`, and `code`, and `onTransformElement` and `cloneElement` overrides run on it as usual. A hook can change the class or code, and the pre-scan reserves what the hook returns. A hook can set new properties and read them back.

- `onCloned` does not run on an identity-only clone. Core `onCloned` overrides read properties the clone does not have, and they do not change identity. It runs normally in the transformation.
- Reading any other property, enumerating the clone's keys, or spreading it throws `IdentityOnlyCloneReadError`. The pre-scan catches the error, reserves the source identity for that element, counts it in `identityFallbacks`, and logs one warning per run. If the hook changes that element's class or code, its insert fails with `reservation-conflict`.
- The hook's code applies when its input code matches the transformation's: a mapped or matched scope, or a new scope in federation GUID form (see [Code scopes that are new elements](#code-scopes-that-are-new-elements)).

The scan counts these elements in `identityOnlyTransforms`.

## Code scopes that are new elements

When a parent and its coded children are both new, the transformation inserts the children with the parent's new target ID as their code scope. That ID does not exist when the reservations are made, and a reservation with a different scope string is a `reservation-conflict`.

The pre-scan reserves those children with the parent's federation GUID as the code scope. In `"transform"` mode, the hooks get that GUID-scoped code, through `IModelCloneContext.withClonedCode` or the identity-only clone, so the reservation has the spec and value the hooks return. During the transformation, the hooks get the parent's target ID as the scope. Before the insert, `onExportElement` replaces only the scope with the reserved federation GUID, so the insert keeps the hooks' spec and value and matches its reservation. If a hook returns a different code in the transformation than in the pre-scan, the insert fails with `reservation-conflict`. If a hook moves the code to another scope, the pre-scan reserves that code and the insert keeps the hook's scope. The backend resolves the GUID scope to the parent's target ID, and the stored code is the same as without reservations. `ElementReservationPreScan.test.ts` covers this case, including a hook that renames the children, with the real reservation backend.

A new scope without a federation GUID cannot be named in a reservation. The pre-scan counts its coded children in `unreservableScopes` and reserves them with an empty code, so their inserts fail with `reservation-conflict` when reservations are submitted. Reserving them correctly needs reservations in dependency levels, which this prototype does not implement.

A code scope that the traversal reaches after the element is unmapped at insert. The transformation inserts an empty code and fills it in later, so the pre-scan reserves an empty code.

## Limits

- Every filter runs twice: `shouldExportElement` in the pre-scan and again in the transformation. In `"transform"` mode, `onTransformElement` also runs twice for classified elements.
- In `"transform"` mode, a hook for a geometric element, a GeometryPart, or an element whose model or category is new sees only the identity-only clone. A hook that reads other properties falls back to the source identity for that element.
- Hooks that change identity must return the same identity in both passes. Unlike #473's replay, nothing checks this; a mismatch fails at insert with `reservation-conflict`.
- `"identity"` ignores identity changes made by hooks, except class remaps registered with `remapElementClass`. The source `Code` object trims the code value while the native clone keeps it, so a value with leading or trailing whitespace can conflict.
- A missing CodeSpec is imported into the target during the scan, as the transformation's clone would import it.
- Reservations are not released if the transformation fails after the batch is submitted.
- The batch is not split. Planning state is held in memory as `Map` objects.
- Change processing builds the changed-element traversal twice: once for the pre-scan and once for the transformation.
- Server-based reservations require iTwin.js 5.15, which fixes ECSQL `IdSet` rejecting reserved IDs. The prototype pins 5.15.0-dev.10.

## Comparison with plan, reserve, and replay (#473)

|                       | Pre-scan                                                                                                                                                                | Plan, reserve, replay (#473)                                                         |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| Transformation        | Unchanged, except the new-scope code scope override                                                                                                                     | Exporter events recorded and replayed through a swapped handler                      |
| Hooks                 | Filters run twice; `onTransformElement` twice only in transform, the first time with an identity-only clone for geometric elements or when the model or category is new | All hooks run twice, the first time with an identity-only view when the model is new |
| Identity check        | None; a mismatch fails at insert                                                                                                                                        | `plan-changed` when replay differs from the plan                                     |
| New exporter callback | Not affected                                                                                                                                                            | Must be added to the recording                                                       |
| Matching              | Same order, separate implementation                                                                                                                                     | Separate planner                                                                     |
| Geometry              | Not loaded, unless a custom filter needs it                                                                                                                             | Loaded during planning                                                               |
| Default               | Automatic on reservation targets only                                                                                                                                   | Automatic                                                                            |

## Measured cost

These measurements predate the current default: the scan ran on every target, so the numbers are the cost a reservation target pays. Other targets now skip the scan by default.

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
- In full transformations into an empty target, `transform` cost the same as `identity`. No model is mapped while the scan runs, so at the time every such element fell back to `"identity"` without running hooks; in a local run of the standalone fixture, 10,000 of 10,005 elements fell back. Those elements now run the hooks on identity-only clones, which these numbers do not measure.
- Peak RSS stayed within 5 MB of `main`, except standalone-full-transform, where the workflow reported +78 MB in both modes. A local run of the same shape measured +11 MB (176 MB to 187 MB) with no heap growth. The difference is not explained.

For comparison, #473 estimated 1.2–1.9× before optimizations. The pre-scan measured 1.03–1.24×.

### After automatic mode selection and identity-only loads

With the mode picked automatically and identity-only loads, the CI quick-performance overhead against `main` dropped as follows. These runs used revision `79feacef`, where the `"auto"` default ran the scan on every target and skipped only the `reserveElements` call on a target without server-based reservations, so the scan ran in every fixture. The current default skips the scan on such targets.

| Fixture                      | Before (`identity`) | After |
| ---------------------------- | ------------------- | ----- |
| balanced-incremental         | +5.2%               | +2.0% |
| hierarchical-incremental     | +20.8%              | +8.7% |
| large-base-incremental       | +2.5%               | +1.5% |
| realistic-building-transform | +10.0%              | +2.9% |
| standalone-full-transform    | +19.3%              | +7.4% |
