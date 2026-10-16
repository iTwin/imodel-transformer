# Automatic element reservations

`IModelTransformer` bulk-reserves eligible target element inserts before executing each processing call when the target uses server-based element reservations and does not hold the schema lock. There is no opt-in option or caller preparation step. Local/no-op reservation targets and the schema-lock-held bypass keep their existing processing behavior.

Automatic reservations require iTwin.js 5.15 or later (`@bentley/imodeljs-native` 5.15.14 or later). Reserved element IDs start with `0xfffff6`, and earlier native versions rejected IDs at or above `0x8000000000000000` in the ECSQL `IdSet` function used by aspect cleanup, aspect export, and bulk deletion ([imodel-native#1640](https://github.com/iTwin/imodel-native/issues/1640)).

Element reservations do not enable SchemaSync or import schemas. Configure the target's synchronization and locking policy separately, and call `processSchemas()` when the target needs schema preparation. See [Schema processing](./schema-processing.md).

## Who handles what

| The transformer handles                                                                                    | The caller handles                                                                                                  |
| ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| Deciding which target inserts need a reservation, and skipping matches, updates, deletions, and exclusions | Configuring the target's shared reservations, SchemaSync, and lock policy, and running `processSchemas()` if needed |
| One bulk `reserveElements` request per outermost processing call, before that call's element writes        | Making every briefcase that inserts a shared reserved identity insert identical contents                            |
| Keeping the reserved class, GUID, and code (including GUID-form code scopes) through ordinary insertion    | Keeping identity-affecting hooks stable between preparation and execution, and free of database writes              |
| Rejecting unsupported cases with `ElementReservationUnsupported` before element writes                     | Abandoning or saving the target transaction after a failure, and using a fresh transformer after rolling back       |
| Rejecting execution that drifts from the plan with `ElementReservationPlanChanged`                         | Treating reservations as outliving an abandoned transaction; they are not rolled back                               |
| Logging a failed reservation request once and rethrowing the backend error                                 | Retrying, if wanted; the transformer does not retry or fall back to single-element requests                         |

## What is reserved

The transformer first applies export selection and resolves existing target matches: explicit mappings, source federation GUIDs, then transformed codes with the existing class checks. It reserves the resulting target inserts that have explicit federation GUIDs, using their transformed class, GUID, and code. It does not reserve updates, deletions, or excluded elements.

A source change's kind does not determine whether a reservation is needed. A source update that has no target match becomes a target insert and is reserved if its transformed target identity has a federation GUID. A source insert that already matches a target element is not reserved. An insert whose transformed props have no GUID remains an ordinary unreserved insert, executed after the reservation stage. The transformer does not generate a GUID merely to reserve it.

A matching committed reservation can be reused. The backend validates the request and assigns the reserved ID during ordinary insertion. The transformer does not assign provisional IDs or use forced IDs to consume reservations.

A Category's native insertion creates its default SubCategory. The transformer does not submit a separate reservation for that default; it maps and updates the actual backend-created element during execution, preserving its source appearance. Other new SubCategories are prepared and reserved normally.

### Shared identity requires identical contents

The application must ensure that every briefcase inserting a shared reserved identity inserts identical contents. This includes geometry, references, and custom transformation results, not just the class and code. An absent target match or a matching federation GUID does not establish that guarantee. Backend reservation checks and the transformer's identity checks do not compare complete element contents across briefcases.

## Processing-call boundaries

Each outermost call to `process()`, `processElement()`, `processChildElements()`, `processModel()`, `processModelContents()`, `processSubject()`, or `processRelationships()` is one operation:

1. Discover the selected entities and required dependencies, and prepare their target identities.
2. Submit one bulk reservation request if the operation has eligible inserts.
3. Execute the deferred imports, updates, deletions, and provenance work.

Nested dependency processing joins the same operation. Two separate subset calls are two operations, even when they share a transaction. There is no public API to collect arbitrary calls into a larger reservation group. An operation with no eligible inserts makes no reservation request.

The transformer retains source IDs and compact identity/event metadata while preparing the operation, rather than retaining every element's geometry. Preparation adds memory use and delays the first import. Selection callbacks are not rerun in a second export traversal. Keep the source data, caller-owned target data, and export configuration stable throughout the call.

`process()` retains its normal finalization behavior. Incremental processing retains its configured `saveTargetChanges` callback. Neither runs a finalization/save tail after an earlier preparation, reservation, or execution failure. Subset calls do not finalize or save automatically.

### Copy a subtree

This example assumes that the source and target are open, their schemas are compatible, and the target has no active edit transaction or unsaved changes. The target's shared-reservation configuration and lock policy are already established. `sourceParentId` must identify a non-root element; its selected children are part of the same operation.

```ts
import type { Id64String } from "@itwin/core-bentley";
import { IModelDb, withEditTxn } from "@itwin/core-backend";
import { IModelTransformer } from "@itwin/imodel-transformer";

async function copySubtree(
  sourceDb: IModelDb,
  targetDb: IModelDb,
  sourceParentId: Id64String
): Promise<void> {
  await withEditTxn(targetDb, "Copy subtree", async (targetEditTxn) => {
    const transformer = new IModelTransformer({
      source: sourceDb,
      target: targetEditTxn,
    });
    try {
      await transformer.processElement(sourceParentId);
      transformer.importer.finalize();
    } finally {
      transformer.dispose();
    }
  });
}
```

`withEditTxn` saves on success and abandons on failure. That abandonment is the caller's transaction policy, not reservation rollback. Successful reservations may remain after the transaction is abandoned. For a transaction owned by a larger workflow, follow the [EditTxn lifecycle guidance](../EditTxnInTransformer.md) instead of starting another transaction.

## Codes and dependencies

For an existing code scope, the transformer uses its actual target element ID. A scope matched by code may have a different GUID from its source; the source GUID is not used as a substitute for that target identity.

For a new code scope, the transformer can reserve a dependent element using the scope's prepared target GUID before either element exists in the target. The same GUID representation must reach ordinary insertion. Switching to the equivalent numeric scope after inserting the parent causes a backend `reservation-conflict`, even though the stored element code ultimately contains a numeric scope ID.

New modeled elements, models, and their contained elements can be prepared before insertion when their identities can be determined without target writes. A new scope without a usable GUID cannot support a dependent coded insert in that preparation: the transformer rejects the unresolved dependency instead of inserting the scope and making a later reservation request. Existing scopes can use numeric IDs even when they have no usable GUID.

Exact code whitespace is preserved. An intrinsic empty code does not require a scope dependency; an empty-valued domain code retains its CodeSpec rather than being replaced unconditionally with the intrinsic empty code.

## Custom transformations and supported modes

Reservation-enabled operations run identity-affecting transformation hooks during preparation and again during execution. Hooks must produce stable identities without depending on earlier target writes. The transformer does not overwrite a customized GUID, class, or code to make an insertion match its reservation.

When a target model does not yet exist, preparation cannot perform a full native clone. Existing `onCloned` and `onTransformElement` hooks receive an identity-only target view for that case. They may customize class, GUID, and code using source data and available identity fields. Reading an unavailable target property, or enumerating the entire target object, fails with `ElementReservationUnsupported`. A hook that requires complete target props is unsupported in this situation even if it ultimately leaves the identity unchanged. When the real target model is available, preparation uses full native cloning.

Move identity decisions that depend on already-inserted target data to stable source/configuration inputs. Do not mutate databases in preparation hooks. A customization that changes its identity during execution or introduces an unplanned insert can fail after earlier execution writes; preparation does not prove arbitrary callback purity.

Source-ID preservation, copied-target, and in-place/template workflows must not be converted silently into shared-ID allocation. Combinations that require unsupported reserved inserts are rejected during preparation. Existing-match-only operations retain their existing behavior where the mode permits them. Targets outside the automatic reservation path retain their previous behavior.

Direct use of `IModelImporter`, or direct calls to export-handler callbacks, does not create a transformer processing-call boundary. Those callers remain responsible for reservations. Custom importers must preserve the planned insertion identity and use the ordinary insertion path.

## Failure handling

| Failure stage | Effect and caller action                                                                                                                                                                                                                                                                                                |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Preparation   | Known unsupported identities, dependencies, or modes fail before the operation's element/model/aspect/relationship/provenance modifications. Correct the cause before issuing another processing call.                                                                                                                  |
| Reservation   | The original backend error escapes. The transformer logs it once in the transformer logger category with `phase: "reserve-elements"`, `batchSize`, and the error. It does not retry, fall back to single-element requests, or execute the operation's modifications.                                                    |
| Execution     | The original insertion/import error, or a transformer identity-drift error, escapes. Earlier execution writes can remain pending. The transformer does not abandon the caller's transaction or delete successful reservations. Decide whether to abandon; use a fresh transformer after rolling back execution changes. |

Preparation and reservation failure do not imply that the entire target database is unchanged. Schema, CodeSpec, and font/resource prerequisites may already have been written. Implicit initialization defers scope-provenance persistence until reservations succeed; an explicit earlier `initialize()` call retains its existing writes. Arbitrary caller code that writes directly to a database is outside this guarantee.

Reservation-batch atomicity belongs to the backend. A failing batch does not erase older committed reservations. Likewise, abandoning target edits after a successful reservation does not erase that reservation. Do not treat reservation and target transactions as one atomic operation.

Transformer-owned unsupported preparation errors use `IModelTransformerError.ElementReservationUnsupported`. Execution-time identity drift or detected unplanned inserts use `ElementReservationPlanChanged`. Check these with `ITwinError.isError`, `IModelTransformerErrorScope`, and the enum key. Backend reservation conflicts, invalid-reservation errors, and native insert failures retain their original error contract; do not match their message text. See [Error handling](./error-handling.md).
