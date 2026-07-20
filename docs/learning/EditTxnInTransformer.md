# EditTxn in imodel-transformer

> For general `EditTxn` concepts (lifecycle, `withEditTxn`, migration from implicit writes, common failure modes), see the [iTwin.js EditTxn learning page](https://www.itwinjs.org/learning/backend/edittxn/).

This document covers transformer-specific patterns only.

## IModelTransformer

The transformer requires a **started** `EditTxn` for the target database. Pass the transaction directly:

[[include:EditTxnInTransformer.direct-transformer]]

To customize import behavior, create an `IModelImporter` with the started transaction and pass the importer instead:

[[include:EditTxnInTransformer.transformer-with-custom-importer]]

The caller owns the transaction lifecycle and must call `editTxn.end()`. During `processChanges`, `ProcessChangesOptions.saveTargetChanges` controls whether the transformer calls `saveChanges()`.

## Reverse synchronization

Reverse synchronization writes provenance to the source branch database. It therefore requires one transaction for the target master database and another for the source branch database. The example uses this helper to create each started transaction:

[[include:EditTxnInTransformer.create-started-edit-txn]]

In the transformation below, `branchDb` is the source and `masterDb` is the target:

[[include:EditTxnInTransformer.reverse-synchronization]]

The transformer detects reverse sync automatically based on provenance direction. Without `sourceEditTxn`, it throws at runtime.

## IModelImporter

The importer takes a single `EditTxn` and derives `targetDb` from it:

[[include:EditTxnInTransformer.custom-importer]]

## TemplateModelCloner

`TemplateModelCloner` performs in-place operations, so its source and target are the same database. The example starts with `facilityEditTxn` already active for the database that owns the template and the new instance:

[[include:EditTxnInTransformer.template-cloner-construction]]

Pass the template model ID, target model ID, and placement when placing the template:

[[include:EditTxnInTransformer.template-cloner-placement]]

## initializeBranchProvenance

`initializeBranchProvenance` creates and ends its own transaction. Pass an `initProvenanceArgs` object with `master` and `branch` properties; do not create a transaction for this operation:

[[include:EditTxnInTransformer.initialize-branch-provenance]]

## Ending transactions after errors

`EditTxn.end()` saves by default. For a disposable database or a test that can rely on database cleanup after a failure, call `end()` after successful processing:

[[include:EditTxnInTransformer.direct-transformer]]

If `process()` throws before `end()` runs, the transaction remains active until the database closes.

### Abandon on failure

Use `try` and `finally` when failed processing must explicitly abandon the transaction. This prevents production code from saving or pushing partial changes:

[[include:EditTxnInTransformer.rollback-on-failure]]

The `finally` block saves a successful transformation and abandons a failed one.

### Reverse sync: two transactions

Reverse synchronization must save or abandon the source and target transactions together. Track one `processSucceeded` flag, then call `end(processSucceeded ? "save" : "abandon")` on both transactions in the `finally` block.

### Summary

| Pattern | Cleanup |
|---|---|
| Test or disposable database | Call `end()` after successful processing. Closing the database cleans up an active transaction after a failure. |
| Production transformation | In `finally`, save after success or abandon after failure. |
| Reverse synchronization | End both transactions with the same save or abandon mode. |
| `initializeBranchProvenance` | No caller-managed transaction. |
