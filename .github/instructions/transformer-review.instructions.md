---
applyTo: "{packages/transformer/**/*.{ts,tsx,md,json},common/api/**/*,change/**/*,docs/learning/transformer/**/*,docs/learning/EditTxnInTransformer.md,docs/changehistory/NEXT_VERSION.md,.github/workflows/**/*.{yml,yaml}}"
---

# Transformer Review Rules

Use the repository-root `AGENTS.md` and `packages/transformer/README.md` as contract sources for transformer implementation and test changes.

## Public and package contracts

- Keep the strict `@itwin/core-backend` peer-dependency contract visible; surface an incompatible dependency rather than hiding it behind a fallback.
- Keep `IModelTransformContext` as the supported context type and `IModelCloneContext` as an implementation-only type.
- Regenerate `common/api/*` reports with the repository API tooling when a public API changes, and include the corresponding change file and release-note impact.
- Record migration guidance in `docs/changehistory/NEXT_VERSION.md` for behavior changes that require consumer action.

## Error and transaction boundaries

- Use `ITwinError` with `IModelTransformerErrorScope` and an `IModelTransformerError` key for transformer-owned, consumer-actionable conditions.
- Preserve errors from core, backend, and database layers; retain the original error as `cause` when translating one.
- Use plain `Error` or assertions for internal invariants and implementation bugs.
- Perform transformer writes under an active target `EditTxn`; preserve caller ownership and make success, failure, save, and abandon behavior explicit.
- Treat `initializeBranchProvenance` as the documented transaction-ownership exception because it creates and ends its own transaction.
- Treat source and target transactions in reverse synchronization as one success or failure boundary.
- Use the established `withEditTxn` pattern in database-editing tests so the helper owns the test transaction lifecycle.

```ts
const id = withEditTxn(db, "insert PhysicalObject", (txn) => {
  return insertPhysicalObject(txn);
});
```

## Transformation semantics

- Trace source IDs, target IDs, provenance, and remaps through full export, change export, subset processing, reverse synchronization, and retry paths as applicable.
- Keep filters, exclusions, hierarchy rules, model boundaries, and `shouldExportElement` decisions consistent across traversal modes.
- Preserve ElementAspect accepted-owner semantics, class and aspect exclusions, owner-scoped batching, deduplication, cleanup-before-rebuild, and deferred reference completion.
- Correlate aspect processing through callback IDs and documented state so owner and aspect callback ordering remains explicit.
- Preserve relationship and aspect ownership, cardinality, duplicate handling, and target cleanup semantics.
- Prefer existing exporter, importer, provenance, query, and transaction helpers over near-duplicates.

## Tests

- Exercise changed branches and failure boundaries in addition to the happy path.
- For traversal changes, cover empty scopes, duplicates, excluded elements, missing parents, deep or cyclic hierarchy inputs, and parity with the existing traversal path when relevant.
- For cache changes, cover key scope, invalidation, repeated calls, missing records, and cross-run isolation.
- For concurrency or prefetch changes, cover ordering, failure propagation, cancellation or cleanup, and operation-boundary isolation.
