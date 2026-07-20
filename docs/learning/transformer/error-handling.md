# Error handling in imodel-transformer

`@itwin/imodel-transformer` identifies package-owned errors with [ITwinError]($bentley). Each identified error has the scope [IModelTransformerErrorScope]($transformer) and a key from [IModelTransformerError]($transformer).

## Handle an identified transformer error

Use `ITwinError.isError` with both the transformer scope and the expected key. Do not branch on the message because messages may change without notice.

```ts
import { ITwinError } from "@itwin/core-bentley";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "@itwin/imodel-transformer";

try {
  await transformer.process();
} catch (error) {
  if (
    ITwinError.isError(
      error,
      IModelTransformerErrorScope,
      IModelTransformerError.DanglingReference
    )
  ) {
    // Correct the source reference or choose a different dangling-reference policy.
    return;
  }

  throw error;
}
```

The enum documentation describes each condition. The key is stable within the API lifecycle indicated by its release tag.

## Error ownership

The error type identifies which layer owns the failure:

- The transformer uses its scope and an `IModelTransformerError` key when it detects a condition that a caller can identify or correct.
- Errors from iTwin.js core, the backend, or the database retain their original type and status. For example, a database operation may still throw `IModelError`. The transformer does not relabel an upstream failure only to add transformer context.
- When the transformer converts an upstream failure into a more specific transformer condition, the identified transformer error retains the upstream error as `cause`.
- Impossible internal states and implementation defects use plain `Error` or assertions. They do not receive stable identifiers because callers cannot recover from them reliably.

Custom transformer and importer subclasses should use the same boundary. Add a transformer identifier for a package-owned condition that callers can handle. Preserve upstream errors, and use plain errors for internal invariants.

## Migrating from IModelError checks

Transformer-owned conditions previously used a mix of `IModelError` and plain `Error`. Code that checks `instanceof IModelError`, reads `errorNumber`, or compares messages must use the transformer scope and key instead.

```ts
if (
  ITwinError.isError(
    error,
    IModelTransformerErrorScope,
    IModelTransformerError.TargetClassNotFound
  )
) {
  // Import the missing schema before retrying.
}
```

Continue using the upstream error contract when the error originates outside the transformer. The transformer intentionally passes those errors through, so existing status checks for core or database failures remain valid.

## Inspect the cause

A translated transformer error may include the original failure in `cause`. Treat it as diagnostic context rather than the primary discriminator.

```ts
if (
  ITwinError.isError(
    error,
    IModelTransformerErrorScope,
    IModelTransformerError.TargetClassNotFound
  )
) {
  console.error(error.cause);
}
```
