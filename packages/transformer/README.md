# @itwin/imodel-transformer

Copyright © Bentley Systems, Incorporated. All rights reserved. See LICENSE.md for license terms and full copyright notice.

## Description

The **@itwin/imodel-transformer** package contains classes that handle traversing iModels for exporting and importing their parts.

## Documentation

See the [iTwin.js](https://www.itwinjs.org) documentation for more information.

### Transformation context

Retrieve the supported transformation context from an `IModelTransformer`. Depend on `IModelTransformContext` when passing the context to helpers or creating test doubles; `IModelCloneContext` is an implementation detail.

```ts
import type { IModelTransformContext } from "@itwin/imodel-transformer";

const context: IModelTransformContext = transformer.context;
```

## Versioning

This package, for the time being, relies on @internal APIs in iTwin.js, and therefore has very strict peerDependencies versions.
We perform a version check at runtime to ensure this. Every new iTwin.js version must be validated, and fixes are rarely ported
to old versions currently, you must request this in an issue. Removing Dependencies on internal APIs is ongoing.
## Environment Variables

### TRANSFORMER_NO_STRICT_DEP_CHECK

Set this variable to `1` to disable strict dependency checks in `packages/transformer/src/imodel-transformer.ts`. The strict dependency check validates that the version of `@itwin/core-backend` provided falls in the range defined by the peer dependencies of the transformer package.

```dotenv
TRANSFORMER_NO_STRICT_DEP_CHECK=1
```

### IMODEL_TRANSFORMER_MAX_ESA_CACHE_SIZE

The transformer preloads per-Kind in-memory indexes of the scope's `ExternalSourceAspect` provenance rows (one query per Kind per run) instead of querying per entity. Set this variable to override the maximum number of rows (per Kind) that may be cached; when a scope has more rows than this limit, the transformer falls back to per-row queries to bound memory usage. Defaults to `4000000`. Set to `0` to disable the cache entirely.

```dotenv
IMODEL_TRANSFORMER_MAX_ESA_CACHE_SIZE=1000000
```
