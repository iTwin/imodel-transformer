# @itwin/imodel-transformer

Copyright © Bentley Systems, Incorporated. All rights reserved. See LICENSE.md for license terms and full copyright notice.

## Description

The **@itwin/imodel-transformer** package contains classes that handle traversing iModels for exporting and importing their parts.

## Module format

`@itwin/imodel-transformer` is an ESM-only package. Import its public API from the package root:

```ts
import {
  IModelTransformer,
  type IModelTransformContext,
} from "@itwin/imodel-transformer";
```

CommonJS `require()` and implementation paths such as `@itwin/imodel-transformer/lib/cjs/*` are not supported. Node applications must run as ESM, and TypeScript applications should use `module` and `moduleResolution` set to `NodeNext`.

### Package smoke test

After building the package, run `pnpm test:package` from `packages/transformer`. The test packs the publishable tarball and validates it from a clean consumer rather than importing from the workspace source tree.

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
