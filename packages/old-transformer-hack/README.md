# old-transformer-hack

## Description

This package exists solely for testing the @itwin/imodel-transformer. It reexports an older version of the iModelTransformer (currently 0.4.4-dev.0) so that iModels can be tested in the transition from old transformer to new transformer (^1.x). 

See packages\transformer\src\test\TestUtils\TimelineTestUtil.ts for its use.


`import { IModelTransformer as OldIModelTransformer } from "old-transformer-hack"`;

## Notes

Currently both the old transformer and new transformer work with the same versions of @itwin/core-backend package. This may no longer be true in the future and may require additional changes to support having two versions of the transformer with different versions of itwinjs.