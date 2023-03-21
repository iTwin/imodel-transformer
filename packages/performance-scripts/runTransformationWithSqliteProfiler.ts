/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import * as path from "path";
import * as fs from "fs";
import { IModelTransformer, TransformerEvent } from "@itwin/transformer";
import { hookIntoTransformer } from "./hookIntoTransformer";

interface ProfileArgs {
  profileFullName?: string;
}

const hooks = {
  processSchemas(t: IModelTransformer) {
    t.events.on(TransformerEvent.beginProcessSchemas, () => {
      t.sourceDb.nativeDb.startProfiler(
        "transformer",
        "processSchemas",
        undefined,
        true
      );
    });

    t.events.on(TransformerEvent.endProcessSchemas, () => {
      const result = t.sourceDb.nativeDb.stopProfiler();
      console.log(result.fileName);
      // TODO: rename the filename to the name we want
    });
  },

  processAll(t: IModelTransformer) {
    t.events.on(TransformerEvent.beginProcessAll, () => {
      t.sourceDb.nativeDb.startProfiler(
        "transformer",
        "processAll",
        undefined,
        true
      );
    });

    t.events.on(TransformerEvent.endProcessAll, () => {
      const result = t.sourceDb.nativeDb.stopProfiler();
      console.log(result.fileName);
    });
  },

  processChanges(t: IModelTransformer) {
    t.events.on(TransformerEvent.beginProcessChanges, () => {
      t.sourceDb.nativeDb.startProfiler(
        "transformer",
        "processChanges",
        undefined,
        true
      );
    });

    t.events.on(TransformerEvent.endProcessChanges, () => {
      const result = t.sourceDb.nativeDb.stopProfiler();
      console.log(result.fileName);
    });
  },
};

hookIntoTransformer((t: IModelTransformer) => {
  hooks.processAll(t);
  hooks.processSchemas(t);
  hooks.processChanges(t);
});

