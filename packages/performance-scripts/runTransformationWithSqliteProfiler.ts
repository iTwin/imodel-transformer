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
    for (const db of [t.sourceDb, t.targetDb]) {
      t.events.on(TransformerEvent.beginProcessSchemas, () => {
        db.nativeDb.startProfiler(
          "transformer",
          "processSchemas",
          undefined,
          true
        );
      });

      t.events.on(TransformerEvent.endProcessSchemas, () => {
        const result = db.nativeDb.stopProfiler();
        console.log(result.fileName);
        // TODO: rename the filename to the name we want
      });
    }
  },

  processAll(t: IModelTransformer) {
    for (const db of [t.sourceDb, t.targetDb]) {
      t.events.on(TransformerEvent.beginProcessAll, () => {
        db.nativeDb.startProfiler(
          "transformer",
          "processAll",
          undefined,
          true
        );
      });

      t.events.on(TransformerEvent.endProcessAll, () => {
        const result = db.nativeDb.stopProfiler();
        console.log(result.fileName);
      });
    }
  },

  processChanges(t: IModelTransformer) {
    for (const db of [t.sourceDb, t.targetDb]) {
      t.events.on(TransformerEvent.beginProcessChanges, () => {
        db.nativeDb.startProfiler(
          "transformer",
          "processChanges",
          undefined,
          true
        );
      });

      t.events.on(TransformerEvent.endProcessChanges, () => {
        const result = db.nativeDb.stopProfiler();
        console.log(result.fileName);
      });
    }
  },
};

hookIntoTransformer((t: IModelTransformer) => {
  hooks.processAll(t);
  hooks.processSchemas(t);
  hooks.processChanges(t);
});

