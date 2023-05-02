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

type IModelDb = IModelTransformer["sourceDb"];

const profName = (profileName: string) => {
  const profileDir = process.env.ITWIN_TESTS_CPUPROF_DIR ?? process.cwd();
  const profileExtension = ".sqliteprofile.db";
  const nameTimePortion = `_${new Date().toISOString().replace(":", "_")}`;
  return path.join(profileDir, `${profileName}${nameTimePortion}${profileExtension}`);
}

// FIXME: make this a function!
const hooks = {
  processSchemas(t: IModelTransformer) {
    for (const [db, type] of [[t.sourceDb, "source"], [t.targetDb, "target"]] as const) {
      t.events.on(TransformerEvent.beginProcessSchemas, () => {
        db.nativeDb.startProfiler("transformer", "processSchemas", true, true);
      });

      t.events.on(TransformerEvent.endProcessSchemas, () => {
        const result = db.nativeDb.stopProfiler();
        try {
            // This fails on Windows OS because the file is still locked at this point so we swallow the error.
            if (result.fileName)
              fs.renameSync(result.fileName, profName(`processSchemas_${type}`));
        } catch (err) {
            err;
        }
      });
    }
  },

  processAll(t: IModelTransformer) {
    for (const [db, type] of [[t.sourceDb, "source"], [t.targetDb, "target"]] as const) {
      t.events.on(TransformerEvent.beginProcessAll, () => {
        db.nativeDb.startProfiler("transformer", "processAll", true, true);
      });

      t.events.on(TransformerEvent.endProcessAll, () => {
        const result = db.nativeDb.stopProfiler();
        try {
          // This fails on Windows OS because the file is still locked at this point so we swallow the error.
          if (result.fileName)
            fs.renameSync(result.fileName, profName(`processAll_${type}`));
        } catch (err) {
            err;
        }
      });
    }
  },

  processChanges(t: IModelTransformer) {
    for (const [db, type] of [[t.sourceDb, "source"], [t.targetDb, "target"]] as const) {
      t.events.on(TransformerEvent.beginProcessChanges, () => {
        db.nativeDb.startProfiler("transformer", "processChanges", true, true);
      });

      t.events.on(TransformerEvent.endProcessChanges, () => {
        const result = db.nativeDb.stopProfiler();
        try {
          // This fails on Windows OS because the file is still locked at this point so we swallow the error.
          if (result.fileName)
            fs.renameSync(result.fileName, profName(`processChanges_${type}`));
        } catch (err) {
            err;
        }
      });
    }
  },
};

hookIntoTransformer((t: IModelTransformer) => {
  hooks.processAll(t);
  hooks.processSchemas(t);
  hooks.processChanges(t);
});

