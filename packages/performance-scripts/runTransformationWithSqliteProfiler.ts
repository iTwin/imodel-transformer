/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import * as path from "path";
import { IModelTransformer, TransformerEvent } from "@itwin/transformer";
import { hookIntoTransformer } from "./hookIntoTransformer";

interface ProfileArgs {
  profileFullName?: string;
}

const hooks = {
  processSchemas(t: IModelTransformer, _args: ProfileArgs) {
    t.events.on(TransformerEvent.beginProcessSchemas, () => {
      t.sourceDb.nativeDb.startProfiler(
        "transformer",
        "processSchemas",
        undefined,
        true
      );
    });

    t.events.on(TransformerEvent.endProcessSchemas, () => {
      const _result = t.sourceDb.nativeDb.stopProfiler();
      // TODO: rename the filename to the name we want
    });
  },

  processAll(t: IModelTransformer, _args: ProfileArgs) {
    t.events.on(TransformerEvent.beginProcessAll, () => {
      t.sourceDb.nativeDb.startProfiler(
        "transformer",
        "processAll",
        undefined,
        true
      );
    });

    t.events.on(TransformerEvent.endProcessAll, () => {
      t.sourceDb.nativeDb.stopProfiler();
    });
  },

  processChanges(t: IModelTransformer, _args: ProfileArgs) {
    t.events.on(TransformerEvent.beginProcessChanges, () => {
      t.sourceDb.nativeDb.startProfiler(
        "transformer",
        "processChanges",
        undefined,
        true
      );
    });

    t.events.on(TransformerEvent.endProcessChanges, () => {
      t.sourceDb.nativeDb.stopProfiler();
    });
  },
};

hookIntoTransformer((
  t: IModelTransformer,
  {
    profileDir = process.env.ITWIN_TESTS_CPUPROF_DIR ?? process.cwd(),
    /** append an ISO timestamp to the name you provided */
    timestamp = true,
    profileName = "profile",
    /** an extension to append to the profileName, including the ".". Defaults to ".sqlite.cpuprofile" */
    profileExtension = ".sqlite.profile",
  } = {}
) => {
  const maybeNameTimePortion = timestamp ? `_${new Date().toISOString()}` : "";
  const profileFullName = `${profileName}${maybeNameTimePortion}${profileExtension}`;
  const profilePath = path.join(profileDir, profileFullName);

  const profArgs = { profileFullName };
  hooks.processAll(t, profArgs);
  hooks.processSchemas(t, profArgs);
  hooks.processChanges(t, profArgs);
});

