/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import * as path from "path";
import * as fs from "fs";
import { IModelDb } from "@itwin/core-backend";

const profName = (profileName: string) => {
  const profileDir = process.env.ITWIN_TESTS_CPUPROF_DIR ?? process.cwd();
  const profileExtension = ".sqliteprofile.db";
  const nameTimePortion = `_${new Date().toISOString().replace(":", "_")}`;
  return path.join(profileDir, `${profileName}${nameTimePortion}${profileExtension}`);
}

export default function RunWithSqliteProfiler(funcData: { object: any, key: string }[]) {
  for (const { object, key } of funcData) {
    const original = object[key];
    object[key] = (...args: any[]) => {
      const db = args.find(a => a instanceof IModelDb);
      if (db === undefined)
        throw Error("no argument of the instrumented function was an IModelDb to profile")
      db.nativeDb.startProfiler("transformer", "processChanges", true, true);
      const result = original.call(object, ...args);
      const profileResult = db.nativeDb.stopProfiler();
      try {
        // This fails on Windows OS because the file is still locked at this point so we swallow the error.
        if (profileResult.fileName)
          fs.renameSync(profileResult.fileName, profName(key));
      } catch (err) {
          console.error(err);
      }
      return result;
    };
  }
};


