/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "node:path";
import * as fs from "node:fs";
import * as inspector from "node:inspector";
import { runWithCleanup } from "./runWithCleanup";

export interface CpuProfilerOptions {
  profileDir?: string;
  timestamp?: boolean;
  profileName?: string;
  profileExtension?: string;
  sampleIntervalMicroSec?: number;
}

export interface CpuProfileResult<T> {
  profilePath: string;
  result: T;
}

/**
 * Runs a function under the cpu profiler, by default creates cpu profiles in the working directory of
 * the test runner process.
 * You can override the default across all calls with the environment variable ITWIN_TESTS_CPUPROF_DIR,
 * or per function just pass a specific `profileDir`
 */
export async function runWithCpuProfiler<T>(
  f: () => Promise<T>,
  {
    profileDir = process.env.ITWIN_TESTS_CPUPROF_DIR ?? process.cwd(),
    /** append an ISO timestamp to the name you provided */
    timestamp = true,
    profileName = "profile",
    /** an extension to append to the profileName, including the ".". Defaults to ".js.cpuprofile" */
    profileExtension = ".js.cpuprofile",
    /** profile sampling interval in microseconds, you may want to adjust this to increase the resolution of your test
     * default to half a millesecond
     */
    sampleIntervalMicroSec = +(process.env.PROFILE_SAMPLE_INTERVAL ?? 500), // half a millisecond
  }: CpuProfilerOptions = {}
): Promise<CpuProfileResult<T>> {
  const maybeNameTimePortion = timestamp
    ? `_${new Date().toISOString().replace(/[:.]/g, "-")}`
    : "";
  const profilePath = path.join(
    profileDir,
    `${profileName}${maybeNameTimePortion}${profileExtension}`
  );
  await fs.promises.mkdir(profileDir, { recursive: true });
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  // implementation influenced by https://github.com/wallet77/v8-inspector-api/blob/master/src/utils.js
  const invokeFunc = async (
    thisSession: inspector.Session,
    funcName: string,
    args: any = {}
  ) => {
    return new Promise<void>((resolve, reject) => {
      thisSession.post(funcName, args, (err) =>
        err ? reject(err) : resolve()
      );
    });
  };
  const stopProfiler = async (
    thisSession: inspector.Session,
    funcName: "Profiler.stop",
    writePath: string
  ) => {
    return new Promise<void>((resolve, reject) => {
      thisSession.post(funcName, (err, res) => {
        if (err) return reject(err);
        void fs.promises
          .writeFile(writePath, JSON.stringify(res.profile))
          .then(() => resolve(), reject);
      });
    });
  };
  const session = new inspector.Session();
  session.connect();
  let profilerEnabled = false;
  let profilerStarted = false;
  const result = await runWithCleanup(async () => {
    await invokeFunc(session, "Profiler.enable");
    profilerEnabled = true;
    await invokeFunc(session, "Profiler.setSamplingInterval", {
      interval: sampleIntervalMicroSec,
    });
    await invokeFunc(session, "Profiler.start");
    profilerStarted = true;
    return f();
  }, [
    {
      name: "write JavaScript CPU profile",
      run: async () => {
        if (profilerStarted)
          await stopProfiler(session, "Profiler.stop", profilePath);
      },
    },
    {
      name: "disable JavaScript CPU profiler",
      run: async () => {
        if (profilerEnabled) await invokeFunc(session, "Profiler.disable");
      },
    },
    {
      name: "disconnect JavaScript CPU profiler",
      run: () => session.disconnect(),
    },
  ]);
  return { profilePath, result };
}

export default function RunWithJSCpuProfiler(
  funcData: { object: any; key: string }[]
) {
  for (const { object, key } of funcData) {
    const original = object[key];
    object[key] = function (...args: any[]) {
      return runWithCpuProfiler(
        () => {
          const result = original.call(this, ...args);
          const isPromise = Promise.resolve(result) === result;
          if (!isPromise)
            throw Error(
              "runWithCpuProfiler only supports instrumenting async functions!"
            );
          return result;
        },
        { profileName: key }
      ).then(({ result }) => result);
    };
  }
}
