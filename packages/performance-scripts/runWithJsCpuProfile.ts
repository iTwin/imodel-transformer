/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "node:path";
import * as fs from "node:fs";
import * as inspector from "node:inspector";

/**
 * Runs a function under the cpu profiler, by default creates cpu profiles in the working directory of
 * the test runner process.
 * You can override the default across all calls with the environment variable ITWIN_TESTS_CPUPROF_DIR,
 * or per function just pass a specific `profileDir`
 */
export async function runWithCpuProfiler<F extends () => any>(
  f: F,
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
  } = {}
): Promise<ReturnType<F>> {
  const maybeNameTimePortion = timestamp
    ? `_${new Date().toISOString().replace(/[:.]/g, "-")}`
    : "";
  const profilePath = path.join(
    profileDir,
    `${profileName}${maybeNameTimePortion}${profileExtension}`
  );
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
      thisSession.post(funcName, async (err, res) => {
        if (err) return reject(err);
        await fs.promises.writeFile(writePath, JSON.stringify(res.profile));
        resolve();
      });
    });
  };
  const session = new inspector.Session();
  session.connect();
  await invokeFunc(session, "Profiler.enable");
  await invokeFunc(session, "Profiler.setSamplingInterval", {
    interval: sampleIntervalMicroSec,
  });
  await invokeFunc(session, "Profiler.start");
  let result!: Awaited<ReturnType<F>>;
  let operationError: unknown;
  try {
    result = await f();
  } catch (error) {
    operationError = error;
  }

  const cleanupErrors: unknown[] = [];
  try {
    await stopProfiler(session, "Profiler.stop", profilePath);
  } catch (error) {
    cleanupErrors.push(error);
  }
  try {
    await invokeFunc(session, "Profiler.disable");
  } catch (error) {
    cleanupErrors.push(error);
  } finally {
    session.disconnect();
  }

  if (operationError !== undefined) {
    if (cleanupErrors.length > 0)
      throw new AggregateError(
        [operationError, ...cleanupErrors],
        "Profiled operation and profiler cleanup both failed",
        { cause: operationError }
      );
    throw operationError;
  }
  if (cleanupErrors.length === 1) throw cleanupErrors[0];
  if (cleanupErrors.length > 1)
    throw new AggregateError(cleanupErrors, "Profiler cleanup failed");
  return result;
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
              "runWithLinuxPerf only supports instrumenting async functions!"
            );
          return result;
        },
        { profileName: key }
      );
    };
  }
}
