/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

const usageText = `\
To use this the 'linux-perf' PROFILE_TYPE, you must include the
"--perf-prof" and "--interpreted-frames-native-stack" arguments
in the NODE_OPTIONS environment variables, like so:

NODE_OPTIONS='--perf-prof --interpreted-frames-native-stack --require performance-scripts'
PROFILE_TYPE='linux-perf'

You may also provide the following variables:

PROFILE_SAMPLE_RATE='99' # the stack samples per second taken by perf (passed to perf's -F option)
ITWIN_TESTS_CPUPROF_DIR="process.cwd()" # the directoty in which to write the resulting profile
# this one may be replaced in the future with better readiness detection
PERF_WARMUP_DELAY='500' # the amount of time to wait for perf to initialize before running the instrumented functions

The program will now exit.
`;

if (![
  "--perf-prof",
  "--interpreted-frames-native-stack",
].every(a => process.env.NODE_OPTIONS?.includes(a))) {
  console.error(usageText);
}

import * as path from "path";
import * as fs from "fs";
import * as v8 from "v8";
import * as child_process from "child_process";

let attachedLinuxPerf: child_process.ChildProcess | undefined = undefined;

/**
 * Attaches linux's perf, by default creates cpu profiles in the working directory of
 * the test runner process.
 * You can override the default across all calls with the environment variable ITWIN_TESTS_CPUPROF_DIR,
 * or per function just pass a specific `profileDir`
 */
export async function runWithLinuxPerf<F extends () => any>(
  f: F,
  {
    profileDir = process.env.ITWIN_TESTS_CPUPROF_DIR ?? process.cwd(),
    /** append an ISO timestamp to the name you provided */
    timestamp = true,
    profileName = "profile",
    /** an extension to append to the profileName, including the ".". Defaults to ".cpp.cpuprofile" */
    profileExtension = ".cpp.cpuprofile",
    /** profile sampling interval in microseconds, you may want to adjust this to increase the resolution of your test
     * default to half a millesecond
     */
    sampleHertz = process.env.PROFILE_SAMPLE_RATE ?? 99,
  } = {}
): Promise<ReturnType<F>> {
  if (attachedLinuxPerf !== undefined)
    throw Error("tried to attach, but perf was already attached!");

  // this does not work, so we error on it at the top
  v8.setFlagsFromString("--perf-prof --interpreted-frames-native-stack");

  // TODO: add an environment variable that names a command to run to get the password and use sudo,
  // so that we don't need to run the whole thing as root
  attachedLinuxPerf = child_process.spawn(
    "perf",
    ["record", "-F", `${sampleHertz}`, "-g", "-p", `${process.pid}`],
    { stdio: "inherit" }
  );

  await new Promise((res, rej) => attachedLinuxPerf!.on("spawn", res).on("error", rej));

  // FIXME: listen for some verbose perf output or something to determine when it's started listening. Or maybe wait for SIGPROF?
  // give perf a moment to attach
  const perfWarmupDelay = +(process.env.PERF_WARMUP_DELAY || 500);
  await new Promise(r => setTimeout(r, perfWarmupDelay));

  const result = await f();

  const attachedPerfExited = new Promise((res, rej) => attachedLinuxPerf!.on("exit", res).on("error", rej));
  attachedLinuxPerf.kill("SIGTERM");
  await attachedPerfExited;

  const maybeNameTimePortion = timestamp ? `_${new Date().toISOString()}` : "";
  const profilePath = path.join(profileDir, `${profileName}${maybeNameTimePortion}${profileExtension}`);

  const perfDump = child_process.spawn(
    "perf",
    ["script"],
    { stdio: ["inherit", "pipe", "inherit"] }
  );

  const outStream = fs.createWriteStream(profilePath);
  perfDump.stdout.pipe(outStream);

  await new Promise((res, rej) => perfDump.on("exit", res).on("error", rej));
  outStream.close(); // doesn't seem to flush when the pipe closes

  try {
    await fs.promises.unlink("perf.data");
  } catch {}

  attachedLinuxPerf = undefined;
  return result;
}

export default function RunWithLinuxPerf(funcData: { object: any, key: string }[]) {
  for (const { object, key } of funcData) {
    const original = object[key];
    object[key] = function (...args: any[]) {
      return runWithLinuxPerf(() => {
        const result = original.call(this, ...args);
        const isPromise = Promise.resolve(result) === result;
        if (!isPromise)
          throw Error("runWithLinuxPerf only supports instrumenting async functions!")
        return result;
      }, { profileName: key });
    };
  }
};

