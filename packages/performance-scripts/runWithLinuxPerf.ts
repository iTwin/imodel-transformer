/*---------------------------------------------------------------------------------------------
* Copyright (c) Bentley Systems, Incorporated. All rights reserved.
* See LICENSE.md in the project root for license terms and full copyright notice.
*--------------------------------------------------------------------------------------------*/

import * as path from "path";
import * as fs from "fs";
import * as v8 from "v8";
import * as child_process from "child_process";

import { IModelTransformer } from "@itwin/transformer";
import { hookIntoTransformer } from "./hookIntoTransformer";

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
    sampleHertz = process.env.PROFILE_SAMPLE_RATe ?? 99,
  } = {}
): Promise<ReturnType<F>> {
  if (attachedLinuxPerf !== undefined)
    throw Error("tried to attach, but perf was already attached!");

  v8.setFlagsFromString("--perf-prof --interpreted-frames-native-stack");

  // TODO: add an environment variable that names a command to run to get the password and use sudo,
  // so that we don't need to run the whole thing as root
  attachedLinuxPerf = child_process.spawn(
    "perf",
    ["record", "-F", `${sampleHertz}`, "-g", "-p", `${process.pid}`],
  );

  await new Promise((res, rej) => attachedLinuxPerf!.on("spawn", res).on("error", rej));

  // give perf a moment to attach
  await new Promise(r => setTimeout(r, 25));

  const result = await f();

  const maybeNameTimePortion = timestamp ? `_${new Date().toISOString()}` : "";
  const profilePath = path.join(profileDir, `${profileName}${maybeNameTimePortion}${profileExtension}`);

  attachedLinuxPerf.kill();

  const perfDump = child_process.spawn(
    "perf",
    ["script"],
    { stdio: ["inherit", "pipe", "inherit"] }
  );

  await new Promise((res, rej) => perfDump.on("exit", res).on("error", rej));

  perfDump.stdout.pipe(fs.createWriteStream(profilePath));

  try {
    await fs.promises.unlink("perf.data");
  } catch {}

  attachedLinuxPerf = undefined;
  return result;
}

type LinuxPerfProfArgs = Parameters<typeof runWithLinuxPerf>[1];

hookIntoTransformer((t: IModelTransformer) => {
  const originalProcessAll = t.processAll;
  const originalProcessSchemas = t.processSchemas;
  const originalProcessChanges = t.processChanges;

  const profArgs: LinuxPerfProfArgs = {};

  t.processAll = async (...args: Parameters<typeof t.processAll>) =>
    runWithLinuxPerf(() => originalProcessAll.call(t, ...args), { ...profArgs, profileName: "processAll" });
  t.processSchemas = async (...args: Parameters<typeof t.processSchemas>) =>
    runWithLinuxPerf(() => originalProcessSchemas.call(t, ...args), { ...profArgs, profileName: "processSchemas" });
  t.processChanges = async (...args: Parameters<typeof t.processChanges>) =>
    runWithLinuxPerf(() => originalProcessChanges.call(t, ...args), { ...profArgs, profileName: "processChanges" });
});

