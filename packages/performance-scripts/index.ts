/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import { resolve } from "import-meta-resolve";
import { createRequire } from "node:module";
import * as os from "node:os";
import * as path from "node:path";
import { pathToFileURL } from "node:url";

const profileTypes = ["linux-perf", "js-cpu", "sqlite"] as const;
const profileType = process.env.PROFILE_TYPE;

const usageText = `\
To use this package, import it before anything else by setting the 'NODE_OPTIONS'
environment variable like so:

NODE_OPTIONS='--import @bentley/hook-profiler'

Then run your program.
You must also set in the environment the 'PROFILE_TYPE' and the 'FUNCTIONS' variables.

FUNCTIONS must be a comma-separated list of expressions that evaluate to functions. Use load() to
resolve an ESM or CommonJS package from the profiled program's working directory, or require() for a
CommonJS package. Some profilers only support async functions.

For example:

FUNCTIONS='(await load("@itwin/imodel-transformer")).IModelTransformer.prototype.processAll,(await load("@itwin/imodel-transformer")).IModelTransformer.prototype.processSchemas'

PROFILE_TYPE must be a valid profile type, which is one of the following: ${profileTypes.join(", ")}

Each profile type may have its own required settings, which it will complain about and exit
if you do not use them.

The program will now exit.`;

if (!process.env.FUNCTIONS) {
  console.error(usageText);
  process.exit(1);
}

const contextUrl = pathToFileURL(path.resolve(process.cwd(), "package.json"));
const contextRequire = createRequire(contextUrl);
const load = async (specifier: string): Promise<unknown> =>
  import(resolve(specifier, contextUrl.href));
const AsyncFunction = Object.getPrototypeOf(async () => undefined)
  .constructor as new (
  ...args: string[]
) => (...values: unknown[]) => Promise<unknown>;

const funcsToInstrument = process.env.FUNCTIONS.split(",").map((s) => s.trim());
const funcData = await Promise.all(
  funcsToInstrument.map(async (expression) => {
    const dotIndex = expression.lastIndexOf(".");
    const objectExpression = expression.substring(0, dotIndex);
    const key = expression.substring(dotIndex + 1);
    const evaluate = new AsyncFunction(
      "load",
      "require",
      `return ${objectExpression}`
    );
    const object = await evaluate(load, contextRequire);
    return { object, key };
  })
);

switch (profileType) {
  case "linux-perf":
    if (os.userInfo().uid !== 0)
      console.warn(
        "You are not running as root, perf may have issues, see stderr."
      );
    (await import("./runWithLinuxPerf.js")).default(funcData);
    break;
  case "js-cpu":
    (await import("./runWithJsCpuProfile.js")).default(funcData);
    break;
  case "sqlite":
    (await import("./runWithSqliteProfiler.js")).default(funcData);
    break;
  default:
    console.error(usageText);
    process.exit(1);
}
