import * as os from "os";

const profileTypes = ["linux-native", "js-cpu", "sqlite"] as const;
const profileType = process.env.PROFILE_TYPE;

const usageText = `\
To use this package, you should require it before anything else. One easy way to do that is
set the 'NODE_OPTIONS' environment variable like so:

NODE_OPTIONS='--require performance-scripts'

Then run your program.
You must also set in the environment the 'PROFILE_TYPE' and the 'FUNCTIONS' variables.

FUNCTIONS must be a comma-separated list of accessors from either global scope or a require statement
that when evaluated lead to async functions. Sync functions are not currently supported.
For example:

FUNCTIONS='require("@itwin/imodel-transformer").IModelTransformer.prototype.processAll,require("@itwin/imodel-transformer").IModelTransformer.prototype.processSchemas'

PROFILE_TYPE must be a valid profile type, which is one of the following: ${profileTypes.join(", ")}

Each profile type may have its own required settings, which it will complain about and exit
if you do not use them.

The program will now exit.`;

if (!process.env.FUNCTIONS) {
  console.error(usageText)
  process.exit(1);
}

const funcsToInstrument = process.env.FUNCTIONS.split(",").map(s => s.trim());
const funcData = funcsToInstrument.map(f => {
  const dotIndex = f.lastIndexOf('.');
  const object = eval(f.substring(0, dotIndex));
  const key = f.substring(dotIndex + 1);
  return { object, key };
})

switch (profileType) {
  case "linux-native":
    if (os.userInfo().uid !== 0)
      console.warn("You are not running as root, perf may have issues, see stderr.");
    (require("./runWithLinuxPerf") as typeof import("./runWithLinuxPerf"))(funcData);
    break;
  case "js-cpu":
    (require("./runWithJsCpuProfile") as typeof import("./runWithJsCpuProfile"))(funcData);
    break;
  case "sqlite":
    (require("./runTransformationWithSqliteProfiler") as typeof import("./runTransformationWithSqliteProfiler"))(funcData);
    break;
  default:
    console.error(usageText);
    process.exit(1);
}

