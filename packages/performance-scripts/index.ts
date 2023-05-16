import * as os from "os";

const profileTypes = ["linux-perf", "js-cpu", "sqlite"] as const;
const profileType = process.env.PROFILE_TYPE;

const usageText = `\
To use this package, you should require it before anything else. One easy way to do that is
set the 'NODE_OPTIONS' environment variable like so:

NODE_OPTIONS='--require performance-scripts'

Then run your program.
You must also set in the environment the 'PROFILE_TYPE' and the 'FUNCTIONS' variables.

FUNCTIONS must be a comma-separated list of accessors from either global scope or a require statement
that evaluate to a function. Some profilers only support async functions.

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

import * as vm from "vm";

const funcsToInstrument = process.env.FUNCTIONS.split(",").map(s => s.trim());
const funcData = funcsToInstrument.map(f => {
  const dotIndex = f.lastIndexOf('.');
  const objExpr = f.substring(0, dotIndex);
  const key = f.substring(dotIndex + 1);

  // this custom require will make the require relative not to this module but to the process
  // current working directory, which is what the user will expect since the FUNCTIONS objects
  // should be evaluated in the context of the actual node script that is being run.
  // One could argue that it would be better to do `path.dirname(process.argv[1])` but I think this
  // is slightly more flexible
  const ctxRequire = (path: string): any => {
    const absPath = require.resolve(path, { paths: [process.cwd()] });
    return require(absPath);
  };

  const context = vm.createContext({ require: ctxRequire });
  const object = vm.runInContext(objExpr, context);
  return { object, key };
})

switch (profileType) {
  case "linux-perf":
    if (os.userInfo().uid !== 0)
      console.warn("You are not running as root, perf may have issues, see stderr.");
    (require("./runWithLinuxPerf") as typeof import("./runWithLinuxPerf")).default(funcData);
    break;
  case "js-cpu":
    (require("./runWithJsCpuProfile") as typeof import("./runWithJsCpuProfile")).default(funcData);
    break;
  case "sqlite":
    (require("./runWithSqliteProfiler") as typeof import("./runWithSqliteProfiler")).default(funcData);
    break;
  default:
    console.error(usageText);
    process.exit(1);
}

