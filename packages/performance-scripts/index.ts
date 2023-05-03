
import * as os from "os";

const profileTypes = ["linux-native", "js-cpu", "sqlite"] as const;
const profileType = process.env.PROFILE_TYPE;

const usageText = `\
To use this package, you should require it before anything else. One easy way to do that is
set the 'NODE_OPTIONS' environment variable like so:

NODE_OPTIONS='--require performance-scripts'

Then run your program.
You must also set in the environment the 'PROFILE_TYPE' variable.

Valid profile types are: ${profileTypes.join(", ")}

Each profile type may have its own required settings, which it will complain about and exit
if you do not use them.

The program will now exit.`;

switch (profileType) {
  case "linux-native":
    if (os.userInfo().uid !== 0)
      console.warn("You are not running as root, perf may have issues, see stderr.");
    require("./runWithLinuxPerf");
    break;
  case "js-cpu":
    require("./runWithJsCpuProfile");
    break;
  case "sqlite":
    require("./runTransformationWithSqliteProfiler");
    break;
  default:
    console.error(usageText);
    process.exit(1);
}

