import util = require("node:util");
import childPoc = require("node:child_process");

export async function getBranchName(): Promise<string> {
  const exec = util.promisify(childPoc.exec);

  const { stdout, stderr } = await exec("git rev-parse --abbrev-ref HEAD");
  if (stderr)
    throw new Error(`exec error: ${stderr}`);
  else
    return stdout.trim();
}
