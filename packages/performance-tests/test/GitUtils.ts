
const util = require('node:util');
const exec = util.promisify(require('node:child_process').exec);

export async function getBranchName(): Promise<string> {
  const { stdout, stderr } = await exec('git rev-parse --abbrev-ref HEAD');
  if (stderr)
    throw new Error(`exec error: ${stderr}`);
  else 
    return stdout.trim();
}
