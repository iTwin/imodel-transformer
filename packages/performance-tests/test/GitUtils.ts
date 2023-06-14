import util from "util";
import child_proc from "child_process";

export async function getBranchName(): Promise<string> {
  const exec = util.promisify(child_proc.exec);

  const { stdout, stderr } = await exec("git rev-parse --abbrev-ref HEAD");
  if (stderr)
    throw new Error(`exec error: ${stderr}`);
  else
    return stdout.trim();
}
