import util from "util";
import child_proc from "child_process";

export async function getBranchName(): Promise<string> {
  let branch: string;
  const exec = util.promisify(child_proc.exec);

  const { stdout, stderr } = await exec("git rev-parse --abbrev-ref HEAD");
  if (stderr)
    throw new Error(`exec error: ${stderr}`);
  else
    branch = stdout.trim();
    
  if (branch === "HEAD")
    return "main";
  else
    return branch; 
}
