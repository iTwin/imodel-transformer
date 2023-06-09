
export function getBranchName(): string{
  const { exec } = require('child_process');
  let branchName: string = '';
  exec('git rev-parse --abbrev-ref HEAD', (err: boolean, stdout: string, stderr: string) => {
    if (err)
      throw stderr
    branchName = stdout.trim()
  });
  return branchName;
}
