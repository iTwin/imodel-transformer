import * as fs from "fs";
import * as path from "path";
import { PromiseReturnType, StopWatch } from "@itwin/core-bentley";


export function initOutputFile(fileBaseName: string, outputDir: string) {
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir);
    }
    const outputFileName = path.join(outputDir, fileBaseName);
    if (fs.existsSync(outputFileName)) {
      fs.unlinkSync(outputFileName);
    }
    return outputFileName;
  }
  
  export function timed<F extends (() => any) | (() => Promise<any>)>(
    f: F
  ): [StopWatch, ReturnType<F>] | Promise<[StopWatch, PromiseReturnType<F>]> {
    const stopwatch = new StopWatch();
    stopwatch.start();
    const result = f();
    if (result instanceof Promise) {
      return result.then<[StopWatch, PromiseReturnType<F>]>((innerResult) => {
        stopwatch.stop();
        return [stopwatch, innerResult];
      });
    } else {
      stopwatch.stop();
      return [stopwatch, result];
    }
  }