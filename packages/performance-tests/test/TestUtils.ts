import * as fs from "fs";
import * as path from "path";
import { assert } from "chai";
import { PromiseReturnType, StopWatch } from "@itwin/core-bentley";
import { TestIModel } from "./TestContext";


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

// Mocha tests must know the test cases ahead time, so we collect the the Imodels first before beginning the tests
export async function preFetchAsyncIterator<T>(iter: AsyncGenerator<T>): Promise<T[]> {
  let elements:T[] = [];
  for await (const elem of iter) {
    elements.push(elem)
  }
  return elements;
}

export function filterIModels(iModel:TestIModel): boolean{
  const iModelIdStr = process.env.IMODEL_IDS;
  assert(iModelIdStr, "no Imodel Ids");
  const iModelIds = iModelIdStr === "*" ? "" : iModelIdStr.split(",");
  if(iModelIds.includes(iModel.iModelId) || iModelIds === "" )
    return true;
  else
    return false;
}