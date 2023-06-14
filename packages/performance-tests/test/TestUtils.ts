import * as fs from "fs";
import * as path from "path";
import { assert } from "chai";
import { StopWatch } from "@itwin/core-bentley";
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

type PromiseInnerType<T> = T extends Promise<infer R> ? R : never;

export function timed<R extends any | Promise<any>>(
  f: () => R
): R extends Promise<any> ? Promise<[StopWatch, PromiseInnerType<R>]> : [StopWatch, R] {
  const stopwatch = new StopWatch();
  stopwatch.start();
  const result = f();
  if (result instanceof Promise) {
    return result.then((innerResult) => {
      stopwatch.stop();
      return [stopwatch, innerResult];
    }) as any; // stupid type system
  } else {
    stopwatch.stop();
    return [stopwatch, result] as any;
  }
}

// Mocha tests must know the test cases ahead time, so we collect the the Imodels first before beginning the tests
export async function preFetchAsyncIterator<T>(iter: AsyncGenerator<T>): Promise<T[]> {
  const elements: T[] = [];
  for await (const elem of iter) {
    elements.push(elem);
  }
  return elements;
}

export function filterIModels(iModel: TestIModel): boolean{
  const iModelIdStr = process.env.IMODEL_IDS;
  assert(iModelIdStr, "no Imodel Ids");
  const iModelIds = iModelIdStr === "*" ? "" : iModelIdStr.split(",");
  return iModelIds.includes(iModel.iModelId) || iModelIds === "";
}
