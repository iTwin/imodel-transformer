/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import * as fs from "node:fs";
import * as path from "node:path";
import { assert } from "chai";
import { IModelDb } from "@itwin/core-backend";
import { DbResult, StopWatch } from "@itwin/core-bentley";
import { GeometryStreamBuilder, GeometryStreamProps } from "@itwin/core-common";
import { Box, Point3d, Vector3d } from "@itwin/core-geometry";
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

export function createBox(size: Point3d): GeometryStreamProps {
  const box = Box.createDgnBox(
    Point3d.createZero(),
    Vector3d.unitX(),
    Vector3d.unitY(),
    new Point3d(0, 0, size.z),
    size.x,
    size.y,
    size.x,
    size.y,
    true
  );
  if (box === undefined) {
    throw new Error("Unable to create box geometry");
  }

  const geometryStreamBuilder = new GeometryStreamBuilder();
  geometryStreamBuilder.appendGeometry(box);
  return geometryStreamBuilder.geometryStream;
}

export function count(
  iModelDb: IModelDb,
  classFullName: string,
  whereClause?: string
): number {
  // eslint-disable-next-line @typescript-eslint/no-deprecated
  return iModelDb.withPreparedStatement(
    `SELECT COUNT(*) FROM ${classFullName}${
      whereClause ? ` WHERE ${whereClause}` : ""
    }`,
    (statement): number => {
      return DbResult.BE_SQLITE_ROW === statement.step()
        ? statement.getValue(0).getInteger()
        : 0;
    }
  );
}

type PromiseInnerType<T> = T extends Promise<infer R> ? R : never;

// eslint-disable-next-line @typescript-eslint/no-redundant-type-constituents
export function timed<R extends any | Promise<any>>(
  f: () => R
): R extends Promise<any>
  ? Promise<[StopWatch, PromiseInnerType<R>]>
  : [StopWatch, R] {
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
export async function preFetchAsyncIterator<T>(
  iter: AsyncGenerator<T>
): Promise<T[]> {
  const elements: T[] = [];
  for await (const elem of iter) {
    elements.push(elem);
  }
  return elements;
}

export function filterIModels(iModel: TestIModel): boolean {
  const iModelIdStr = process.env.IMODEL_IDS;
  assert(iModelIdStr, "no Imodel Ids");
  const iModelIds = iModelIdStr === "*" ? "" : iModelIdStr.split(",");
  return iModelIds.includes(iModel.iModelId) || iModelIds === "";
}
