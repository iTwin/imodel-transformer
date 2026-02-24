/* eslint-disable no-console */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/**
 * Performance tests for IModelTransformer.
 *
 * These tests are skipped by default. To run them, remove the `.skip` modifier
 * from individual tests or the entire describe block. Useful for:
 * - Profiling transformation performance
 * - Comparing performance before/after changes
 * - Identifying performance regressions
 */

import { assert } from "chai";
import {
  Code,
  ColorDef,
  IModel,
  PhysicalElementProps,
  Placement3d,
} from "@itwin/core-common";
import { Point3d } from "@itwin/core-geometry";
import {
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
} from "@itwin/core-backend";
import * as coreBackendPkgJson from "@itwin/core-backend/package.json";
import { IModelTransformer } from "../../IModelTransformer";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";

import "./TransformerTestStartup"; // calls startup/shutdown IModelHost before/after all tests

function printResults(results: {
  elementCount: number;
  insertionMs: number;
  schemaMs: number;
  processMs: number;
}): void {
  const separator = "===========================================";
  const avgPerElement = results.processMs / results.elementCount;

  console.log(separator);
  console.log(`  Results: core-backend ${coreBackendPkgJson.version}`);
  console.log(separator);
  console.log(`  Elements: ${results.elementCount}`);
  console.log(`  Element insertion: ${results.insertionMs.toFixed(2)} ms`);
  console.log(`  Schema processing: ${results.schemaMs.toFixed(2)} ms`);
  console.log(`  process() duration: ${results.processMs.toFixed(2)} ms`);
  console.log(`  Avg per element: ${avgPerElement.toFixed(4)} ms`);
  console.log(separator);
}

describe.skip("IModelTransformer Performance Tests", () => {
  it("should transform 10k elements", async function () {
    this.timeout(120000); // 2 minutes for large element count

    const elementCount = 10000;

    // Create source iModel
    const sourceDbFile = IModelTransformerTestUtils.prepareOutputFile(
      "IModelTransformer",
      "Source10kElements.bim"
    );
    const sourceDb = SnapshotDb.createEmpty(sourceDbFile, {
      rootSubject: { name: "Source 10k Elements Test" },
    });

    // Set up category and model
    const categoryId = SpatialCategory.insert(
      sourceDb,
      IModel.dictionaryId,
      "TestCategory",
      { color: ColorDef.green.toJSON() }
    );
    const modelId = PhysicalModel.insert(
      sourceDb,
      IModel.rootSubjectId,
      "TestPhysicalModel"
    );

    // Insert 10k elements
    const geom = IModelTransformerTestUtils.createBox(Point3d.create(1, 1, 1));
    const insertStartTime = performance.now();
    for (let i = 0; i < elementCount; i++) {
      const physicalObjectProps: PhysicalElementProps = {
        classFullName: PhysicalObject.classFullName,
        model: modelId,
        category: categoryId,
        code: Code.createEmpty(),
        userLabel: `Element-${i}`,
        geom,
        placement: Placement3d.fromJSON({
          origin: { x: i % 100, y: Math.floor(i / 100), z: 0 },
          angles: {},
        }),
      };
      sourceDb.elements.insertElement(physicalObjectProps);
    }
    const insertEndTime = performance.now();
    sourceDb.saveChanges();

    // Verify source element count
    let sourceCount = 0;
    for await (const _row of sourceDb.createQueryReader(
      `SELECT COUNT(*) FROM ${PhysicalObject.classFullName}`
    )) {
      sourceCount = _row[0] as number;
    }
    assert.equal(sourceCount, elementCount, "Source should have 10k elements");

    // Create empty target iModel
    const targetDbFile = IModelTransformerTestUtils.prepareOutputFile(
      "IModelTransformer",
      "Target10kElements.bim"
    );
    const targetDb = SnapshotDb.createEmpty(targetDbFile, {
      rootSubject: { name: "Target 10k Elements Test" },
    });

    // Transform
    const transformer = new IModelTransformer(sourceDb, targetDb);

    const schemasStartTime = performance.now();
    await transformer.processSchemas();
    const schemasEndTime = performance.now();

    const startTime = performance.now();
    await transformer.process();
    const endTime = performance.now();
    targetDb.saveChanges();

    printResults({
      elementCount,
      insertionMs: insertEndTime - insertStartTime,
      schemaMs: schemasEndTime - schemasStartTime,
      processMs: endTime - startTime,
    });

    // Verify target element count
    let targetCount = 0;
    for await (const _row of targetDb.createQueryReader(
      `SELECT COUNT(*) FROM ${PhysicalObject.classFullName}`
    )) {
      targetCount = _row[0] as number;
    }
    assert.equal(targetCount, elementCount, "Target should have 10k elements");

    // Cleanup
    transformer.dispose();
    sourceDb.close();
    targetDb.close();
  });
});
