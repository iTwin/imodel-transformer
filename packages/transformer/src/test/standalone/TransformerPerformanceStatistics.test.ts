/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert } from "chai";
import * as sinon from "sinon";
import { SnapshotDb } from "@itwin/core-backend";
import {
  IModelTransformer,
  IModelTransformOptions,
} from "../../IModelTransformer";
import {
  clearExporterPerformanceCollector,
  getExporterPerformanceCollector,
  setExporterPerformanceCollector,
  TransformerPerformanceCollector,
  TransformerPerformanceOperation,
} from "../../TransformerPerformanceStatistics";
import {
  createStartedEditTxn,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";

import "./TransformerTestStartup";

describe.only("Transformer performance statistics", () => {
  it("only clears exporter measurements owned by the disposing collector", () => {
    const exporter = {};
    const firstCollector = new TransformerPerformanceCollector();
    const secondCollector = new TransformerPerformanceCollector();
    setExporterPerformanceCollector(exporter, firstCollector);
    setExporterPerformanceCollector(exporter, secondCollector);

    clearExporterPerformanceCollector(exporter, firstCollector);

    assert.strictEqual(
      getExporterPerformanceCollector(exporter),
      secondCollector
    );
  });

  it("aggregates successful and failed measurements", async () => {
    const times = [0, 5, 10, 17];
    const collector = new TransformerPerformanceCollector(() => {
      const time = times.shift();
      if (time === undefined) throw new Error("No test time available");
      return time;
    });

    await collector.measure(
      TransformerPerformanceOperation.ElementsAndModels,
      async () => {}
    );
    try {
      await collector.measure(
        TransformerPerformanceOperation.ElementsAndModels,
        async () => {
          throw new Error("expected");
        }
      );
      assert.fail("Expected measurement action to throw");
    } catch (error) {
      assert.equal((error as Error).message, "expected");
    }

    const statistics = collector.getStatistics();
    const metric =
      statistics.operations[TransformerPerformanceOperation.ElementsAndModels];
    assert.deepEqual(metric, {
      invocationCount: 2,
      totalMilliseconds: 12,
      maximumMilliseconds: 7,
      failureCount: 1,
    });
    assert.isTrue(Object.isFrozen(statistics));
    assert.isTrue(Object.isFrozen(statistics.operations));
    assert.isTrue(Object.isFrozen(metric));
  });

  it("collects phase measurements without repeating synchronous work", async () => {
    const sourceDb = SnapshotDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "TransformerPerformanceStatistics",
        "Source.bim"
      ),
      { rootSubject: { name: "Performance statistics source" } }
    );
    const targetDb = SnapshotDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "TransformerPerformanceStatistics",
        "Target.bim"
      ),
      { rootSubject: { name: "Performance statistics target" } }
    );
    const editTxn = createStartedEditTxn(targetDb);
    const transformer = new IModelTransformer(
      { source: sourceDb, target: editTxn },
      { collectPerformanceStatistics: true, noProvenance: true }
    );
    const computeProjectExtentsSpy = sinon.spy(
      transformer.importer,
      "computeProjectExtents"
    );

    try {
      await transformer.processSchemas();
      await transformer.process();

      const statistics = transformer.getPerformanceStatistics();
      assert.isDefined(statistics);
      assert.equal(
        statistics?.operations[TransformerPerformanceOperation.Schemas]
          ?.invocationCount,
        1
      );
      assert.equal(
        statistics?.operations[TransformerPerformanceOperation.Process]
          ?.invocationCount,
        1
      );
      assert.equal(
        statistics?.operations[TransformerPerformanceOperation.Initialization]
          ?.failureCount,
        0
      );
      assert.equal(
        statistics?.operations[
          TransformerPerformanceOperation.ElementsAndModels
        ]?.invocationCount,
        1
      );
      assert.equal(
        statistics?.operations[TransformerPerformanceOperation.Finalization]
          ?.invocationCount,
        1
      );
      sinon.assert.calledOnce(computeProjectExtentsSpy);
    } finally {
      transformer.dispose();
      editTxn.end();
      sourceDb.close();
      targetDb.close();
    }
  });

  it("does not collect statistics unless enabled", () => {
    const sourceDb = SnapshotDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "TransformerPerformanceStatistics",
        "DisabledSource.bim"
      ),
      { rootSubject: { name: "Disabled statistics source" } }
    );
    const targetDb = SnapshotDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "TransformerPerformanceStatistics",
        "DisabledTarget.bim"
      ),
      { rootSubject: { name: "Disabled statistics target" } }
    );
    const editTxn = createStartedEditTxn(targetDb);
    const transformer = new IModelTransformer({
      source: sourceDb,
      target: editTxn,
    });

    try {
      assert.isUndefined(transformer.getPerformanceStatistics());
    } finally {
      transformer.dispose();
      editTxn.end();
      sourceDb.close();
      targetDb.close();
    }
  });

  it("records failed process calls", async () => {
    class FailingTransformer extends IModelTransformer {
      public constructor(
        source: SnapshotDb,
        target: ReturnType<typeof createStartedEditTxn>,
        options: IModelTransformOptions
      ) {
        super({ source, target }, options);
      }

      public override async initialize(): Promise<void> {
        throw new Error("expected");
      }
    }

    const sourceDb = SnapshotDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "TransformerPerformanceStatistics",
        "FailureSource.bim"
      ),
      { rootSubject: { name: "Failed statistics source" } }
    );
    const targetDb = SnapshotDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "TransformerPerformanceStatistics",
        "FailureTarget.bim"
      ),
      { rootSubject: { name: "Failed statistics target" } }
    );
    const editTxn = createStartedEditTxn(targetDb);
    const transformer = new FailingTransformer(sourceDb, editTxn, {
      collectPerformanceStatistics: true,
    });

    try {
      try {
        await transformer.process();
        assert.fail("Expected process to throw");
      } catch (error) {
        assert.equal((error as Error).message, "expected");
      }

      assert.equal(
        transformer.getPerformanceStatistics()?.operations[
          TransformerPerformanceOperation.Process
        ]?.failureCount,
        1
      );
    } finally {
      transformer.dispose();
      editTxn.end();
      sourceDb.close();
      targetDb.close();
    }
  });
});
