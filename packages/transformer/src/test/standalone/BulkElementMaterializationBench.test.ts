/* eslint-disable no-console */
/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

/**
 * Microbenchmark for issue #7 ("single query bulk getElement").
 *
 * Compares strategies for materializing every Element in an iModel:
 *  (a) per-element `getElement({ wantGeometry: false })` (exporter default path)
 *  (b) per-element `getElement({ wantGeometry: true, wantBRepData: true })`
 *  (c) streamed `SELECT $ FROM bis.Element` + post-processing + constructEntity
 *  (d) `SELECT $` batched by id (mirrors an exporter prefetch-cache pattern)
 *  (e) per-element `getElementProps` (no JS class construction)
 *  (f) raw `SELECT $` streaming + JSON.parse only (isolates query cost)
 *
 * Skipped by default; run with:
 *   RUN_BULK_BENCH=1 pnpm exec vitest run src/test/standalone/BulkElementMaterializationBench.test.ts
 * Optionally set BULK_BENCH_ELEM_COUNT (default 100000).
 */

import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  StandaloneDb,
  withEditTxn,
} from "@itwin/core-backend";
import { DbResult, Id64String, OpenMode } from "@itwin/core-bentley";
import {
  Code,
  ColorDef,
  IModel,
  PhysicalElementProps,
  QueryBinder,
} from "@itwin/core-common";
import { Point3d } from "@itwin/core-geometry";
import * as coreBackendPkgJson from "@itwin/core-backend/package.json";
import { afterAll, assert, beforeAll, describe, expect, it } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";
import { ensureECSqlReaderIsAsyncIterableIterator } from "../../ECSqlReaderAsyncIterableIteratorAdapter";
import {
  instanceRowToElementProps,
  parseInstanceRow,
} from "../TestUtils/InstanceQueryElementUtils";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";

const RUN_BENCH = process.env.RUN_BULK_BENCH !== undefined;
const ELEM_COUNT = Number(process.env.BULK_BENCH_ELEM_COUNT ?? 100_000);
const BATCH_SIZE = 5_000;

interface BenchResult {
  name: string;
  elementCount: number;
  runsMs: number[];
}

const results: BenchResult[] = [];

async function bench(
  name: string,
  runs: number,
  fn: () => Promise<number>
): Promise<void> {
  const runsMs: number[] = [];
  let elementCount = 0;
  for (let i = 0; i < runs; i++) {
    (globalThis as { gc?: () => void }).gc?.();
    const start = performance.now();
    elementCount = await fn();
    runsMs.push(performance.now() - start);
  }
  results.push({ name, elementCount, runsMs });
  console.log(
    `  ${name}: ${runsMs.map((ms) => `${ms.toFixed(0)}ms`).join(", ")} ` +
      `(${elementCount} elements, best ${(
        elementCount /
        (Math.min(...runsMs) / 1000)
      ).toFixed(0)} elems/s)`
  );
}

function printSummary(): void {
  const baseline = results.find((r) => r.name.startsWith("(a)"));
  const lines: string[] = [];
  lines.push("=========================================================");
  lines.push("  bulk getElement microbenchmark");
  lines.push(
    `  core-backend ${coreBackendPkgJson.version}, node ${process.version}`
  );
  lines.push(`  ${ELEM_COUNT} synthetic elements + fixture overhead`);
  lines.push("=========================================================");
  for (const r of results) {
    const best = Math.min(...r.runsMs);
    const vsBaseline = baseline
      ? ` (${((best / Math.min(...baseline.runsMs)) * 100).toFixed(1)}% of baseline)`
      : "";
    lines.push(
      `  ${r.name}\n    best ${best.toFixed(0)}ms, runs [${r.runsMs
        .map((ms) => ms.toFixed(0))
        .join(", ")}]${vsBaseline}`
    );
  }
  lines.push("=========================================================");
  const report = lines.join("\n");
  console.log(`\n${report}\n`);
  const outDir = process.env.BULK_BENCH_OUTPUT_DIR ?? process.cwd();
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, `bulk-bench-results-${ELEM_COUNT}.txt`);
  fs.writeFileSync(outPath, `${report}\n`);
  fs.writeFileSync(
    outPath.replace(/\.txt$/, ".json"),
    JSON.stringify(
      {
        coreBackend: coreBackendPkgJson.version,
        node: process.version,
        elemCount: ELEM_COUNT,
        results,
      },
      undefined,
      2
    )
  );
}

(RUN_BENCH ? describe : describe.skip)(
  "bulk element materialization benchmark (issue #7)",
  () => {
    let db: StandaloneDb;
    let allElementIds: Id64String[] = [];

    beforeAll(async () => {
      const dbPath = IModelTransformerTestUtils.prepareOutputFile(
        "BulkElementMaterialization",
        `bench-${ELEM_COUNT}.bim`
      );
      {
        const snapshot = SnapshotDb.createEmpty(dbPath, {
          rootSubject: { name: "bulk-getelement-bench" },
        });
        snapshot.close();
      }
      db = StandaloneDb.openFile(dbPath, OpenMode.ReadWrite);

      console.log(`inserting ${ELEM_COUNT} elements...`);
      const insertStart = performance.now();
      withEditTxn(db, "insert bench elements", (txn) => {
        const categoryId = SpatialCategory.insert(
          txn,
          IModel.dictionaryId,
          "BenchCategory",
          { color: ColorDef.green.toJSON() }
        );
        const modelId = PhysicalModel.insert(
          txn,
          IModel.rootSubjectId,
          "BenchPhysicalModel"
        );
        const geom = IModelTransformerTestUtils.createBox(
          Point3d.create(1, 1, 1)
        );
        for (let i = 0; i < ELEM_COUNT; i++) {
          const props: PhysicalElementProps = {
            classFullName: PhysicalObject.classFullName,
            model: modelId,
            category: categoryId,
            code: Code.createEmpty(),
            userLabel: `Element-${i}`,
            geom,
            placement: {
              origin: { x: i % 500, y: Math.floor(i / 500), z: 0 },
              angles: {},
            },
            // representative jsonProperties payload on a tenth of elements
            ...(i % 10 === 0 && {
              jsonProperties: {
                UserProps: { bench: { index: i, tags: ["a", "b"] } },
              },
            }),
          };
          txn.insertElement(props);
        }
      });
      console.log(
        `inserted in ${((performance.now() - insertStart) / 1000).toFixed(1)}s`
      );

      allElementIds = [];
      for await (const row of db.createQueryReader(
        "SELECT ECInstanceId FROM bis.Element ORDER BY ECInstanceId"
      )) {
        allElementIds.push(row[0] as Id64String);
      }
      assert(allElementIds.length >= ELEM_COUNT);
    });

    afterAll(() => {
      db?.close();
    });

    it("compares getElement vs SELECT $ materialization", async () => {
      const RUNS = 3;

      await bench("(a) getElement wantGeometry=false", RUNS, async () => {
        let count = 0;
        for (const id of allElementIds) {
          const element = db.elements.getElement({ id, wantGeometry: false });
          if (element !== undefined) count++;
        }
        return count;
      });

      await bench(
        "(b) getElement wantGeometry=true wantBRepData=true",
        RUNS,
        async () => {
          let count = 0;
          for (const id of allElementIds) {
            const element = db.elements.getElement({
              id,
              wantGeometry: true,
              wantBRepData: true,
            });
            if (element !== undefined) count++;
          }
          return count;
        }
      );

      await bench("(c) SELECT $ streamed + constructEntity", RUNS, async () => {
        let count = 0;
        const reader = db.createQueryReader(
          "SELECT $ FROM bis.Element ORDER BY ECInstanceId OPTIONS USE_JS_PROP_NAMES DO_NOT_TRUNCATE_BLOB",
          undefined,
          { usePrimaryConn: true }
        );
        for await (const rowProxy of ensureECSqlReaderIsAsyncIterableIterator(
          reader
        )) {
          const row = parseInstanceRow(rowProxy[0]);
          const { props } = instanceRowToElementProps(row);
          const element = db.constructEntity<Element>(props);
          if (element !== undefined) count++;
        }
        return count;
      });

      await bench(
        `(d) SELECT $ batched by id (batch=${BATCH_SIZE}) + constructEntity`,
        RUNS,
        async () => {
          let count = 0;
          for (
            let offset = 0;
            offset < allElementIds.length;
            offset += BATCH_SIZE
          ) {
            const batch = allElementIds.slice(offset, offset + BATCH_SIZE);
            const params = new QueryBinder().bindIdSet("ids", batch);
            const reader = db.createQueryReader(
              "SELECT $ FROM bis.Element WHERE InVirtualSet(:ids, ECInstanceId) ORDER BY ECInstanceId OPTIONS USE_JS_PROP_NAMES DO_NOT_TRUNCATE_BLOB",
              params,
              { usePrimaryConn: true }
            );
            for await (const rowProxy of ensureECSqlReaderIsAsyncIterableIterator(
              reader
            )) {
              const row = parseInstanceRow(rowProxy[0]);
              const { props } = instanceRowToElementProps(row);
              const element = db.constructEntity<Element>(props);
              if (element !== undefined) count++;
            }
          }
          return count;
        }
      );

      await bench("(e) getElementProps (no construction)", RUNS, async () => {
        let count = 0;
        for (const id of allElementIds) {
          const props = db.elements.getElementProps({
            id,
            wantGeometry: false,
          });
          if (props !== undefined) count++;
        }
        return count;
      });

      await bench("(f) SELECT $ streamed, parse only", RUNS, async () => {
        let count = 0;
        const reader = db.createQueryReader(
          "SELECT $ FROM bis.Element ORDER BY ECInstanceId OPTIONS USE_JS_PROP_NAMES DO_NOT_TRUNCATE_BLOB",
          undefined,
          { usePrimaryConn: true }
        );
        for await (const rowProxy of ensureECSqlReaderIsAsyncIterableIterator(
          reader
        )) {
          const row = parseInstanceRow(rowProxy[0]);
          if (row.id !== undefined) count++;
        }
        return count;
      });

      // fair comparison against (a): geometry blobs truncated, since the
      // wantGeometry=false baseline doesn't materialize them either
      await bench(
        "(g) SELECT $ streamed (truncated blobs) + constructEntity",
        RUNS,
        async () => {
          let count = 0;
          const reader = db.createQueryReader(
            "SELECT $ FROM bis.Element ORDER BY ECInstanceId OPTIONS USE_JS_PROP_NAMES",
            undefined,
            { usePrimaryConn: true }
          );
          for await (const rowProxy of ensureECSqlReaderIsAsyncIterableIterator(
            reader
          )) {
            const row = parseInstanceRow(rowProxy[0]);
            const { props } = instanceRowToElementProps(row);
            const element = db.constructEntity<Element>(props);
            if (element !== undefined) count++;
          }
          return count;
        }
      );

      // synchronous prepared statement, avoiding the concurrent-query
      // machinery entirely (the prototype's late "statement preparation" fix)
      try {
        await bench(
          "(h) sync prepared stmt SELECT $, parse only",
          RUNS,
          async () =>
            // eslint-disable-next-line @typescript-eslint/no-deprecated -- deliberately measuring the sync statement path
            db.withPreparedStatement(
              "SELECT $ FROM bis.Element ORDER BY ECInstanceId",
              (stmt) => {
                let count = 0;
                while (DbResult.BE_SQLITE_ROW === stmt.step()) {
                  const row = parseInstanceRow(stmt.getValue(0).getString());
                  if (row.id !== undefined || row.eCInstanceId !== undefined)
                    count++;
                }
                return count;
              }
            )
        );
      } catch (err) {
        console.log(`  (h) sync prepared stmt SELECT $ failed: ${err}`);
      }

      printSummary();

      // sanity: every strategy visited every element
      for (const result of results) {
        expect(result.elementCount).to.equal(allElementIds.length);
      }
    });
  }
);
