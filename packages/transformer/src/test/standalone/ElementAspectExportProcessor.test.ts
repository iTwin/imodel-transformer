/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  ElementAspect,
  ElementMultiAspect,
  ElementOwnsMultiAspects,
  ElementOwnsUniqueAspect,
  ElementUniqueAspect,
  SnapshotDb,
  Subject,
  withEditTxn,
} from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import { ElementAspectProps, IModel } from "@itwin/core-common";
import { expect, vi } from "vitest";
import {
  ElementAspectExportProcessor,
  ElementAspectExportProcessorHandler,
} from "../../ElementAspectExportProcessor";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";
import { importElementAspectTestSchema } from "../TestUtils/ElementAspectTestUtils";

function createHandler(
  overrides: Partial<ElementAspectExportProcessorHandler> = {}
): ElementAspectExportProcessorHandler {
  return {
    shouldExportElementAspect: async () => true,
    onExportElementUniqueAspect: async () => {},
    onExportElementMultiAspects: async () => {},
    trackProgress: async () => {},
    ...overrides,
  };
}

describe("ElementAspectExportProcessor", () => {
  it("preflights exportable content once until its cache is invalidated", async () => {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "ElementAspectExportProcessor",
      "Preflight.bim"
    );
    const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
      rootSubject: { name: "ElementAspectExportProcessorPreflight" },
    });
    try {
      await importElementAspectTestSchema(sourceDb);
      const binaryValue = Uint8Array.from([1, 2, 3]);
      const { emptyOwnerId, populatedOwnerId } = withEditTxn(
        sourceDb,
        "insert preflight test data",
        (txn) => {
          const emptyId = Subject.insert(
            txn,
            IModel.rootSubjectId,
            "EmptyOwner"
          );
          const populatedId = Subject.insert(
            txn,
            IModel.rootSubjectId,
            "PopulatedOwner"
          );
          txn.insertAspect({
            classFullName: "ExporterAspectTest:UniqueAspect",
            element: new ElementOwnsUniqueAspect(populatedId),
            binaryValue,
          } as ElementAspectProps);
          return { emptyOwnerId: emptyId, populatedOwnerId: populatedId };
        }
      );
      const exportedAspects: ElementUniqueAspect[] = [];
      const processor = new ElementAspectExportProcessor(
        sourceDb,
        createHandler({
          onExportElementUniqueAspect: async (aspect) => {
            exportedAspects.push(aspect);
          },
        })
      );
      const createQueryReader = vi.spyOn(sourceDb, "createQueryReader");

      await processor.exportAllElementAspects(new Set([emptyOwnerId]));
      expect(createQueryReader).toHaveBeenCalledTimes(2);

      await processor.exportAllElementAspects(new Set([populatedOwnerId]));
      expect(createQueryReader).toHaveBeenCalledTimes(3);
      expect(exportedAspects).to.have.lengthOf(1);
      expect(exportedAspects[0].element.id).to.equal(populatedOwnerId);
      expect(exportedAspects[0].asAny.binaryValue).to.deep.equal(binaryValue);

      createQueryReader.mockClear();
      processor.excludeElementAspectClass(ElementAspect.classFullName);
      await processor.exportAllElementAspects(new Set([emptyOwnerId]));
      await processor.exportAllElementAspects(new Set([populatedOwnerId]));
      expect(createQueryReader).toHaveBeenCalledTimes(1);
      expect(exportedAspects).to.have.lengthOf(1);

      createQueryReader.mockClear();
      processor.resetCaches();
      await processor.exportAllElementAspects(new Set([populatedOwnerId]));
      expect(createQueryReader).toHaveBeenCalledTimes(1);
      expect(exportedAspects).to.have.lengthOf(1);
    } finally {
      vi.restoreAllMocks();
      sourceDb.close();
    }
  });

  it("emits one complete multi-aspect group per owner", async () => {
    const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile(
      "ElementAspectExportProcessor",
      "MultiAspectOwnerGroups.bim"
    );
    const sourceDb = SnapshotDb.createEmpty(sourceDbPath, {
      rootSubject: { name: "ElementAspectExportProcessorOwnerGroups" },
    });
    try {
      await importElementAspectTestSchema(sourceDb);
      const ownerIds = withEditTxn(
        sourceDb,
        "insert multi-aspect owner groups",
        (txn) => {
          const ids = [
            Subject.insert(txn, IModel.rootSubjectId, "OwnerA"),
            Subject.insert(txn, IModel.rootSubjectId, "OwnerB"),
          ];
          for (const id of ids) {
            for (const classFullName of [
              "ExporterAspectTest:MultiAspectA",
              "ExporterAspectTest:MultiAspectB",
            ]) {
              for (let index = 0; index < 2; index++) {
                txn.insertAspect({
                  classFullName,
                  element: new ElementOwnsMultiAspects(id),
                } as ElementAspectProps);
              }
            }
          }
          return ids;
        }
      );
      const groups: ElementMultiAspect[][] = [];
      const processor = new ElementAspectExportProcessor(
        sourceDb,
        createHandler({
          onExportElementMultiAspects: async (aspects) => {
            groups.push(aspects);
          },
        })
      );

      await processor.exportAllElementAspects(new Set<Id64String>(ownerIds));

      expect(groups).to.have.lengthOf(ownerIds.length);
      expect(groups.flat()).to.have.lengthOf(8);
      expect(
        groups.every(
          (group) =>
            group.length === 4 &&
            new Set(group.map((aspect) => aspect.element.id)).size === 1 &&
            new Set(group.map((aspect) => aspect.classFullName)).size === 2
        )
      ).to.be.true;
      expect(new Set(groups.map((group) => group[0].element.id))).to.deep.equal(
        new Set(ownerIds)
      );
    } finally {
      sourceDb.close();
    }
  });
});
