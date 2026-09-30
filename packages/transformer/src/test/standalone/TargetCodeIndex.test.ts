/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  EditTxn,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  IModelDb,
  PhysicalModel,
  PhysicalObject,
  SnapshotDb,
  SpatialCategory,
  withEditTxn,
} from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import {
  Code,
  CodeProps,
  CodeScopeSpec,
  CodeSpec,
  ElementProps,
  IModel,
  PhysicalElementProps,
  Placement3d,
  QueryBinder,
  SubCategoryAppearance,
} from "@itwin/core-common";
import { Point3d } from "@itwin/core-geometry";
import { expect } from "vitest";
import {
  createStartedEditTxn,
  IModelTransformerTestUtils,
} from "../IModelTransformerUtils";
import { IModelTransformer } from "../../IModelTransformer";
import { TargetCodeIndex } from "../../TargetCodeIndex";

const codeSpecName = "TargetCodeIndexTest";
const nbsp = "\xa0";
let dbCount = 0;

interface TestDb {
  db: SnapshotDb;
  codeSpecId: Id64String;
  modelId: Id64String;
  categoryId: Id64String;
}

function createTestDb(fileName: string): TestDb {
  const db = SnapshotDb.createEmpty(
    IModelTransformerTestUtils.prepareOutputFile("TargetCodeIndex", fileName),
    { rootSubject: { name: "TargetCodeIndex" } }
  );
  // keep whitespace in code values exactly as the transformer does
  db.codeValueBehavior = "exact";
  const ids = withEditTxn(
    db,
    "insert code spec, model and category",
    (txn) => ({
      codeSpecId: db.codeSpecs.insert(
        txn,
        CodeSpec.create(db, codeSpecName, CodeScopeSpec.Type.Repository)
      ),
      modelId: PhysicalModel.insert(txn, IModel.rootSubjectId, "Physical"),
      categoryId: SpatialCategory.insert(
        txn,
        IModel.dictionaryId,
        "Category",
        new SubCategoryAppearance()
      ),
    })
  );
  return { db, ...ids };
}

function elementProps(
  { codeSpecId, modelId, categoryId }: TestDb,
  codeValue: string | undefined,
  classFullName = PhysicalObject.classFullName
): PhysicalElementProps {
  return {
    classFullName,
    model: modelId,
    category: categoryId,
    code:
      codeValue === undefined
        ? Code.createEmpty()
        : { spec: codeSpecId, scope: IModel.rootSubjectId, value: codeValue },
    placement: Placement3d.fromJSON({
      origin: Point3d.createZero(),
      angles: {},
    }),
  };
}

function insertElements(
  testDb: TestDb,
  codeValues: readonly (string | undefined)[]
): Id64String[] {
  return withEditTxn(testDb.db, "insert elements", (txn) =>
    codeValues.map((value) => txn.insertElement(elementProps(testDb, value)))
  );
}

function codeOf(testDb: TestDb, value: string): Required<CodeProps> {
  return { spec: testDb.codeSpecId, scope: IModel.rootSubjectId, value };
}

/** The lookup the index replaces. */
async function queryElementIdByCode(
  db: IModelDb,
  code: Required<CodeProps>
): Promise<Id64String | undefined> {
  const reader = db.createQueryReader(
    "SELECT ECInstanceId FROM BisCore:Element WHERE CodeSpec.Id=? AND CodeScope.Id=? AND CodeValue=?",
    new QueryBinder()
      .bindId(1, code.spec)
      .bindId(2, code.scope)
      .bindString(3, code.value),
    { usePrimaryConn: true }
  );
  return (await reader.step()) ? reader.current[0] : undefined;
}

const storedCodeValues = [
  "Alpha",
  "beta ",
  " gamma",
  `delta${nbsp}`,
  "ÉCOLE",
  "straße",
  "Zeta",
];
const probedCodeValues = [
  "Alpha",
  "alpha",
  "ALPHA",
  "Alpha ",
  "beta ",
  "BETA ",
  "beta",
  " gamma",
  "gamma",
  `delta${nbsp}`,
  "delta",
  "ÉCOLE",
  "École",
  "école",
  "STRASSE",
  "straße",
  "zeta",
  "",
  "missing",
];

const loadOnFirstLookup = { lookupsBeforeLoad: 0 };

describe("TargetCodeIndex", () => {
  let testDb: TestDb;
  let elementIds: Id64String[];

  beforeEach(() => {
    testDb = createTestDb(`Index${++dbCount}.bim`);
    elementIds = insertElements(testDb, [...storedCodeValues, undefined]);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    testDb.db.close();
  });

  it("matches the ECSQL lookup for exact whitespace, ASCII-only case folding, and empty values", async () => {
    const index = new TargetCodeIndex(testDb.db, loadOnFirstLookup);
    await index.withIndex(async () => {
      for (const value of probedCodeValues) {
        const code = codeOf(testDb, value);
        expect(await index.findElementId(code), JSON.stringify(value)).toBe(
          await queryElementIdByCode(testDb.db, code)
        );
      }
    });
    expect(
      await index.withIndex(async () =>
        index.findElementId(codeOf(testDb, "ALPHA"))
      )
    ).toBe(elementIds[0]);
  });

  it("loads each (CodeSpec, CodeScope) once per indexed operation", async () => {
    const index = new TargetCodeIndex(testDb.db, loadOnFirstLookup);
    const createQueryReader = vi.spyOn(testDb.db, "createQueryReader");
    await index.withIndex(async () => {
      for (const value of probedCodeValues)
        await index.findElementId(codeOf(testDb, value));
      // another scope is another bucket, which does not see the root Subject scope's codes
      expect(
        await index.findElementId({
          ...codeOf(testDb, "Alpha"),
          scope: IModel.dictionaryId,
        })
      ).toBeUndefined();
    });
    expect(createQueryReader).toHaveBeenCalledTimes(2);
  });

  it("queries each (CodeSpec, CodeScope) directly until it has been looked up often enough to load", async () => {
    const index = new TargetCodeIndex(testDb.db, { lookupsBeforeLoad: 2 });
    const createQueryReader = vi.spyOn(testDb.db, "createQueryReader");
    await index.withIndex(async () => {
      for (const value of probedCodeValues) {
        const code = codeOf(testDb, value);
        expect(await index.findElementId(code), JSON.stringify(value)).toBe(
          await queryElementIdByCode(testDb.db, code)
        );
      }
    });
    // two lookup queries, one bucket load, and one reference query per probe
    expect(createQueryReader).toHaveBeenCalledTimes(
      3 + probedCodeValues.length
    );
  });

  it("queries every lookup outside an indexed operation", async () => {
    const index = new TargetCodeIndex(testDb.db, loadOnFirstLookup);
    const createQueryReader = vi.spyOn(testDb.db, "createQueryReader");
    expect(await index.findElementId(codeOf(testDb, "alpha"))).toBe(
      elementIds[0]
    );
    expect(await index.findElementId(codeOf(testDb, "alpha"))).toBe(
      elementIds[0]
    );
    expect(createQueryReader).toHaveBeenCalledTimes(2);
  });

  it("finds recorded codes that were written after their bucket was loaded", async () => {
    const index = new TargetCodeIndex(testDb.db, loadOnFirstLookup);
    await index.withIndex(async () => {
      expect(await index.findElementId(codeOf(testDb, "Eta "))).toBeUndefined();
      const [etaId, emptyId] = insertElements(testDb, ["Eta ", undefined]);
      index.recordElementCode(etaId, codeOf(testDb, "Eta "));
      index.recordElementCode(emptyId, Code.createEmpty());
      expect(await index.findElementId(codeOf(testDb, "eta "))).toBe(etaId);
      expect(await index.findElementId(codeOf(testDb, "Eta"))).toBeUndefined();
      expect(await index.findElementId(codeOf(testDb, ""))).toBeUndefined();
    });
  });

  it("does not return indexed elements that were deleted or whose code changed", async () => {
    const index = new TargetCodeIndex(testDb.db, loadOnFirstLookup);
    await index.withIndex(async () => {
      expect(await index.findElementId(codeOf(testDb, "Alpha"))).toBe(
        elementIds[0]
      );
      expect(await index.findElementId(codeOf(testDb, "Zeta"))).toBe(
        elementIds[6]
      );
      const newZetaId = withEditTxn(testDb.db, "delete and rename", (txn) => {
        txn.deleteElement(elementIds[0]);
        txn.updateElement({
          ...testDb.db.elements.getElementProps(elementIds[1]),
          code: codeOf(testDb, "renamed"),
        });
        txn.deleteElement(elementIds[6]);
        return txn.insertElement(elementProps(testDb, "zeta"));
      });
      index.recordElementCode(elementIds[1], codeOf(testDb, "renamed"));
      expect(
        await index.findElementId(codeOf(testDb, "Alpha"))
      ).toBeUndefined();
      expect(
        await index.findElementId(codeOf(testDb, "beta "))
      ).toBeUndefined();
      expect(await index.findElementId(codeOf(testDb, "Renamed"))).toBe(
        elementIds[1]
      );
      // the fallback query finds an unrecorded element that took over a stale code
      expect(await index.findElementId(codeOf(testDb, "Zeta"))).toBe(newZetaId);
    });
  });

  it("starts each outermost indexed operation from the current target", async () => {
    const index = new TargetCodeIndex(testDb.db, loadOnFirstLookup);
    await index.withIndex(async () => {
      await index.withIndex(async () => {
        expect(
          await index.findElementId(codeOf(testDb, "theta"))
        ).toBeUndefined();
      });
    });
    const [thetaId] = insertElements(testDb, ["Theta"]);
    await index.withIndex(async () => {
      expect(await index.findElementId(codeOf(testDb, "theta"))).toBe(thetaId);
    });
  });

  it("queries every lookup in buckets that exceed the code limits", async () => {
    for (const index of [
      new TargetCodeIndex(testDb.db, {
        ...loadOnFirstLookup,
        maxBucketCodes: storedCodeValues.length - 1,
      }),
      new TargetCodeIndex(testDb.db, {
        ...loadOnFirstLookup,
        maxIndexedCodes: storedCodeValues.length - 1,
      }),
    ]) {
      const createQueryReader = vi.spyOn(testDb.db, "createQueryReader");
      await index.withIndex(async () => {
        for (const value of probedCodeValues) {
          const code = codeOf(testDb, value);
          expect(await index.findElementId(code), JSON.stringify(value)).toBe(
            await queryElementIdByCode(testDb.db, code)
          );
        }
      });
      // one bucket load, then one lookup query and one reference query per probe
      expect(createQueryReader).toHaveBeenCalledTimes(
        1 + 2 * probedCodeValues.length
      );
      createQueryReader.mockRestore();
    }
  });
});

describe.each([
  ["loaded on first lookup", loadOnFirstLookup],
  ["default load threshold", undefined],
])("IModelTransformer lookup by Code (%s)", (_name, indexOptions) => {
  let sourceDb: TestDb;
  let targetDb: TestDb;
  let editTxn: EditTxn | undefined;

  beforeEach(() => {
    const testName = `Transformer${++dbCount}`;
    sourceDb = createTestDb(`${testName}-source.bim`);
    targetDb = createTestDb(`${testName}-target.bim`);
    editTxn = undefined;
  });

  afterEach(() => {
    if (editTxn?.isActive) editTxn.end();
    sourceDb.db.close();
    targetDb.db.close();
  });

  function useCodeIndex<T extends IModelTransformer>(transformer: T): T {
    Reflect.set(
      transformer,
      "_targetCodeIndex",
      new TargetCodeIndex(targetDb.db, indexOptions)
    );
    return transformer;
  }

  function targetCodeValue(id: Id64String): string | undefined {
    return targetDb.db.elements.getElementProps(id).code.value;
  }

  it("merges into a target that already has matching codes", async () => {
    const [targetAlpha, targetBetaSpace, targetEcole] = insertElements(
      targetDb,
      ["Alpha", "beta ", "école"]
    );
    const targetMismatch = withEditTxn(targetDb.db, "insert", (txn) =>
      txn.insertElement(
        elementProps(targetDb, "Mismatch", "Generic:SpatialLocation")
      )
    );
    const [
      sourceAlpha,
      sourceBetaSpace,
      sourceBeta,
      sourceEcole,
      sourceMismatch,
      sourceNew,
    ] = insertElements(sourceDb, [
      "ALPHA",
      "beta ",
      "beta",
      "ÉCOLE",
      "Mismatch",
      "New",
    ]);

    const transformer = useCodeIndex(
      new IModelTransformer(
        {
          source: sourceDb.db,
          target: (editTxn = createStartedEditTxn(targetDb.db)),
        },
        { noProvenance: true }
      )
    );
    await transformer.process();
    const targetIdOf = (id: Id64String) =>
      transformer.context.findTargetElementId(id);

    expect(targetIdOf(sourceDb.modelId)).toBe(targetDb.modelId);
    expect(targetIdOf(sourceDb.categoryId)).toBe(targetDb.categoryId);
    expect(targetIdOf(sourceAlpha)).toBe(targetAlpha);
    expect(targetCodeValue(targetAlpha)).toBe("ALPHA");
    expect(targetIdOf(sourceBetaSpace)).toBe(targetBetaSpace);
    expect(targetCodeValue(targetBetaSpace)).toBe("beta ");
    for (const [source, value] of [
      [sourceBeta, "beta"],
      [sourceEcole, "ÉCOLE"],
      [sourceNew, "New"],
    ]) {
      expect(targetIdOf(source)).not.toBe(targetBetaSpace);
      expect(targetIdOf(source)).not.toBe(targetEcole);
      expect(targetCodeValue(targetIdOf(source))).toBe(value);
    }
    expect(targetCodeValue(targetEcole)).toBe("école");

    // a class mismatch inserts a new element and clears its code
    expect(targetIdOf(sourceMismatch)).not.toBe(targetMismatch);
    expect(targetCodeValue(targetIdOf(sourceMismatch))).toBe("");
    expect(targetCodeValue(targetMismatch)).toBe("Mismatch");
    // empty code values are stored as NULL, so an empty lookup never matches
    targetDb.db.withSqliteStatement(
      "SELECT COUNT(*) FROM bis_Element WHERE CodeValue=''",
      (stmt) => {
        stmt.step();
        expect(stmt.getValue(0).getInteger()).toBe(0);
      }
    );
    targetDb.db.withSqliteStatement(
      "SELECT CodeValue IS NULL FROM bis_Element WHERE Id=?",
      (stmt) => {
        stmt.bindId(1, targetIdOf(sourceMismatch));
        stmt.step();
        expect(stmt.getValue(0).getInteger()).toBe(1);
      }
    );
    transformer.dispose();
  });

  it("finds elements inserted earlier in the same transformation by Code", async () => {
    const [sourceFirst, sourceSecond] = insertElements(sourceDb, [
      "Dup",
      "second",
    ]);

    class RenamingTransformer extends IModelTransformer {
      public override async onTransformElement(sourceElement: Element) {
        const props: ElementProps = await super.onTransformElement(
          sourceElement
        );
        if (sourceElement.id === sourceSecond) props.code.value = "DUP";
        return props;
      }
    }
    const transformer = useCodeIndex(
      new RenamingTransformer(
        {
          source: sourceDb.db,
          target: (editTxn = createStartedEditTxn(targetDb.db)),
        },
        { noProvenance: true }
      )
    );
    await transformer.process();

    const targetId = transformer.context.findTargetElementId(sourceFirst);
    expect(transformer.context.findTargetElementId(sourceSecond)).toBe(
      targetId
    );
    expect(targetCodeValue(targetId)).toBe("DUP");
    transformer.dispose();
  });
});
