/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, expect } from "chai";
import * as sinon from "sinon";
import { EditTxn, IModelJsFs, SnapshotDb } from "@itwin/core-backend";
import * as ECSchemaMetaData from "@itwin/ecschema-metadata";
import { SchemaLoader } from "@itwin/ecschema-metadata";
import { IModelExporter } from "../../IModelExporter";
import { IModelTransformer } from "../../IModelTransformer";
import {
  DynamicSchemaUnionStrategy,
  NewerVersionSchemaImportStrategy,
  SchemaProcessingStrategy,
} from "../../SchemaProcessingStrategy";
import {
  isSchemaProcessingError,
  SchemaProcessingError,
  SchemaProcessingErrorKey,
} from "../../SchemaProcessingErrors";
import { IModelTransformerTestUtils } from "../IModelTransformerUtils";
import * as TestUtils from "../TestUtils";

import "./TransformerTestStartup";

describe("Schema processing", () => {
  interface Fixture {
    sourceDb: SnapshotDb;
    targetDb: SnapshotDb;
    transformer: IModelTransformer;
  }

  const fixtures: Fixture[] = [];

  const createDb = async (name: string, schemas: string[] = []) => {
    const db = SnapshotDb.createEmpty(
      IModelTransformerTestUtils.prepareOutputFile(
        "IModelTransformer",
        `${name}.bim`
      ),
      { rootSubject: { name } }
    );
    if (schemas.length > 0) await db.importSchemaStrings(schemas);
    return db;
  };

  const createFixture = async (
    name: string,
    sourceSchemas: string[] = [],
    targetSchemas: string[] = [],
    sourceExporter?: (db: SnapshotDb) => IModelExporter
  ) => {
    const sourceDb = await createDb(`${name}Source`, sourceSchemas);
    const targetDb = await createDb(`${name}Target`, targetSchemas);
    const editTxn = new EditTxn(targetDb, "IModelTransformer");
    editTxn.start();
    const transformer = new IModelTransformer({
      source: sourceExporter?.(sourceDb) ?? sourceDb,
      target: editTxn,
    });
    const fixture = { sourceDb, targetDb, transformer };
    fixtures.push(fixture);
    return fixture;
  };

  const captureError = async (operation: Promise<unknown>) => {
    try {
      await operation;
    } catch (error: unknown) {
      return error;
    }
    throw new Error("Expected operation to fail");
  };

  const expectSchemaProcessingError = async (
    operation: Promise<unknown>,
    key: SchemaProcessingErrorKey
  ): Promise<SchemaProcessingError> => {
    const error = await captureError(operation);
    if (!isSchemaProcessingError(error, key))
      throw new Error(`Expected schema processing error '${key}'`);
    return error;
  };

  const dynamicSchema = (
    schemaName: string,
    version: string,
    itemName = "Same",
    options: {
      bisCoreVersion?: string;
      additionalReference?: string;
      propertyType?: string;
    } = {}
  ) => `<?xml version="1.0" encoding="UTF-8"?>
    <ECSchema schemaName="${schemaName}" alias="${schemaName.toLowerCase()}" version="${version}" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
      <ECSchemaReference name="BisCore" version="${options.bisCoreVersion ?? "01.00.00"}" alias="bis"/>
      ${options.additionalReference ?? ""}
      <ECCustomAttributes>
        <DynamicSchema xmlns="CoreCustomAttributes.01.00.03"/>
      </ECCustomAttributes>
      <ECEntityClass typeName="${itemName}">
        <BaseClass>bis:PhysicalElement</BaseClass>
        ${
          options.propertyType
            ? `<ECProperty propertyName="Value" typeName="${options.propertyType}"/>`
            : ""
        }
      </ECEntityClass>
    </ECSchema>`;

  afterEach(() => {
    sinon.restore();
    const databases = new Set<SnapshotDb>();
    while (fixtures.length > 0) {
      const fixture = fixtures.pop()!;
      fixture.transformer.dispose();
      databases.add(fixture.sourceDb);
      databases.add(fixture.targetDb);
    }
    for (const database of databases) database.close();
  });

  it("handles out-of-order exported schemas", async () => {
    const schema1Path = IModelTransformerTestUtils.prepareOutputFile(
      "IModelTransformer",
      "TestSchema1.ecschema.xml"
    );
    const schema2Path = IModelTransformerTestUtils.prepareOutputFile(
      "IModelTransformer",
      "TestSchema2.ecschema.xml"
    );
    IModelJsFs.writeFileSync(
      schema1Path,
      `<?xml version="1.0" encoding="UTF-8"?>
      <ECSchema schemaName="TestSchema1" alias="ts1" version="01.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
        <ECSchemaReference name="BisCore" version="01.00" alias="bis"/>
        <ECEntityClass typeName="TestElement1">
          <BaseClass>bis:PhysicalElement</BaseClass>
          <ECProperty propertyName="MyProp1" typeName="string"/>
        </ECEntityClass>
      </ECSchema>`
    );
    IModelJsFs.writeFileSync(
      schema2Path,
      `<?xml version="1.0" encoding="UTF-8"?>
      <ECSchema schemaName="TestSchema2" alias="ts2" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.2">
        <ECSchemaReference name="BisCore" version="01.00.00" alias="bis"/>
        <ECSchemaReference name="TestSchema1" version="01.00.00" alias="ts1"/>
        <ECEntityClass typeName="TestElement2">
          <BaseClass>ts1:TestElement1</BaseClass>
          <ECProperty propertyName="MyProp2" typeName="string"/>
        </ECEntityClass>
      </ECSchema>`
    );

    class OrderedExporter extends IModelExporter {
      public override async exportSchemas(
        options?: Parameters<IModelExporter["exportSchemas"]>[0]
      ) {
        const sourceLoader = new SchemaLoader((name) =>
          this.sourceDb.getSchemaProps(name)
        );
        const onExport =
          options?.onExportSchema ??
          (async (schema: ECSchemaMetaData.Schema) =>
            this.handler.onExportSchema(schema));
        await onExport(sourceLoader.getSchema("TestSchema2"));
        await onExport(sourceLoader.getSchema("TestSchema1"));
      }
    }

    const fixture = await createFixture(
      "SchemaOrder",
      [],
      [],
      (db) => new OrderedExporter(db)
    );
    await fixture.sourceDb.importSchemas([schema1Path, schema2Path]);
    await fixture.transformer.processSchemas();
    const targetLoader = new SchemaLoader((name) =>
      fixture.targetDb.getSchemaProps(name)
    );
    assert.isDefined(targetLoader.getSchema("TestSchema1"));
    assert.isDefined(targetLoader.getSchema("TestSchema2"));
  });

  it("waits for schema import before deleting the export directory", async () => {
    const fixture = await createFixture("SchemaCleanupTiming");
    await fixture.sourceDb.importSchemas([
      TestUtils.IModelTestUtils.resolveAssetFile(
        "CloneTest.01.00.00.ecschema.xml"
      ),
    ]);
    const importResolved = sinon.spy();
    sinon.replace(
      fixture.targetDb,
      "importSchemas",
      sinon.fake(async () => {
        await new Promise<void>((resolve) =>
          setImmediate(() => {
            importResolved();
            resolve();
          })
        );
      })
    );
    const removeSpy = sinon.spy(IModelJsFs, "removeSync");

    await fixture.transformer.processSchemas();

    assert(removeSpy.calledAfter(importResolved));
  });

  it("reports dependency cycles with a typed stable error", async () => {
    const context = new ECSchemaMetaData.SchemaContext();
    const schemaA = new ECSchemaMetaData.Schema(
      context,
      "CycleA",
      "ca",
      1,
      0,
      0
    );
    const schemaB = new ECSchemaMetaData.Schema(
      context,
      "CycleB",
      "cb",
      1,
      0,
      0
    );
    schemaA.references.push(schemaB);
    schemaB.references.push(schemaA);

    class CycleExporter extends IModelExporter {
      public override async exportSchemas(
        options?: Parameters<IModelExporter["exportSchemas"]>[0]
      ) {
        await options?.onExportSchema?.(schemaA);
        await options?.onExportSchema?.(schemaB);
      }
    }

    const fixture = await createFixture(
      "SchemaCycle",
      [],
      [],
      (db) => new CycleExporter(db)
    );
    const error = await expectSchemaProcessingError(
      fixture.transformer.processSchemas(),
      SchemaProcessingErrorKey.SchemaDependencyCycle
    );
    expect(error.schemaNames).to.deep.equal(["cyclea", "cycleb"]);
  });

  it("normalizes generic aggregate failures and preserves cardinality", async () => {
    const fixture = await createFixture("SchemaAggregate");
    const firstCause = new Error("first schema failure");
    const secondCause = new Error("second schema failure");
    const strategy: SchemaProcessingStrategy = {
      async processSchemas() {
        throw new AggregateError([firstCause, secondCause], "raw failures");
      },
    };
    const error = await captureError(
      fixture.transformer.processSchemas({ strategy })
    );
    assert.instanceOf(error, AggregateError);
    expect(error.errors).to.have.lengthOf(2);
    expect(
      error.errors.every((entry) =>
        isSchemaProcessingError(
          entry,
          SchemaProcessingErrorKey.SchemaProcessing
        )
      )
    ).to.be.true;
    expect(error.errors.map((entry) => entry.cause)).to.deep.equal([
      firstCause,
      secondCause,
    ]);

    const oneCause = new Error("one schema failure");
    const oneEntryFixture = await createFixture("SchemaAggregateOneEntry");
    const oneEntryError = await expectSchemaProcessingError(
      oneEntryFixture.transformer.processSchemas({
        strategy: {
          async processSchemas() {
            throw new AggregateError([oneCause], "one raw failure");
          },
        },
      }),
      SchemaProcessingErrorKey.SchemaProcessing
    );
    expect(oneEntryError.cause).to.equal(oneCause);
  });

  it("wraps cleanup failures without replacing the cause", async () => {
    const fixture = await createFixture("SchemaCleanupFailure");
    const cleanupCause = new Error("cleanup failed");
    sinon.replace(IModelJsFs, "removeSync", () => {
      throw cleanupCause;
    });
    const error = await expectSchemaProcessingError(
      fixture.transformer.processSchemas({
        strategy: {
          async processSchemas() {
            return [];
          },
        },
      }),
      SchemaProcessingErrorKey.SchemaProcessing
    );
    expect(error.cause).to.equal(cleanupCause);
  });

  it("uses equivalent default hooks for implicit and explicit selection", async () => {
    const schema = (version: string) => `<?xml version="1.0"?>
      <ECSchema schemaName="SchemaHook" alias="sh" version="${version}" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
        <ECSchemaReference name="BisCore" version="01.00.00" alias="bis"/>
      </ECSchema>`;
    const sourceDb = await createDb("SchemaHookSource", [schema("01.00.00")]);

    class HookTransformer extends IModelTransformer {
      public shouldExportSchemaCount = 0;
      public onExportSchemaCount = 0;
      public override async shouldExportSchema(
        key: ECSchemaMetaData.SchemaKey
      ) {
        ++this.shouldExportSchemaCount;
        return super.shouldExportSchema(key);
      }
      public override async onExportSchema(value: ECSchemaMetaData.Schema) {
        ++this.onExportSchemaCount;
        return super.onExportSchema(value);
      }
    }
    const createHookFixture = async (
      name: string,
      targetSchemas: string[] = []
    ) => {
      const targetDb = await createDb(name, targetSchemas);
      const txn = new EditTxn(targetDb, "IModelTransformer");
      txn.start();
      const transformer = new HookTransformer({
        source: sourceDb,
        target: txn,
      });
      fixtures.push({ sourceDb, targetDb, transformer });
      return { targetDb, transformer };
    };

    const implicit = await createHookFixture("SchemaHookImplicitTarget");
    await implicit.transformer.processSchemas();
    expect(implicit.transformer.shouldExportSchemaCount).to.be.greaterThan(0);
    expect(implicit.transformer.onExportSchemaCount).to.be.greaterThan(0);
    expect(implicit.targetDb.querySchemaVersion("SchemaHook")).to.equal(
      "1.0.0"
    );

    const explicit = await createHookFixture("SchemaHookExplicitTarget");
    await explicit.transformer.processSchemas({
      strategy: new NewerVersionSchemaImportStrategy(),
    });
    expect(explicit.transformer.shouldExportSchemaCount).to.be.greaterThan(0);
    expect(explicit.transformer.onExportSchemaCount).to.be.greaterThan(0);
    expect(explicit.targetDb.querySchemaVersion("SchemaHook")).to.equal(
      "1.0.0"
    );

    class ObservingStrategy extends NewerVersionSchemaImportStrategy {
      public sourceSchemaNames: string[] = [];
      public override async processSchemas(
        context: Parameters<
          NewerVersionSchemaImportStrategy["processSchemas"]
        >[0]
      ) {
        this.sourceSchemaNames = context.sourceSchemas.map(({ name }) => name);
        return super.processSchemas(context);
      }
    }
    const newer = await createHookFixture("SchemaHookNewerTarget", [
      schema("01.00.01"),
    ]);
    const strategy = new ObservingStrategy();
    await newer.transformer.processSchemas({ strategy });
    expect(strategy.sourceSchemaNames).to.include("SchemaHook");
    expect(newer.targetDb.querySchemaVersion("SchemaHook")).to.equal("1.0.1");
  });

  it("unions dynamic schemas with dependencies and skips no-op differences", async () => {
    const sourceSchemas = [
      dynamicSchema("Dynamic", "01.00.07", "SourceOnly"),
      dynamicSchema("Dynamic2", "01.00.06", "SourceOnly2", {
        bisCoreVersion: "01.00.01",
      }),
      dynamicSchema("DynamicNoOp", "01.00.06"),
      dynamicSchema("DynamicDependent", "01.00.06", "SourceDependent", {
        additionalReference:
          '<ECSchemaReference name="DynamicNoOp" version="01.00.06" alias="dno"/>',
      }),
    ];
    const targetSchemas = [
      dynamicSchema("Dynamic", "01.00.05", "TargetOnly"),
      dynamicSchema("Dynamic2", "01.00.04", "TargetOnly2"),
      dynamicSchema("DynamicNoOp", "01.00.05"),
      dynamicSchema("DynamicDependent", "01.00.05", "TargetDependent", {
        additionalReference:
          '<ECSchemaReference name="DynamicNoOp" version="01.00.05" alias="dno"/>',
      }),
    ];
    const { targetDb, transformer } = await createFixture(
      "DynamicUnion",
      sourceSchemas,
      targetSchemas
    );
    await transformer.processSchemas({
      strategy: new DynamicSchemaUnionStrategy(),
    });

    expect(targetDb.querySchemaVersion("Dynamic")).to.equal("1.0.8");
    expect(targetDb.containsClass("Dynamic:SourceOnly")).to.be.true;
    expect(targetDb.containsClass("Dynamic:TargetOnly")).to.be.true;
    expect(targetDb.querySchemaVersion("Dynamic2")).to.equal("1.0.7");
    expect(targetDb.containsClass("Dynamic2:SourceOnly2")).to.be.true;
    expect(targetDb.containsClass("Dynamic2:TargetOnly2")).to.be.true;
    expect(targetDb.querySchemaVersion("DynamicNoOp")).to.equal("1.0.6");
    expect(targetDb.querySchemaVersion("DynamicDependent")).to.equal("1.0.7");
    expect(targetDb.containsClass("DynamicDependent:SourceDependent")).to.be
      .true;
    expect(targetDb.containsClass("DynamicDependent:TargetDependent")).to.be
      .true;
  });

  it("handles version-only, root-version, and overflow dynamic changes", async () => {
    const versionOnly = await createFixture(
      "DynamicVersionOnly",
      [dynamicSchema("Dynamic", "01.00.07")],
      [dynamicSchema("Dynamic", "01.00.05")]
    );
    await versionOnly.transformer.processSchemas({
      strategy: new DynamicSchemaUnionStrategy(),
    });
    expect(versionOnly.targetDb.querySchemaVersion("Dynamic")).to.equal(
      "1.0.5"
    );

    const root = await createFixture(
      "DynamicRootVersion",
      [dynamicSchema("Dynamic", "02.00.00")],
      [dynamicSchema("Dynamic", "01.00.00")]
    );
    await expectSchemaProcessingError(
      root.transformer.processSchemas({
        strategy: new DynamicSchemaUnionStrategy(),
      }),
      SchemaProcessingErrorKey.SchemaConflict
    );
    expect(root.targetDb.querySchemaVersion("Dynamic")).to.equal("1.0.0");

    const overflow = await createFixture(
      "DynamicOverflow",
      [dynamicSchema("Dynamic", "01.00.9999999", "OverflowSource")],
      [dynamicSchema("Dynamic", "01.00.9999998", "OverflowTarget")]
    );
    await expectSchemaProcessingError(
      overflow.transformer.processSchemas({
        strategy: new DynamicSchemaUnionStrategy(),
      }),
      SchemaProcessingErrorKey.SchemaProcessing
    );
    expect(overflow.targetDb.querySchemaVersion("Dynamic")).to.equal(
      "1.0.9999998"
    );
  });

  it("aggregates conflicts without importing a partial union", async () => {
    const source = ["DynamicConflict", "DynamicConflict2"].map((name) =>
      dynamicSchema(name, "01.00.01", "Item", { propertyType: "int" })
    );
    const target = ["DynamicConflict", "DynamicConflict2"].map((name) =>
      dynamicSchema(name, "01.00.00", "Item", { propertyType: "string" })
    );
    const fixture = await createFixture("DynamicConflict", source, target);
    const error = await captureError(
      fixture.transformer.processSchemas({
        strategy: new DynamicSchemaUnionStrategy(),
      })
    );
    assert.instanceOf(error, AggregateError);
    expect(error.errors).to.have.lengthOf(2);
    expect(
      error.errors.every((entry) =>
        isSchemaProcessingError(entry, SchemaProcessingErrorKey.SchemaConflict)
      )
    ).to.be.true;
    expect(fixture.targetDb.querySchemaVersion("DynamicConflict")).to.equal(
      "1.0.0"
    );
    expect(fixture.targetDb.querySchemaVersion("DynamicConflict2")).to.equal(
      "1.0.0"
    );
    expect(
      fixture.targetDb.queryEntityIds({ from: "DynamicConflict.Item" }).size
    ).to.equal(0);
  });

  it("allows the differencing hook to resolve conflicts", async () => {
    const schema = (
      version: string,
      propertyType: string,
      sourceOnly: boolean
    ) =>
      dynamicSchema("DynamicConflictResolved", version, "Item", {
        propertyType,
      }).replace(
        "</ECSchema>",
        `${
          sourceOnly
            ? `<ECEntityClass typeName="SourceOnly">
                 <BaseClass>bis:PhysicalElement</BaseClass>
               </ECEntityClass>`
            : ""
        }</ECSchema>`
      );
    const fixture = await createFixture(
      "DynamicConflictResolved",
      [schema("01.00.01", "int", true)],
      [schema("01.00.00", "string", false)]
    );
    class ResolvingStrategy extends DynamicSchemaUnionStrategy {
      protected override async onSchemaDifferences(
        source: ECSchemaMetaData.Schema,
        target: ECSchemaMetaData.Schema,
        differences: Parameters<
          DynamicSchemaUnionStrategy["onSchemaDifferences"]
        >[2]
      ) {
        expect(source.name).to.equal("DynamicConflictResolved");
        expect(target.name).to.equal("DynamicConflictResolved");
        return {
          ...differences,
          differences: differences.differences.filter(
            (difference) =>
              !differences.conflicts?.some(
                (conflict) => conflict.difference === difference
              )
          ),
          conflicts: undefined,
        };
      }
    }
    await fixture.transformer.processSchemas({
      strategy: new ResolvingStrategy(),
    });
    expect(
      fixture.targetDb.querySchemaVersion("DynamicConflictResolved")
    ).to.equal("1.0.2");
    expect(fixture.targetDb.containsClass("DynamicConflictResolved:SourceOnly"))
      .to.be.true;
  });

  it("normalizes differencing hook failures", async () => {
    const schema = dynamicSchema("DynamicHookFailure", "01.00.00", "Item");
    const fixture = await createFixture(
      "DynamicHookFailure",
      [schema],
      [schema]
    );
    const hookError = new Error("schema differencing hook failed");
    class FailingStrategy extends DynamicSchemaUnionStrategy {
      protected override async onSchemaDifferences(
        _source: ECSchemaMetaData.Schema,
        _target: ECSchemaMetaData.Schema,
        _differences: Parameters<
          DynamicSchemaUnionStrategy["onSchemaDifferences"]
        >[2]
      ): Promise<
        Parameters<DynamicSchemaUnionStrategy["onSchemaDifferences"]>[2]
      > {
        throw hookError;
      }
    }
    const error = await expectSchemaProcessingError(
      fixture.transformer.processSchemas({ strategy: new FailingStrategy() }),
      SchemaProcessingErrorKey.SchemaProcessing
    );
    expect(error.cause).to.equal(hookError);
    expect(fixture.targetDb.querySchemaVersion("DynamicHookFailure")).to.equal(
      "1.0.0"
    );
  });

  it("rejects incompatible dynamic reference versions", async () => {
    const reference = (version: string) => `<?xml version="1.0"?>
      <ECSchema schemaName="DynamicRef" alias="dr" version="${version}" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1"/>`;
    const schema = (version: string, referenceVersion: string) =>
      dynamicSchema("DynamicReference", version, "Item", {
        additionalReference: `<ECSchemaReference name="DynamicRef" version="${referenceVersion}" alias="dr"/>`,
      });
    const fixture = await createFixture(
      "DynamicReference",
      [reference("02.00.00"), schema("01.00.01", "02.00.00")],
      [reference("01.00.00"), schema("01.00.00", "01.00.00")]
    );
    const error = await expectSchemaProcessingError(
      fixture.transformer.processSchemas({
        strategy: new DynamicSchemaUnionStrategy(),
      }),
      SchemaProcessingErrorKey.SchemaConflict
    );
    expect(error.cause).to.be.instanceOf(Error);
    expect(fixture.targetDb.querySchemaVersion("DynamicReference")).to.equal(
      "1.0.0"
    );
  });
});
