/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert } from "chai";
import * as path from "path";
import {
  DefinitionContainer,
  DefinitionGroup,
  DefinitionGroupGroupsDefinitions,
  DefinitionModel,
  DocumentListModel,
  Drawing,
  DrawingCategory,
  DrawingGraphic,
  DrawingModel,
  EditTxn,
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementOwnsChildElements,
  EntityClassType,
  IModelDb,
  IModelJsFs,
  LinkElement,
  PhysicalElement,
  PhysicalElementIsOfType,
  PhysicalModel,
  PhysicalObject,
  PhysicalType,
  RecipeDefinitionElement,
  RepositoryLink,
  SnapshotDb,
  SpatialCategory,
  TemplateRecipe2d,
  TemplateRecipe3d,
  TypeDefinitionElement,
  withEditTxn,
} from "@itwin/core-backend";
import { IModelTestUtils } from "../TestUtils/IModelTestUtils";
import { KnownTestLocations as BackendKnownTestLocations } from "../TestUtils/KnownTestLocations";
import {
  Id64,
  Id64Set,
  Id64String,
  Logger,
  LogLevel,
} from "@itwin/core-bentley";
import {
  Code,
  CodeScopeSpec,
  DefinitionElementProps,
  GeometricElement2dProps,
  GeometryStreamProps,
  IModel,
  PhysicalElementProps,
  Placement2d,
  Placement3d,
  QueryBinder,
  RepositoryLinkProps,
  SubCategoryAppearance,
} from "@itwin/core-common";
import {
  Angle,
  Point2d,
  Point3d,
  Range2d,
  Range3d,
  YawPitchRollAngles,
} from "@itwin/core-geometry";
import { IModelImporter } from "../../IModelImporter";
import {
  IModelTransformer,
  IModelTransformOptions,
  TemplateModelCloner,
} from "../../IModelTransformer";
import { TransformerLoggerCategory } from "../../TransformerLoggerCategory";
import { createStartedEditTxn } from "../IModelTransformerUtils";

import "./TransformerTestStartup"; // calls startup/shutdown IModelHost before/after all tests

const createClassViews = false; // can set to true to make it easier to debug the catalog structure

/** Structure of a Catalog
 * - As with *normal* iModels, the CodeValue of the root Subject is the name of this catalog.
 * - It is expected that a catalog will import the same domain schemas that a domain application would.
 * - A catalog must contain one or more DefinitionContainers (one per product line).
 * - A DefinitionContainer (inserted into the dictionary model) and its DefinitionModel sub-model should contain:
 *   - All TypeDefinitions (properties that vary by component type, not by component instance)
 *   - All template recipes (The elements in the recipe's sub-model is what will be cloned when placing an instance)
 *   - All prerequisite definitions (categories, GeometryParts, etc.) used by the template recipes
 *   - No elements directly in the DefinitionContainer or in a template recipe sub-model should refer to any elements outside of the DefinitionContainer
 *
 * @note Standard IModelExporter exclusion techniques (exclude* methods or shouldExport overrides) can be used to filter out template recipes and TypeDefinitions for a partial import.
 * However, all other DefinitionElements (categories, GeometryParts, etc.) directly in the DefinitionContainer should be imported if any template recipe is imported since elements
 * in the template recipe sub-models may reference those definitions.
 *
 * @note Each catalog creator should create their own CodeSpec for DefinitionContainers with a prefix/namespace that ensures uniqueness.
 *
 * @note Standard domain categories are supported by finding in the catalog by name using the DefinitionContainer scope and remapping to the standard domain category known by the application.
 *
 * @note Templates from different perspectives (physical vs. drawing, for example) can be related via a DefinitionGroup (or subclass thereof)
 * and by the DefinitionGroupGroupsDefinitions relationship (or subclass thereof).
 */

/** This function mocks a Catalog Connector that reads ACME Equipment product data and outputs a catalog of components.
 * Notes about this catalog:
 * - Utilizes the TestDomain schema
 * - Has templates for the physical perspective and for a drawing perspective.
 * - Groups the physical template with a corresponding drawing template.
 */
async function createAcmeCatalog(dbFile: string): Promise<void> {
  const db = SnapshotDb.createEmpty(dbFile, {
    rootSubject: { name: "ACME Equipment" },
    createClassViews,
  });
  const domainSchemaFilePath = path.join(
    BackendKnownTestLocations.assetsDir,
    "TestDomain.ecschema.xml"
  );
  await db.importSchemas([domainSchemaFilePath]);
  const manufacturerName = "ACME";
  const productLineName = `${manufacturerName} Product Line A`;
  withEditTxn(db, "create ACME catalog", (txn) => {
    const containerCodeSpecId = db.codeSpecs.insert(
      txn,
      "ACME:Equipment",
      CodeScopeSpec.Type.Repository
    ); // A catalog creator should insert their own CodeSpec for DefinitionContainers
    const templateGroupCodeSpecId = db.codeSpecs.insert(
      txn,
      "ACME:TemplateGroup",
      CodeScopeSpec.Type.Model
    );
    const containerCode = createContainerCode(
      containerCodeSpecId,
      productLineName
    );
    const containerId = DefinitionContainer.insert(
      txn,
      IModel.dictionaryId,
      containerCode
    ); // This sample has a DefinitionContainer per product line
    const spatialCategoryId = SpatialCategory.insert(
      txn,
      containerId,
      "Equipment",
      new SubCategoryAppearance()
    ); // "Equipment" is the name of a standard domain SpatialCategory in this sample
    const drawingCategoryId = DrawingCategory.insert(
      txn,
      containerId,
      "Symbols",
      new SubCategoryAppearance()
    ); // "Symbols" is the name of a standard domain DrawingCategory in this sample

    const codeValue1 = "A-1 Series";
    const physicalGeomProps1 = IModelTestUtils.createBox(new Point3d(1, 1, 1));
    const physicalRecipeId1 = insertEquipmentRecipe(
      txn,
      containerId,
      spatialCategoryId,
      codeValue1,
      physicalGeomProps1
    ); // a template recipe can be referenced by more than one PhysicalType
    insertEquipmentType(
      txn,
      db,
      containerId,
      "A-101",
      physicalRecipeId1,
      manufacturerName,
      productLineName
    );
    insertEquipmentType(
      txn,
      db,
      containerId,
      "A-102",
      physicalRecipeId1,
      manufacturerName,
      productLineName
    );
    const symbolGeomProps1 = IModelTestUtils.createRectangle(
      Point2d.create(1, 1)
    );
    const symbolRecipeId1 = insertSymbolRecipe(
      txn,
      containerId,
      drawingCategoryId,
      codeValue1,
      symbolGeomProps1
    );
    const groupProps1: DefinitionElementProps = {
      classFullName: DefinitionGroup.classFullName,
      model: containerId,
      code: new Code({
        spec: templateGroupCodeSpecId,
        scope: containerId,
        value: codeValue1,
      }),
    };
    const groupId1 = txn.insertElement(groupProps1);
    DefinitionGroupGroupsDefinitions.insert(txn, groupId1, physicalRecipeId1);
    DefinitionGroupGroupsDefinitions.insert(txn, groupId1, symbolRecipeId1);

    const codeValue2 = "A-2 Series";
    const physicalGeomProps2 = IModelTestUtils.createBox(new Point3d(2, 2, 2));
    const physicalRecipeId2 = insertEquipmentRecipe(
      txn,
      containerId,
      spatialCategoryId,
      codeValue2,
      physicalGeomProps2
    );
    insertEquipmentType(
      txn,
      db,
      containerId,
      "A-201",
      physicalRecipeId2,
      manufacturerName,
      productLineName
    );
    insertEquipmentType(
      txn,
      db,
      containerId,
      "A-202",
      physicalRecipeId2,
      manufacturerName,
      productLineName
    );
    insertEquipmentType(
      txn,
      db,
      containerId,
      "A-203",
      physicalRecipeId2,
      manufacturerName,
      productLineName
    );
    const symbolGeomProps2 = IModelTestUtils.createRectangle(
      Point2d.create(2, 2)
    );
    const symbolRecipeId2 = insertSymbolRecipe(
      txn,
      containerId,
      drawingCategoryId,
      codeValue2,
      symbolGeomProps2
    );
    const groupProps2: DefinitionElementProps = {
      classFullName: DefinitionGroup.classFullName,
      model: containerId,
      code: new Code({
        spec: templateGroupCodeSpecId,
        scope: containerId,
        value: codeValue2,
      }),
    };
    const groupId2 = txn.insertElement(groupProps2);
    DefinitionGroupGroupsDefinitions.insert(txn, groupId2, physicalRecipeId2);
    DefinitionGroupGroupsDefinitions.insert(txn, groupId2, symbolRecipeId2);

    const codeValue3 = "A-3 Series";
    const physicalGeomProps3 = IModelTestUtils.createBox(new Point3d(3, 3, 3));
    const physicalRecipeId3 = insertEquipmentRecipe(
      txn,
      containerId,
      spatialCategoryId,
      codeValue3,
      physicalGeomProps3
    );
    insertEquipmentType(
      txn,
      db,
      containerId,
      "A-301",
      physicalRecipeId3,
      manufacturerName,
      productLineName
    );
    const symbolGeomProps3 = IModelTestUtils.createRectangle(
      Point2d.create(3, 3)
    );
    const symbolRecipeId3 = insertSymbolRecipe(
      txn,
      containerId,
      drawingCategoryId,
      codeValue3,
      symbolGeomProps3
    );
    const groupProps3: DefinitionElementProps = {
      classFullName: DefinitionGroup.classFullName,
      model: containerId,
      code: new Code({
        spec: templateGroupCodeSpecId,
        scope: containerId,
        value: codeValue3,
      }),
    };
    const groupId3 = txn.insertElement(groupProps3);
    DefinitionGroupGroupsDefinitions.insert(txn, groupId3, physicalRecipeId3);
    DefinitionGroupGroupsDefinitions.insert(txn, groupId3, symbolRecipeId3);
  });
  db.close();
}

/** This function mocks a Catalog Connector that reads Best Equipment product data and outputs a catalog of components.
 * Notes about this catalog:
 * - Utilizes the TestDomain schema
 * - Demonstrates multiple containers (mapped to product line in this sample) for a single catalog.
 * - Happens to only have templates for the physical perspective (no symbols, no groups)
 */
async function createBestCatalog(dbFile: string): Promise<void> {
  const db = SnapshotDb.createEmpty(dbFile, {
    rootSubject: { name: "Best Equipment" },
  });
  const domainSchemaFilePath = path.join(
    BackendKnownTestLocations.assetsDir,
    "TestDomain.ecschema.xml"
  );
  await db.importSchemas([domainSchemaFilePath]);
  const manufacturerName = "Best";
  withEditTxn(db, "create Best catalog", (txn) => {
    const containerCodeSpecId = db.codeSpecs.insert(
      txn,
      `${manufacturerName}:Equipment`,
      CodeScopeSpec.Type.Repository
    );

    // Product Line B
    const productLineNameB = `${manufacturerName} Product Line B`;
    const containerCodeB = createContainerCode(
      containerCodeSpecId,
      productLineNameB
    );
    const containerIdB = DefinitionContainer.insert(
      txn,
      IModel.dictionaryId,
      containerCodeB
    );
    const categoryIdB = SpatialCategory.insert(
      txn,
      containerIdB,
      "Equipment",
      new SubCategoryAppearance()
    );

    const codeValueB2 = "B-2 Series";
    const physicalGeomPropsB2 = IModelTestUtils.createCylinder(2);
    const physicalRecipeIdB2 = insertEquipmentRecipe(
      txn,
      containerIdB,
      categoryIdB,
      codeValueB2,
      physicalGeomPropsB2
    );
    insertEquipmentType(
      txn,
      db,
      containerIdB,
      "B-201",
      physicalRecipeIdB2,
      manufacturerName,
      productLineNameB
    );
    insertEquipmentType(
      txn,
      db,
      containerIdB,
      "B-202",
      physicalRecipeIdB2,
      manufacturerName,
      productLineNameB
    );

    const codeValueB3 = "B-3 Series";
    const physicalGeomPropsB3 = IModelTestUtils.createCylinder(3);
    const physicalRecipeIdB3 = insertEquipmentRecipe(
      txn,
      containerIdB,
      categoryIdB,
      codeValueB3,
      physicalGeomPropsB3
    );
    insertEquipmentType(
      txn,
      db,
      containerIdB,
      "B-301",
      physicalRecipeIdB3,
      manufacturerName,
      productLineNameB
    );
    insertEquipmentType(
      txn,
      db,
      containerIdB,
      "B-302",
      physicalRecipeIdB3,
      manufacturerName,
      productLineNameB
    );
    insertEquipmentType(
      txn,
      db,
      containerIdB,
      "B-303",
      physicalRecipeIdB3,
      manufacturerName,
      productLineNameB
    );
    insertEquipmentType(
      txn,
      db,
      containerIdB,
      "B-304",
      physicalRecipeIdB3,
      manufacturerName,
      productLineNameB
    );

    // Product Line D
    const productLineNameD = `${manufacturerName} Product Line D`;
    const containerCodeD = createContainerCode(
      containerCodeSpecId,
      productLineNameD
    );
    const containerIdD = DefinitionContainer.insert(
      txn,
      IModel.dictionaryId,
      containerCodeD
    );
    const categoryIdD = SpatialCategory.insert(
      txn,
      containerIdD,
      "Equipment",
      new SubCategoryAppearance()
    );

    const codeValueD1 = "D-1 Series";
    const physicalGeomPropsD1 = IModelTestUtils.createCylinder(1);
    const physicalRecipeIdD1 = insertEquipmentRecipe(
      txn,
      containerIdD,
      categoryIdD,
      codeValueD1,
      physicalGeomPropsD1
    );
    insertEquipmentType(
      txn,
      db,
      containerIdD,
      "D-101",
      physicalRecipeIdD1,
      manufacturerName,
      productLineNameD
    );
    insertEquipmentType(
      txn,
      db,
      containerIdD,
      "D-102",
      physicalRecipeIdD1,
      manufacturerName,
      productLineNameD
    );

    const codeValueD2 = "D-2 Series";
    const physicalGeomPropsD2 = IModelTestUtils.createCylinder(2);
    const physicalRecipeIdD2 = insertEquipmentRecipe(
      txn,
      containerIdD,
      categoryIdD,
      codeValueD2,
      physicalGeomPropsD2
    );
    insertEquipmentType(
      txn,
      db,
      containerIdD,
      "D-201",
      physicalRecipeIdD2,
      manufacturerName,
      productLineNameD
    );
    insertEquipmentType(
      txn,
      db,
      containerIdD,
      "D-202",
      physicalRecipeIdD2,
      manufacturerName,
      productLineNameD
    );
  });
  db.close();
}

/** This function creates test catalog components.
 * Notes about this catalog:
 * - Utilizes only the builtin BisCore and Generic schemas
 * - Has an example template (Assembly) that contains multiple elements
 * - Has no associated PhysicalTypes
 */
async function createTestCatalog(dbFile: string): Promise<void> {
  const db: SnapshotDb = SnapshotDb.createEmpty(dbFile, {
    rootSubject: { name: "Test Catalog" },
    createClassViews,
  });
  withEditTxn(db, "create test catalog", (txn) => {
    const containerCodeSpecId = db.codeSpecs.insert(
      txn,
      "Test:Components",
      CodeScopeSpec.Type.Repository
    );
    const containerCode = createContainerCode(
      containerCodeSpecId,
      "Test Components"
    );
    const containerId = DefinitionContainer.insert(
      txn,
      IModel.dictionaryId,
      containerCode
    );
    const spatialCategoryId = SpatialCategory.insert(
      txn,
      containerId,
      "Test Components",
      new SubCategoryAppearance()
    );
    const drawingCategoryId = DrawingCategory.insert(
      txn,
      containerId,
      "Test Components",
      new SubCategoryAppearance()
    );

    // Cylinder component
    const cylinderTemplateId = TemplateRecipe3d.insert(
      txn,
      containerId,
      "Cylinder Template"
    );
    const cylinderTemplateModel = db.models.getModel<PhysicalModel>(
      cylinderTemplateId,
      PhysicalModel
    );
    assert.isTrue(cylinderTemplateModel.isTemplate);
    const cylinderProps: PhysicalElementProps = {
      classFullName: PhysicalObject.classFullName,
      model: cylinderTemplateId,
      category: spatialCategoryId,
      code: Code.createEmpty(),
      userLabel: "Cylinder",
      placement: {
        origin: Point3d.createZero(),
        angles: { yaw: 0, pitch: 0, roll: 0 },
      },
      geom: IModelTestUtils.createCylinder(1),
    };
    txn.insertElement(cylinderProps);

    // Assembly component
    const assemblyTemplateId = TemplateRecipe3d.insert(
      txn,
      containerId,
      "Assembly Template"
    );
    assert.exists(db.models.getModel<PhysicalModel>(assemblyTemplateId));
    const assemblyHeadProps: PhysicalElementProps = {
      classFullName: PhysicalObject.classFullName,
      model: assemblyTemplateId,
      category: spatialCategoryId,
      code: Code.createEmpty(),
      userLabel: "Assembly Head",
      placement: {
        origin: Point3d.createZero(),
        angles: { yaw: 0, pitch: 0, roll: 0 },
      },
      geom: IModelTestUtils.createCylinder(1),
    };
    const assemblyHeadId: Id64String = txn.insertElement(assemblyHeadProps);
    const childBoxProps: PhysicalElementProps = {
      classFullName: PhysicalObject.classFullName,
      model: assemblyTemplateId,
      category: spatialCategoryId,
      parent: new ElementOwnsChildElements(assemblyHeadId),
      code: Code.createEmpty(),
      userLabel: "Child",
      placement: {
        origin: Point3d.create(2, 0, 0),
        angles: { yaw: 0, pitch: 0, roll: 0 },
      },
      geom: IModelTestUtils.createBox(Point3d.create(1, 1, 1)),
    };
    txn.insertElement(childBoxProps);

    // 2d component
    const drawingGraphicTemplateId = TemplateRecipe2d.insert(
      txn,
      containerId,
      "DrawingGraphic Template"
    );
    const drawingGraphicTemplateModel = db.models.getModel<DrawingModel>(
      drawingGraphicTemplateId,
      DrawingModel
    );
    assert.isTrue(drawingGraphicTemplateModel.isTemplate);
    const drawingGraphicProps: GeometricElement2dProps = {
      classFullName: DrawingGraphic.classFullName,
      model: drawingGraphicTemplateId,
      category: drawingCategoryId,
      code: Code.createEmpty(),
      userLabel: "DrawingGraphic",
      placement: { origin: Point2d.createZero(), angle: 0 },
      geom: IModelTestUtils.createRectangle(Point2d.create(1, 1)),
    };
    txn.insertElement(drawingGraphicProps);
  });
  db.close();
}

/** Mock how Component Center would index a catalog by writing out the hierarchy of the catalog as a markdown file.
 * @note A real implementation for Component Center would probably write the relevant data out to JSON instead.
 */
async function indexCatalog(db: IModelDb, outputFile: string): Promise<void> {
  IModelJsFs.writeFileSync(outputFile, `# ${db.rootSubject.name}\n`);
  if (db.rootSubject.description) {
    IModelJsFs.appendFileSync(outputFile, `${db.rootSubject.description}\n`);
  }
  const containerIds = await queryContainerIds(db);
  for (const containerId of containerIds) {
    const container = db.elements.getElement<DefinitionContainer>(
      containerId,
      DefinitionContainer
    );
    IModelJsFs.appendFileSync(outputFile, `## ${container.code.value}\n`);
    const templateRecipeIds = await queryTemplateRecipeIds(db, containerId);
    if (templateRecipeIds.size > 0) {
      IModelJsFs.appendFileSync(outputFile, "### TemplateRecipes\n");
      for (const templateRecipeId of templateRecipeIds) {
        const templateRecipe = db.elements.getElement<RecipeDefinitionElement>(
          templateRecipeId,
          RecipeDefinitionElement
        );
        IModelJsFs.appendFileSync(
          outputFile,
          `#### ${templateRecipe.code.value}\n`
        );
        const typeDefinitionIds = await queryTypeDefinitionIds(
          db,
          templateRecipeId
        );
        for (const typeDefinitionId of typeDefinitionIds) {
          const typeDefinition = db.elements.getElement<TypeDefinitionElement>(
            typeDefinitionId,
            TypeDefinitionElement
          );
          IModelJsFs.appendFileSync(
            outputFile,
            `- ${typeDefinition.code.value}\n`
          );
          // NOTE: you have the TypeDefinitionElement instance here, you could also write out its property values
        }
      }
    }
    const groupIds = await queryDefinitionGroupIds(db, containerId);
    if (groupIds.size > 0) {
      IModelJsFs.appendFileSync(outputFile, "### DefinitionGroups\n");
      for (const groupId of groupIds) {
        const group = db.elements.getElement<DefinitionGroup>(
          groupId,
          DefinitionGroup
        );
        IModelJsFs.appendFileSync(outputFile, `#### ${group.code.value}\n`);
        const memberIds = await queryDefinitionGroupMemberIds(db, groupId);
        for (const memberId of memberIds) {
          const templateRecipe =
            db.elements.getElement<RecipeDefinitionElement>(
              memberId,
              RecipeDefinitionElement
            );
          IModelJsFs.appendFileSync(
            outputFile,
            `- ${templateRecipe.code.value}\n`
          );
        }
      }
    }
  }
}

/** Mocks the creation of a template recipe that would be the responsibility of a Catalog Connector.
 * @note This sample creates a single element in the template model, but 1-N elements are supported.
 */
function insertEquipmentRecipe(
  txn: EditTxn,
  modelId: Id64String,
  categoryId: Id64String,
  codeValue: string,
  geom: GeometryStreamProps
): Id64String {
  const templateId = TemplateRecipe3d.insert(txn, modelId, codeValue);
  const equipmentProps: PhysicalElementProps = {
    classFullName: "TestDomain:Equipment",
    model: templateId, // the sub-model of the TemplateRecipe3d
    category: categoryId,
    code: Code.createEmpty(),
    userLabel: codeValue,
    placement: {
      origin: Point3d.createZero(),
      angles: { yaw: 0, pitch: 0, roll: 0 },
    },
    geom,
  };
  txn.insertElement(equipmentProps);
  return templateId;
}

function insertSymbolRecipe(
  txn: EditTxn,
  modelId: Id64String,
  categoryId: Id64String,
  codeValue: string,
  geom: GeometryStreamProps
): Id64String {
  const templateId = TemplateRecipe2d.insert(txn, modelId, codeValue);
  const drawingGraphicProps: GeometricElement2dProps = {
    classFullName: DrawingGraphic.classFullName,
    model: templateId, // the sub-model of the TemplateRecipe2d
    category: categoryId,
    code: Code.createEmpty(),
    userLabel: codeValue,
    placement: { origin: Point2d.createZero(), angle: 0 },
    geom,
  };
  txn.insertElement(drawingGraphicProps);
  return templateId;
}

function createContainerCode(codeSpecId: Id64String, codeValue: string): Code {
  return new Code({
    spec: codeSpecId,
    scope: IModel.rootSubjectId, // the scope is always rootSubjectId for CodeScopeSpec.Type.Repository
    value: codeValue,
  });
}

/** Query for catalog-related DefinitionContainers.
 * @note The convention is to insert the catalog DefinitionContainer elements into the DictionaryModel, so this method only looks there.
 */
async function queryContainerIds(db: IModelDb): Promise<Id64Set> {
  const sql = `SELECT ECInstanceId FROM ${DefinitionContainer.classFullName} WHERE Model.Id=:modelId`;
  const containerIds = new Set<Id64String>();
  const params = new QueryBinder().bindId("modelId", IModel.dictionaryId);
  for await (const row of db.createQueryReader(sql, params, {
    usePrimaryConn: true,
  })) {
    containerIds.add(row.id);
  }
  return containerIds;
}

/** Query for DefinitionGroups within a DefinitionContainer.
 * @note This is one way of grouping related TemplateRecipes together
 */
async function queryDefinitionGroupIds(
  db: IModelDb,
  containerId: Id64String
): Promise<Id64Set> {
  const sql = `SELECT ECInstanceId FROM ${DefinitionGroup.classFullName} WHERE Model.Id=:modelId`;
  const groupIds = new Set<Id64String>();
  const params = new QueryBinder().bindId("modelId", containerId);
  for await (const row of db.createQueryReader(sql, params, {
    usePrimaryConn: true,
  })) {
    groupIds.add(row.id);
  }
  return groupIds;
}

/** Query for the members of a DefinitionGroup. */
async function queryDefinitionGroupMemberIds(
  db: IModelDb,
  groupId: Id64String
): Promise<Id64Set> {
  const sql = `SELECT TargetECInstanceId FROM ${DefinitionGroupGroupsDefinitions.classFullName} WHERE SourceECInstanceId=:groupId`;
  const memberIds = new Set<Id64String>();
  const params = new QueryBinder().bindId("groupId", groupId);
  for await (const row of db.createQueryReader(sql, params, {
    usePrimaryConn: true,
  })) {
    memberIds.add(row[0] as Id64String);
  }
  return memberIds;
}

/** This mocks the concept of a standard domain category. */
function queryEquipmentCategory(
  db: IModelDb,
  modelId: Id64String
): Id64String | undefined {
  const code = SpatialCategory.createCode(db, modelId, "Equipment");
  return db.elements.queryElementIdByCode(code);
}

/** This mocks a domain-specific subclass of PhysicalType that would be defined by an aligned domain schema. */
function insertEquipmentType(
  txn: EditTxn,
  db: IModelDb,
  modelId: Id64String,
  codeValue: string,
  recipeId: Id64String,
  manufacturerName: string,
  productLineName: string
): Id64String {
  const equipmentTypeProps = {
    classFullName: "TestDomain:EquipmentType",
    model: modelId,
    code: createEquipmentTypeCode(db, modelId, codeValue),
    recipe: {
      id: recipeId,
      relClassName: "BisCore:PhysicalTypeHasTemplateRecipe",
    },
    manufacturerName,
    productLineName,
  };
  return txn.insertElement(equipmentTypeProps);
}

function createEquipmentTypeCode(
  db: IModelDb,
  modelId: Id64String,
  codeValue: string
): Code {
  return PhysicalType.createCode(db, modelId, codeValue);
}

function queryEquipmentTypeId(
  db: IModelDb,
  modelId: Id64String,
  codeValue: string
): Id64String | undefined {
  const code = createEquipmentTypeCode(db, modelId, codeValue);
  return db.elements.queryElementIdByCode(code);
}

/** Query for all TypeDefinitions that reference a particular template recipe. */
async function queryTypeDefinitionIds(
  db: IModelDb,
  templateRecipeId: Id64String
): Promise<Id64Set> {
  const sql = `SELECT ECInstanceId FROM ${TypeDefinitionElement.classFullName} WHERE Recipe.Id=:templateRecipeId`;
  const typeDefinitionIds = new Set<Id64String>();
  const params = new QueryBinder().bindId("templateRecipeId", templateRecipeId);
  for await (const row of db.createQueryReader(sql, params, {
    usePrimaryConn: true,
  })) {
    typeDefinitionIds.add(row.id);
  }
  return typeDefinitionIds;
}

/** Query for all template recipes in a particular model/container. */
async function queryTemplateRecipeIds(
  db: IModelDb,
  containerId: Id64String
): Promise<Id64Set> {
  const sql = `SELECT ECInstanceId FROM ${RecipeDefinitionElement.classFullName} WHERE Model.Id=:modelId`;
  const templateRecipeIds = new Set<Id64String>();
  const params = new QueryBinder().bindId("modelId", containerId);
  for await (const row of db.createQueryReader(sql, params, {
    usePrimaryConn: true,
  })) {
    templateRecipeIds.add(row.id);
  }
  return templateRecipeIds;
}

/** This mocks the concept of finding important/lead elements in the template recipe sub-model.
 * @note This is important for establishing relationships after placing cloned instances.
 */
async function queryEquipmentId(
  db: IModelDb,
  templateModelId: Id64String
): Promise<Id64String | undefined> {
  const sql =
    "SELECT ECInstanceId FROM TestDomain:Equipment WHERE Model.Id=:modelId LIMIT 1";
  const params = new QueryBinder().bindId("modelId", templateModelId);
  const reader = db.createQueryReader(sql, params, { usePrimaryConn: true });
  return (await reader.step()) ? reader.current.id : undefined;
}

async function countElementsInModel(
  db: IModelDb,
  classFullName: string,
  modelId: Id64String
): Promise<number> {
  const sql = `SELECT COUNT(*) FROM ${classFullName} WHERE Model.Id=:modelId`;
  const params = new QueryBinder().bindId("modelId", modelId);
  const reader = db.createQueryReader(sql, params, { usePrimaryConn: true });
  return (await reader.step()) ? reader.current[0] : 0;
}

/** Create a RepositoryLink for the catalog that will scope the provenance for elements imported from the catalog. */
function insertCatalogRepositoryLink(
  iModelDb: IModelDb,
  codeValue: string,
  url: string,
  editTxn: EditTxn
): Id64String {
  const code = LinkElement.createCode(
    iModelDb,
    IModel.repositoryModelId,
    codeValue
  );
  const repositoryLinkId = iModelDb.elements.queryElementIdByCode(code);
  if (undefined === repositoryLinkId) {
    const repositoryLinkProps: RepositoryLinkProps = {
      classFullName: RepositoryLink.classFullName,
      model: IModel.repositoryModelId,
      code,
      url,
      format: "Catalog", // WIP: need to standardize format names
    };
    return editTxn.insertElement(repositoryLinkProps);
  }
  return repositoryLinkId;
}

/** Specialization of IModelTransformer designed to import definitions from a catalog. */
class CatalogImporter extends IModelTransformer {
  private _targetSpatialCategories: Map<string, Id64String> | undefined;
  private _targetDrawingCategories: Map<string, Id64String> | undefined;

  /** Construct a new CatalogImporter.
   * @param sourceDb The catalog
   * @param targetDb The iModel to import into
   * @param targetScopeElementId The optional Id of the Element in the **target** iModel that represents the **source** repository as a whole and scopes its [ExternalSourceAspect]($backend) instances used to store provenance.
   * If `undefined` then provenance back to the catalog cannot be stored.
   * @param targetSpatialCategories Optional remapping for standard spatial categories.
   * @param targetDrawingCategories Optional remapping for standard drawing categories.
   */
  private constructor(
    sourceDb: IModelDb,
    targetDb: IModelDb,
    editTxn: EditTxn,
    targetScopeElementId?: Id64String,
    targetSpatialCategories?: Map<string, Id64String>,
    targetDrawingCategories?: Map<string, Id64String>
  ) {
    const options: IModelTransformOptions = {
      targetScopeElementId,
      noProvenance: targetScopeElementId ? undefined : true, // can't store provenance if targetScopeElementId is not defined
    };
    const target = new IModelImporter(targetDb, editTxn, {
      autoExtendProjectExtents: false,
    });
    super(sourceDb, target, editTxn, options);
    this._targetSpatialCategories = targetSpatialCategories;
    this._targetDrawingCategories = targetDrawingCategories;
  }

  public static async create(
    sourceDb: IModelDb,
    targetDb: IModelDb,
    editTxn: EditTxn,
    targetScopeElementId?: Id64String,
    targetSpatialCategories?: Map<string, Id64String>,
    targetDrawingCategories?: Map<string, Id64String>
  ): Promise<CatalogImporter> {
    const inst = new this(
      sourceDb,
      targetDb,
      editTxn,
      targetScopeElementId,
      targetSpatialCategories,
      targetDrawingCategories
    );
    await inst.initialize();
    return inst;
  }

  public async importDefinitionContainers(): Promise<void> {
    const containerIds = await queryContainerIds(this.sourceDb);
    for (const containerId of containerIds) {
      await this.importDefinitionContainer(containerId);
    }
  }

  public async importDefinitionContainer(
    sourceContainerId: Id64String
  ): Promise<void> {
    const sourceContainer =
      this.sourceDb.elements.getElement<DefinitionContainer>(
        sourceContainerId,
        DefinitionContainer
      ); // throw Error if not a DefinitionContainer
    const sourceContainerCodeSpec = this.sourceDb.codeSpecs.getById(
      sourceContainer.code.spec
    );
    let targetContainerId: Id64String | undefined;
    try {
      const targetContainerCodeSpec = this.targetDb.codeSpecs.getByName(
        sourceContainerCodeSpec.name
      );
      const targetContainerCode = new Code({
        spec: targetContainerCodeSpec.id,
        scope: IModel.rootSubjectId,
        value: sourceContainer.code.value,
      });
      targetContainerId =
        this.targetDb.elements.queryElementIdByCode(targetContainerCode);
    } catch {
      // catch NotFound error and continue
    }
    if (undefined === targetContainerId) {
      await this._remapSpatialCategories();
      await this._remapDrawingCategories();
      await this.exporter.exportElement(sourceContainerId);
      return this.exporter.exportModel(sourceContainerId);
    }
  }
  private async _remapSpatialCategories(): Promise<void> {
    if (
      undefined === this._targetSpatialCategories ||
      this._targetSpatialCategories.size === 0
    ) {
      return;
    }
    const sql = `SELECT ECInstanceId,CodeValue FROM ${SpatialCategory.classFullName}`;
    for await (const row of this.sourceDb.createQueryReader(sql, undefined, {
      usePrimaryConn: true,
    })) {
      const sourceCategoryId = row.id;
      const sourceCategoryName = row[1];
      if (this._targetSpatialCategories.has(sourceCategoryName)) {
        const targetCategoryId =
          this._targetSpatialCategories.get(sourceCategoryName)!;
        this.context.remapElement(sourceCategoryId, targetCategoryId);
        this.importer.doNotUpdateElementIds.add(targetCategoryId);
      }
    }
  }
  private async _remapDrawingCategories(): Promise<void> {
    if (
      undefined === this._targetDrawingCategories ||
      this._targetDrawingCategories.size === 0
    ) {
      return;
    }
    const sql = `SELECT ECInstanceId,CodeValue FROM ${DrawingCategory.classFullName}`;
    for await (const row of this.sourceDb.createQueryReader(sql, undefined, {
      usePrimaryConn: true,
    })) {
      const sourceCategoryId = row.id;
      const sourceCategoryName = row[1];
      if (this._targetDrawingCategories.has(sourceCategoryName)) {
        const targetCategoryId =
          this._targetDrawingCategories.get(sourceCategoryName)!;
        this.context.remapElement(sourceCategoryId, targetCategoryId);
        this.importer.doNotUpdateElementIds.add(targetCategoryId);
      }
    }
  }
}

/** Catalog test fixture */
describe.skip("Catalog", () => {
  const outputDir = path.join(BackendKnownTestLocations.outputDir, "Catalog");
  const acmeCatalogDbFile = IModelTestUtils.prepareOutputFile(
    "Catalog",
    "AcmeEquipment.catalog"
  ); // WIP: what file extension should catalogs have?
  const bestCatalogDbFile = IModelTestUtils.prepareOutputFile(
    "Catalog",
    "BestEquipment.catalog"
  ); // WIP: what file extension should catalogs have?
  const testCatalogDbFile = IModelTestUtils.prepareOutputFile(
    "Catalog",
    "Test.catalog"
  ); // WIP: what file extension should catalogs have?

  before(async () => {
    if (!IModelJsFs.existsSync(BackendKnownTestLocations.outputDir)) {
      IModelJsFs.mkdirSync(BackendKnownTestLocations.outputDir);
    }
    if (!IModelJsFs.existsSync(outputDir)) {
      IModelJsFs.mkdirSync(outputDir);
    }
    if (false) {
      // optionally initialize logging
      Logger.initializeToConsole();
      Logger.setLevelDefault(LogLevel.Error);
      Logger.setLevel(TransformerLoggerCategory.IModelExporter, LogLevel.Trace);
      Logger.setLevel(TransformerLoggerCategory.IModelImporter, LogLevel.Trace);
      Logger.setLevel(
        TransformerLoggerCategory.IModelTransformer,
        LogLevel.Trace
      );
    }
    await createAcmeCatalog(acmeCatalogDbFile);
    await createBestCatalog(bestCatalogDbFile);
    await createTestCatalog(testCatalogDbFile);
  });

  it("should index catalog", async () => {
    const acmeCatalogDb = SnapshotDb.openFile(acmeCatalogDbFile);
    await indexCatalog(acmeCatalogDb, `${acmeCatalogDb.pathName}.md`);
    acmeCatalogDb.close();

    const bestCatalogDb = SnapshotDb.openFile(bestCatalogDbFile);
    await indexCatalog(bestCatalogDb, `${bestCatalogDb.pathName}.md`);
    bestCatalogDb.close();

    const testCatalogDb = SnapshotDb.openFile(testCatalogDbFile);
    await indexCatalog(testCatalogDb, `${testCatalogDb.pathName}.md`);
    testCatalogDb.close();
  });

  it("should import from catalog", async () => {
    const iModelFile = IModelTestUtils.prepareOutputFile(
      "Catalog",
      "Facility.bim"
    );
    const iModelDb = SnapshotDb.createEmpty(iModelFile, {
      rootSubject: { name: "Facility" },
      createClassViews,
    });
    const domainSchemaFilePath = path.join(
      BackendKnownTestLocations.assetsDir,
      "TestDomain.ecschema.xml"
    );
    await iModelDb.importSchemas([domainSchemaFilePath]);
    const { physicalModelId, spatialCategoryId, drawingId, drawingCategoryId } =
      withEditTxn(iModelDb, "setup facility", (txn) => {
        const drawingListModelId = DocumentListModel.insert(
          txn,
          IModel.rootSubjectId,
          "Drawings"
        );
        return {
          physicalModelId: PhysicalModel.insert(
            txn,
            IModel.rootSubjectId,
            "Physical"
          ),
          spatialCategoryId: SpatialCategory.insert(
            txn,
            IModel.dictionaryId,
            "Equipment",
            new SubCategoryAppearance()
          ),
          drawingId: Drawing.insert(txn, drawingListModelId, "Drawing1"),
          drawingCategoryId: DrawingCategory.insert(
            txn,
            IModel.dictionaryId,
            "Symbols",
            new SubCategoryAppearance()
          ),
        };
      });
    const standardSpatialCategories = new Map<string, Id64String>();
    standardSpatialCategories.set("Equipment", spatialCategoryId);
    const standardDrawingCategories = new Map<string, Id64String>();
    standardDrawingCategories.set("Symbols", drawingCategoryId);

    const facilityEditTxn = createStartedEditTxn(iModelDb);

    {
      // import ACME Equipment catalog
      const catalogDb = SnapshotDb.openFile(acmeCatalogDbFile);
      const catalogContainerIds = await queryContainerIds(catalogDb);
      assert.equal(catalogContainerIds.size, 1); // expected value from createAcmeCatalog
      const catalogContainer =
        catalogDb.elements.getElement<DefinitionContainer>(
          // if non-null assertion operator is removed pnpm -r docs throws "Argument of type 'string | undefined' is not assignable to parameter of type 'string | Code | ElementLoadProps'."
          catalogContainerIds.values().next().value!,
          DefinitionContainer
        );
      const catalogContainerCodeSpec = catalogDb.codeSpecs.getById(
        catalogContainer.code.spec
      );
      const catalogContainerCodeValue = catalogContainer.code.value;
      const catalogRepositoryLinkId = insertCatalogRepositoryLink(
        iModelDb,
        path.basename(acmeCatalogDbFile),
        acmeCatalogDbFile,
        facilityEditTxn
      );
      const catalogImporter = await CatalogImporter.create(
        catalogDb,
        iModelDb,
        facilityEditTxn,
        catalogRepositoryLinkId,
        standardSpatialCategories,
        standardDrawingCategories
      );
      await catalogImporter.importDefinitionContainers();
      catalogImporter.dispose();
      catalogDb.close();

      // assert catalog was imported properly
      assert.isTrue(iModelDb.codeSpecs.hasName(catalogContainerCodeSpec.name));
      const importedContainerCodeSpec = iModelDb.codeSpecs.getByName(
        catalogContainerCodeSpec.name
      );
      const importedContainerId = iModelDb.elements.queryElementIdByCode(
        createContainerCode(
          importedContainerCodeSpec.id,
          catalogContainerCodeValue
        )
      )!;
      iModelDb.elements.getElement<DefinitionContainer>(
        importedContainerId,
        DefinitionContainer
      );
      iModelDb.models.getModel<DefinitionModel>(
        importedContainerId,
        DefinitionModel
      );
      assert.isUndefined(
        queryEquipmentCategory(iModelDb, importedContainerId),
        "Expected category to be remapped"
      );
      assert.isTrue(
        Id64.isValidId64(queryEquipmentCategory(iModelDb, IModel.dictionaryId)!)
      );
      assert.isTrue(
        Id64.isValidId64(
          queryEquipmentTypeId(iModelDb, importedContainerId, "A-101")!
        )
      );
      assert.isTrue(
        Id64.isValidId64(
          queryEquipmentTypeId(iModelDb, importedContainerId, "A-201")!
        )
      );
      assert.isTrue(
        Id64.isValidId64(
          queryEquipmentTypeId(iModelDb, importedContainerId, "A-301")!
        )
      );
      const templateRecipeIds = await queryTemplateRecipeIds(
        iModelDb,
        importedContainerId
      );
      assert.equal(templateRecipeIds.size, 6); // expected value from createAcmeCatalog
    }

    {
      // import Best Equipment catalog
      const catalogDb = SnapshotDb.openFile(bestCatalogDbFile);
      assert.equal(
        await countElementsInModel(
          catalogDb,
          DefinitionContainer.classFullName,
          IModel.dictionaryId
        ),
        2
      ); // expected value from createBestCatalog
      const catalogContainerSql = `SELECT ECInstanceId FROM ${DefinitionContainer.classFullName} WHERE CodeValue=:containerName LIMIT 1`;
      const params = new QueryBinder().bindString(
        "containerName",
        "Best Product Line B"
      );
      const reader = catalogDb.createQueryReader(catalogContainerSql, params, {
        usePrimaryConn: true,
      });
      let catalogContainerId = Id64.invalid;
      for await (const row of reader) {
        catalogContainerId = row[0] as Id64String;
        break;
      }
      const catalogContainer =
        catalogDb.elements.getElement<DefinitionContainer>(
          catalogContainerId,
          DefinitionContainer
        );
      const catalogContainerCodeSpec = catalogDb.codeSpecs.getById(
        catalogContainer.code.spec
      );
      const catalogContainerCodeValue = catalogContainer.code.value;
      const catalogRepositoryLinkId = insertCatalogRepositoryLink(
        iModelDb,
        path.basename(bestCatalogDbFile),
        bestCatalogDbFile,
        facilityEditTxn
      );
      const catalogImporter = await CatalogImporter.create(
        catalogDb,
        iModelDb,
        facilityEditTxn,
        catalogRepositoryLinkId,
        standardSpatialCategories,
        standardDrawingCategories
      );
      await catalogImporter.importDefinitionContainer(catalogContainerId); // only going to import 1 of the 2 containers
      catalogImporter.dispose();
      catalogDb.close();

      // assert catalog was imported properly
      assert.isTrue(iModelDb.codeSpecs.hasName(catalogContainerCodeSpec.name));
      const importedContainerCodeSpec = iModelDb.codeSpecs.getByName(
        catalogContainerCodeSpec.name
      );
      const importedContainerId = iModelDb.elements.queryElementIdByCode(
        createContainerCode(
          importedContainerCodeSpec.id,
          catalogContainerCodeValue
        )
      )!;
      iModelDb.elements.getElement<DefinitionContainer>(
        importedContainerId,
        DefinitionContainer
      );
      iModelDb.models.getModel<DefinitionModel>(
        importedContainerId,
        DefinitionModel
      );
      assert.isUndefined(
        queryEquipmentCategory(iModelDb, importedContainerId),
        "Expected category to be remapped"
      );
      assert.isTrue(
        Id64.isValidId64(queryEquipmentCategory(iModelDb, IModel.dictionaryId)!)
      );
      assert.isTrue(
        Id64.isValidId64(
          queryEquipmentTypeId(iModelDb, importedContainerId, "B-201")!
        )
      );
      assert.isTrue(
        Id64.isValidId64(
          queryEquipmentTypeId(iModelDb, importedContainerId, "B-304")!
        )
      );
      const templateRecipeIds = await queryTemplateRecipeIds(
        iModelDb,
        importedContainerId
      );
      assert.equal(templateRecipeIds.size, 2); // expected value from createBestCatalog
    }

    let testContainerId;
    {
      // import test catalog
      const catalogDb = SnapshotDb.openFile(testCatalogDbFile);
      const catalogContainerIds = await queryContainerIds(catalogDb);
      assert.equal(catalogContainerIds.size, 1); // expected value from createTestCatalog
      const catalogContainer =
        catalogDb.elements.getElement<DefinitionContainer>(
          // if non-null assertion operator is removed pnpm -r docs throws "Argument of type 'string | undefined' is not assignable to parameter of type 'string | Code | ElementLoadProps'."
          catalogContainerIds.values().next().value!,
          DefinitionContainer
        );
      const catalogContainerCodeSpec = catalogDb.codeSpecs.getById(
        catalogContainer.code.spec
      );
      const catalogContainerCodeValue = catalogContainer.code.value;
      const catalogRepositoryLinkId = insertCatalogRepositoryLink(
        iModelDb,
        path.basename(testCatalogDbFile),
        testCatalogDbFile,
        facilityEditTxn
      );
      const catalogTemplateRecipeIds = await queryTemplateRecipeIds(
        catalogDb,
        catalogContainer.id
      );
      assert.equal(catalogTemplateRecipeIds.size, 3); // expected value from createTestCatalog
      const catalogImporter = await CatalogImporter.create(
        catalogDb,
        iModelDb,
        facilityEditTxn,
        catalogRepositoryLinkId
      ); // no standard categories in this case
      const cylinderTemplateCode = TemplateRecipe3d.createCode(
        catalogDb,
        catalogContainer.id,
        "Cylinder Template"
      );
      const cylinderTemplateId =
        catalogDb.elements.queryElementIdByCode(cylinderTemplateCode)!;
      catalogImporter.exporter.excludeElement(cylinderTemplateId); // one way to implement partial import, another is by overriding shouldExportElement
      await catalogImporter.importDefinitionContainer(catalogContainer.id);
      catalogImporter.dispose();
      catalogDb.close();

      // assert catalog was imported properly
      assert.isTrue(iModelDb.codeSpecs.hasName(catalogContainerCodeSpec.name));
      const importedContainerCodeSpec = iModelDb.codeSpecs.getByName(
        catalogContainerCodeSpec.name
      );
      testContainerId = iModelDb.elements.queryElementIdByCode(
        createContainerCode(
          importedContainerCodeSpec.id,
          catalogContainerCodeValue
        )
      )!;
      iModelDb.elements.getElement<DefinitionContainer>(
        testContainerId,
        DefinitionContainer
      );
      iModelDb.models.getModel<DefinitionModel>(
        testContainerId,
        DefinitionModel
      );
      const importedTemplateRecipeIds = await queryTemplateRecipeIds(
        iModelDb,
        testContainerId
      );
      assert.equal(importedTemplateRecipeIds.size, 2); // excluded the "Cylinder" TemplateRecipe
    }

    const importedContainerIds = await queryContainerIds(iModelDb);
    assert.equal(
      importedContainerIds.size,
      3,
      "Expect 1 container from each of ACME, Best, and Test"
    );

    // iterate through the imported PhysicalTypes and place instances for each
    const componentPlacer = new TemplateModelCloner(
      iModelDb,
      iModelDb,
      facilityEditTxn
    );
    const physicalTypeSql = `SELECT ECInstanceId FROM ${PhysicalType.classFullName}`;
    const physicalTypeIds = new Set<Id64String>();
    for await (const row of iModelDb.createQueryReader(
      physicalTypeSql,
      undefined,
      { usePrimaryConn: true }
    )) {
      physicalTypeIds.add(row[0] as Id64String);
    }
    let x = 0;
    for (const physicalTypeId of physicalTypeIds) {
      x += 5;
      const physicalType = iModelDb.elements.getElement<PhysicalType>(
        physicalTypeId,
        PhysicalType
      );
      if (physicalType.recipe?.id) {
        iModelDb.elements.getElement<TemplateRecipe3d>(
          physicalType.recipe.id,
          TemplateRecipe3d
        );
        const placement = new Placement3d(
          new Point3d(x, 0),
          new YawPitchRollAngles(),
          new Range3d()
        );
        const templateToInstanceMap = await componentPlacer.placeTemplate3d(
          physicalType.recipe.id,
          physicalModelId,
          placement
        );
        const templateEquipmentId = await queryEquipmentId(
          iModelDb,
          physicalType.recipe.id
        );
        if (templateEquipmentId) {
          const instanceEquipmentId =
            templateToInstanceMap.get(templateEquipmentId);
          const equipmentClass = iModelDb.getJsClass(
            "TestDomain:Equipment"
          ) as unknown as EntityClassType<Element>;
          const equipment = iModelDb.elements.getElement<PhysicalElement>(
            instanceEquipmentId!,
            equipmentClass
          );
          equipment.typeDefinition = new PhysicalElementIsOfType(
            physicalTypeId
          );
          facilityEditTxn.updateElement(equipment.toJSON());
          assert.isDefined(equipment.typeDefinition?.id);
        }
      }
    }

    const assemblyTemplateCode = TemplateRecipe3d.createCode(
      iModelDb,
      testContainerId,
      "Assembly Template"
    );
    const assemblyTemplateId =
      iModelDb.elements.queryElementIdByCode(assemblyTemplateCode)!;
    const assemblyLocations: Point3d[] = [
      Point3d.create(-10, 0),
      Point3d.create(-20, 0),
      Point3d.create(-30, 0),
    ];
    for (const location of assemblyLocations) {
      const placement = new Placement3d(
        location,
        new YawPitchRollAngles(),
        new Range3d()
      );
      const templateToInstanceMap = await componentPlacer.placeTemplate3d(
        assemblyTemplateId,
        physicalModelId,
        placement
      );
      assert.isAtLeast(templateToInstanceMap.size, 2); // parent + child
      for (const templateElementId of templateToInstanceMap.keys()) {
        const templateElement = iModelDb.elements.getElement(templateElementId); // the element in the template model
        const instanceElement = iModelDb.elements.getElement(
          templateToInstanceMap.get(templateElementId)!
        ); // the element instantiated from the template element
        assert.isDefined(templateElement.federationGuid);
        assert.isDefined(instanceElement.federationGuid);
        assert.notStrictEqual(
          templateElement.federationGuid,
          instanceElement.federationGuid
        );
        assert.equal(
          templateElement.classFullName,
          instanceElement.classFullName
        );
        assert.equal(
          templateElement.parent?.id ? true : false,
          instanceElement.parent?.id ? true : false
        );
      }
    }

    const drawingGraphicTemplateCode = TemplateRecipe2d.createCode(
      iModelDb,
      testContainerId,
      "DrawingGraphic Template"
    );
    const drawingGraphicTemplateId = iModelDb.elements.queryElementIdByCode(
      drawingGraphicTemplateCode
    )!;
    const drawingGraphicLocations: Point2d[] = [
      Point2d.create(10, 10),
      Point2d.create(20, 10),
      Point2d.create(30, 10),
      Point2d.create(10, 20),
      Point2d.create(20, 20),
      Point2d.create(30, 20),
      Point2d.create(10, 30),
      Point2d.create(20, 30),
      Point2d.create(30, 30),
    ];
    assert.equal(
      await countElementsInModel(
        iModelDb,
        DrawingGraphic.classFullName,
        drawingId
      ),
      0
    );
    for (const location of drawingGraphicLocations) {
      const placement = new Placement2d(location, Angle.zero(), new Range2d());
      await componentPlacer.placeTemplate2d(
        drawingGraphicTemplateId,
        drawingId,
        placement
      );
    }
    assert.equal(
      await countElementsInModel(
        iModelDb,
        DrawingGraphic.classFullName,
        drawingId
      ),
      drawingGraphicLocations.length
    );

    componentPlacer.dispose();

    facilityEditTxn.saveChanges("import from catalog");
    facilityEditTxn.end();
    iModelDb.close();
  });

  /** Verifies that a "catalog.bim" can be completely cloned.
   * @note This serves as a good test, but not sure if this will actually be useful in production.
   */
  it("should clone catalog", async () => {
    const sourceDb = SnapshotDb.openFile(acmeCatalogDbFile);
    const targetFile = IModelTestUtils.prepareOutputFile(
      "Catalog",
      "CloneOfAcmeEquipment.bim"
    );
    const targetDb = SnapshotDb.createEmpty(targetFile, {
      rootSubject: { name: "Facility" },
      createClassViews,
    });
    const cloneEditTxn = createStartedEditTxn(targetDb);
    const target = new IModelImporter(targetDb, cloneEditTxn, {
      autoExtendProjectExtents: false,
    }); // WIP: how should a catalog handle projectExtents?
    const cloner = new IModelTransformer(sourceDb, target, cloneEditTxn);
    await cloner.processSchemas();
    await cloner.process();
    cloner.dispose();

    const containerIds = await queryContainerIds(targetDb);
    assert.equal(containerIds.size, 1);
    containerIds.forEach((containerId) => {
      // assert that the cloned target contains the expected elements
      targetDb.elements.getElement<DefinitionContainer>(
        containerId,
        DefinitionContainer
      );
      targetDb.models.getModel<DefinitionModel>(containerId, DefinitionModel);
      assert.isTrue(
        Id64.isValidId64(queryEquipmentCategory(targetDb, containerId)!)
      );
      assert.isTrue(
        Id64.isValidId64(queryEquipmentTypeId(targetDb, containerId, "A-101")!)
      );
      assert.isTrue(
        Id64.isValidId64(queryEquipmentTypeId(targetDb, containerId, "A-201")!)
      );
      assert.isTrue(
        Id64.isValidId64(queryEquipmentTypeId(targetDb, containerId, "A-301")!)
      );
    });

    sourceDb.close();
    cloneEditTxn.saveChanges();
    targetDb.close();
  });
});
