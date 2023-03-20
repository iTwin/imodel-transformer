/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { assert, expect } from "chai";
import * as fs from "fs";
import * as path from "path";
import * as Semver from "semver";
import * as sinon from "sinon";
import {
  CategorySelector, DisplayStyle3d, DocumentListModel, Drawing, DrawingCategory, DrawingGraphic, DrawingModel, ECSqlStatement, Element,
  ElementMultiAspect, ElementOwnsChildElements, ElementOwnsExternalSourceAspects, ElementOwnsMultiAspects, ElementOwnsUniqueAspect, ElementRefersToElements,
  ElementUniqueAspect, ExternalSourceAspect, GenericPhysicalMaterial, GeometricElement, IModelDb, IModelElementCloneContext, IModelHost, IModelJsFs,
  InformationRecordModel, InformationRecordPartition, LinkElement, Model, ModelSelector, OrthographicViewDefinition,
  PhysicalModel, PhysicalObject, PhysicalPartition, PhysicalType, Relationship, RenderMaterialElement, RepositoryLink, Schema, SnapshotDb, SpatialCategory, StandaloneDb,
  SubCategory, Subject, Texture
} from "@itwin/core-backend";
//import * as ECSchemaMetaData from "@itwin/ecschema-metadata";
import * as BackendTestUtils from "@itwin/core-backend/lib/cjs/test";
import { DbResult, Guid, Id64, Id64String, Logger, LogLevel, OpenMode } from "@itwin/core-bentley";
import {
  AxisAlignedBox3d, BriefcaseIdValue, Code, CodeScopeSpec, CodeSpec, ColorDef, CreateIModelProps, DefinitionElementProps, ElementAspectProps, ElementProps,
  ExternalSourceAspectProps, ImageSourceFormat, IModel, IModelError, PhysicalElementProps, Placement3d, ProfileOptions, QueryRowFormat, RelatedElement, RelationshipProps,
} from "@itwin/core-common";
//import { Point3d, Range3d, StandardViewIndex, Transform, YawPitchRollAngles } from "@itwin/core-geometry";
import { IModelExporter, IModelExportHandler, IModelTransformer, IModelTransformOptions, TransformerLoggerCategory } from "../../core-transformer";
import {
  AspectTrackingImporter,
  AspectTrackingTransformer,
  assertIdentityTransformation, AssertOrderTransformer,
  ClassCounter, FilterByViewTransformer, IModelToTextFileExporter, IModelTransformer3d, IModelTransformerTestUtils, PhysicalModelConsolidator,
  RecordingIModelImporter, runWithCpuProfiler, TestIModelTransformer, TransformerExtensiveTestScenario,
} from "../IModelTransformerUtils";
import { KnownTestLocations } from "../KnownTestLocations";

//import "./TransformerTestStartup"; // calls startup/shutdown IModelHost before/after all tests
import { SchemaLoader } from "@itwin/ecschema-metadata";

async function main() {
  await IModelHost.startup();
  if (process.env.LOG) {
    Logger.initializeToConsole();
    Logger.setLevelDefault(LogLevel.Error);
    Logger.setLevel(TransformerLoggerCategory.IModelExporter, LogLevel.Trace);
    Logger.setLevel(TransformerLoggerCategory.IModelImporter, LogLevel.Trace);
    Logger.setLevel(TransformerLoggerCategory.IModelTransformer, LogLevel.Trace);
  }

  const seedDb = SnapshotDb.openFile("/home/mike/work/Juergen.Hofer.Bad.Normals.bim");
  const sourceDbPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", "ExhaustiveIdentityTransformSource.bim");
  const sourceDb = SnapshotDb.createFrom(seedDb, sourceDbPath);

  // previously there was a bug where json display properties of models would not be transformed. This should expose that
  const [physicalModelId] = sourceDb.queryEntityIds({ from: "BisCore.PhysicalModel", limit: 1 });
  const physicalModel = sourceDb.models.getModel(physicalModelId);
  physicalModel.jsonProperties.formatter.fmtFlags.linPrec = 100;
  physicalModel.update();

  sourceDb.saveChanges();

  const targetDbPath = IModelTransformerTestUtils.prepareOutputFile("IModelTransformer", "ExhaustiveIdentityTransformTarget.bim");
  const targetDb = SnapshotDb.createEmpty(targetDbPath, { rootSubject: sourceDb.rootSubject });

  const transformer = new IModelTransformer(sourceDb, targetDb);

  await transformer.processSchemas();

  await transformer.processAll();

  targetDb.saveChanges();

  await assertIdentityTransformation(sourceDb, targetDb, transformer, { compareElemGeom: true });

  const physicalModelInTargetId = transformer.context.findTargetElementId(physicalModelId);
  const physicalModelInTarget = targetDb.models.getModel(physicalModelInTargetId);
  expect(physicalModelInTarget.jsonProperties.formatter.fmtFlags.linPrec).to.equal(100);

  seedDb.close();
  sourceDb.close();
  targetDb.close();
}

main().catch(console.error);
