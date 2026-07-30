/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import {
  // eslint-disable-next-line @typescript-eslint/no-redeclare
  Element,
  ElementAspect,
  ElementMultiAspect,
  ElementUniqueAspect,
  Model,
  Relationship,
} from "@itwin/core-backend";
import { Id64String } from "@itwin/core-bentley";
import { CodeSpec, FontProps } from "@itwin/core-common";
import { Schema, SchemaKey } from "@itwin/ecschema-metadata";
import { ExportSchemaResult, IModelExportHandler } from "./IModelExporter";
import type { IModelTransformer } from "./IModelTransformer";

/** Adapts IModelExporter callbacks to IModelTransformer operations.
 * @internal
 */
export class IModelTransformerExportHandler extends IModelExportHandler {
  public constructor(private readonly _transformer: IModelTransformer) {
    super();
  }

  public override async shouldExportCodeSpec(
    codeSpec: CodeSpec
  ): Promise<boolean> {
    return this._transformer.shouldExportCodeSpec(codeSpec);
  }

  public override async onExportCodeSpec(
    codeSpec: CodeSpec,
    _isUpdate: boolean | undefined
  ): Promise<void> {
    return this._transformer.onExportCodeSpec(codeSpec);
  }

  public override async onExportFont(
    font: FontProps,
    isUpdate: boolean | undefined
  ): Promise<void> {
    return this._transformer.onExportFont(font, isUpdate);
  }

  public override async onExportModel(
    model: Model,
    _isUpdate: boolean | undefined
  ): Promise<void> {
    return this._transformer.onExportModel(model);
  }

  public override async onDeleteModel(modelId: Id64String): Promise<void> {
    return this._transformer.onDeleteModel(modelId);
  }

  public override async shouldExportElement(
    element: Element
  ): Promise<boolean> {
    return this._transformer.shouldExportElement(element);
  }

  public override async onSkipElement(elementId: Id64String): Promise<void> {
    return this._transformer.onSkipElement(elementId);
  }

  public override async preExportElement(element: Element): Promise<void> {
    return this._transformer.preExportElement(element);
  }

  public override async onExportElement(
    element: Element,
    _isUpdate: boolean | undefined
  ): Promise<void> {
    return this._transformer.onExportElement(element);
  }

  public override async onDeleteElement(elementId: Id64String): Promise<void> {
    return this._transformer.onDeleteElement(elementId);
  }

  public override async shouldExportElementAspect(
    aspect: ElementAspect
  ): Promise<boolean> {
    return this._transformer.shouldExportElementAspect(aspect);
  }

  public override async onExportElementUniqueAspect(
    aspect: ElementUniqueAspect,
    _isUpdate: boolean | undefined
  ): Promise<void> {
    return this._transformer.onExportElementUniqueAspect(aspect);
  }

  public override async onExportElementMultiAspects(
    aspects: ElementMultiAspect[]
  ): Promise<void> {
    return this._transformer.onExportElementMultiAspects(aspects);
  }

  public override async shouldExportRelationship(
    relationship: Relationship
  ): Promise<boolean> {
    return this._transformer.shouldExportRelationship(relationship);
  }

  public override async onExportRelationship(
    relationship: Relationship,
    _isUpdate: boolean | undefined
  ): Promise<void> {
    return this._transformer.onExportRelationship(relationship);
  }

  public override async onDeleteRelationship(
    relInstanceId: Id64String
  ): Promise<void> {
    return this._transformer.onDeleteRelationship(relInstanceId);
  }

  public override async shouldExportSchema(
    schemaKey: SchemaKey
  ): Promise<boolean> {
    return this._transformer.shouldExportSchema(schemaKey);
  }

  public override async onExportSchema(
    schema: Schema
  ): Promise<void | ExportSchemaResult> {
    return this._transformer.onExportSchema(schema);
  }

  public override async onProgress(): Promise<void> {
    return this._transformer.onProgress();
  }
}
