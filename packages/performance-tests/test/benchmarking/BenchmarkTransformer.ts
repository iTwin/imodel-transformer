/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Element, Relationship } from "@itwin/core-backend";
import {
  IModelTransformer,
  IModelTransformArgs,
  IModelTransformOptions,
} from "@itwin/imodel-transformer";
import { Schema } from "@itwin/ecschema-metadata";
import { BenchmarkStats, createEmptyStats } from "./BenchmarkStats";

/**
 * An IModelTransformer subclass that captures timing data for the major
 * transformation phases (processSchemas, process) and counts of exported entities.
 */
export class BenchmarkTransformer extends IModelTransformer {
  private readonly _stats: BenchmarkStats;

  public get stats(): BenchmarkStats {
    return this._stats;
  }

  public constructor(
    args: IModelTransformArgs,
    options?: IModelTransformOptions
  ) {
    super(args, options);
    this._stats = createEmptyStats();
  }

  public override async processSchemas(): Promise<void> {
    const start = performance.now();
    await super.processSchemas();
    this._stats.schemaTotalMs = performance.now() - start;
  }

  public override async onExportSchema(schema: Schema): Promise<void> {
    await super.onExportSchema(schema);
    this._stats.schemaCount++;
  }

  public override async process(): Promise<void> {
    const start = performance.now();
    await super.process();
    this._stats.processTotalMs = performance.now() - start;
  }

  public override async onExportElement(sourceElement: Element): Promise<void> {
    await super.onExportElement(sourceElement);
    this._stats.exportElementCount++;
  }

  public override async onExportRelationship(
    sourceRelationship: Relationship
  ): Promise<void> {
    await super.onExportRelationship(sourceRelationship);
    this._stats.exportRelationshipCount++;
  }
}
