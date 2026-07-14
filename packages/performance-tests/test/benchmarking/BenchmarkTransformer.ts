/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { Element, Relationship } from "@itwin/core-backend";
import {
  IModelTransformer,
  IModelTransformArgs,
  IModelTransformOptions,
  ExportSchemaResult,
} from "@itwin/imodel-transformer";
import { Schema } from "@itwin/ecschema-metadata";
import { BenchmarkStats, createEmptyStats } from "./BenchmarkStats";
import { BenchmarkImporter } from "./BenchmarkImporter";

/**
 * An IModelTransformer subclass that captures detailed timing data for all
 * phases of a transformation: schema export, element export, and the overall
 * processSchemas/process calls.
 *
 * Uses a BenchmarkImporter for capturing import-side timing.
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
    // If a BenchmarkImporter was passed, reuse its stats object
    if (args.target instanceof BenchmarkImporter) {
      this._stats = (args.target as any)._stats;
    } else {
      this._stats = createEmptyStats();
    }
  }

  public override async processSchemas(): Promise<void> {
    const start = performance.now();
    await super.processSchemas();
    this._stats.schemaTotalMs = performance.now() - start;
  }

  public override async onExportSchema(
    schema: Schema
  ): Promise<void | ExportSchemaResult> {
    const start = performance.now();
    const result = await super.onExportSchema(schema);
    const elapsed = performance.now() - start;
    this._stats.schemaExportTimes.set(schema.name, elapsed);
    this._stats.schemaCount++;
    return result;
  }

  public override async process(): Promise<void> {
    const start = performance.now();
    await super.process();
    this._stats.processTotalMs = performance.now() - start;
  }

  public override async onExportElement(sourceElement: Element): Promise<void> {
    const start = performance.now();
    await super.onExportElement(sourceElement);
    this._stats.exportElementMs += performance.now() - start;
    this._stats.exportElementCount++;
  }

  public override async onExportRelationship(
    sourceRelationship: Relationship
  ): Promise<void> {
    const start = performance.now();
    await super.onExportRelationship(sourceRelationship);
    this._stats.exportRelationshipMs += performance.now() - start;
    this._stats.exportRelationshipCount++;
  }
}
