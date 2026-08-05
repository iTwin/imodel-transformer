/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

import { Schema, SchemaKey } from "@itwin/ecschema-metadata";

/**
 * The result of processing one schema.
 * @beta
 */
export type SchemaProcessingResult =
  | { kind: "source"; schema: Schema }
  | { kind: "generated"; schema: Schema };

/** Read-only access to schemas already present in the target iModel.
 * @beta
 */
export interface ReadonlySchemaAccessor {
  /** Load a target schema by name, or return `undefined` when it is absent. */
  getSchema(schemaName: string): Promise<Schema | undefined>;
}

/**
 * Context supplied to a [[SchemaProcessingStrategy]].
 * @beta
 */
export interface SchemaProcessingContext {
  /** Source schemas in dependency order. */
  readonly sourceSchemas: readonly Schema[];
  /** Read-only access to schemas already present in the target. */
  readonly targetSchemas: ReadonlySchemaAccessor;
  /** The schema-selection hook used by the default transformer path. */
  readonly shouldExportSchema: (schemaKey: SchemaKey) => Promise<boolean>;
}

/**
 * Selects source schemas and produces definitions for import into a target iModel.
 * @beta
 */
export interface SchemaProcessingStrategy {
  /** Select source schemas and produce definitions for import into the target iModel. */
  processSchemas(
    context: SchemaProcessingContext
  ): Promise<SchemaProcessingResult[]>;
}

/**
 * Options for [[IModelTransformer.processSchemas]].
 * @beta
 */
export interface ProcessSchemasOptions {
  /** Strategy used to select and process source schemas. */
  strategy?: SchemaProcessingStrategy;
}

/**
 * Selects source schemas that are absent from the target or have a newer version.
 * @beta
 */
export class NewerVersionSchemaImportStrategy
  implements SchemaProcessingStrategy
{
  /** Select schemas that are absent from the target or have a newer version. */
  public async processSchemas(
    context: SchemaProcessingContext
  ): Promise<SchemaProcessingResult[]> {
    const results: SchemaProcessingResult[] = [];
    for (const schema of context.sourceSchemas) {
      if (await context.shouldExportSchema(schema.schemaKey)) {
        results.push({ kind: "source", schema });
      }
    }
    return results;
  }
}
