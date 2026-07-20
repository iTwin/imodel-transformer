/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

import { IModelDb } from "@itwin/core-backend";
import {
  AnySchemaDifferenceConflict,
  ConflictCode,
  getSchemaDifferences,
  SchemaDifferenceResult,
  SchemaMerger,
} from "@itwin/ecschema-editing";
import {
  ECVersion,
  Schema,
  SchemaContext,
  SchemaKey,
  SchemaLoader,
} from "@itwin/ecschema-metadata";
import {
  createSchemaProcessingError,
  SchemaProcessingErrorKey,
} from "./SchemaProcessingErrors";

/**
 * The result of processing one schema.
 * @beta
 */
export type SchemaProcessingResult =
  | { kind: "source"; schema: Schema }
  | { kind: "generated"; schema: Schema };

/**
 * Context supplied to a [[SchemaProcessingStrategy]].
 * @beta
 */
export interface SchemaProcessingContext {
  /** Source schemas in dependency order. */
  readonly sourceSchemas: readonly Schema[];
  /**
   * Target iModel containing the schemas being processed.
   * Strategies must use this database only for inspection and must not modify it.
   */
  readonly targetDb: IModelDb;
  /**
   * The schema-selection hook used by the default transformer path.
   * Strategies may use a different selection policy.
   */
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
 * Selects source schemas that are absent from the target or have a newer
 * version.
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
    const errors: Error[] = [];

    for (const schema of context.sourceSchemas) {
      try {
        if (await context.shouldExportSchema(schema.schemaKey)) {
          results.push({ kind: "source", schema });
        }
      } catch (error: unknown) {
        errors.push(
          createSchemaProcessingError({
            schema,
            operation: "Schema selection failed",
            cause: error,
          })
        );
      }
    }

    throwIfSchemaProcessingErrors(errors);
    return results;
  }
}

/**
 * Processes ordinary schemas with newer-version selection and unions
 * compatible differences for existing dynamic schemas. Dynamic schema root
 * read and write versions must match; only the minor version may differ.
 * @beta
 */
export class DynamicSchemaUnionStrategy implements SchemaProcessingStrategy {
  /**
   * Transform schema differences before conflict validation and merging.
   * A subclass that changes the differencing result is responsible for ensuring
   * that its changes are safe for the source and target schemas.
   * @param sourceSchema The source schema being processed.
   * @param targetSchema The existing target schema.
   * @param differences The complete result from [[getSchemaDifferences]].
   * @returns The differencing result to validate and merge.
   * @beta
   */
  protected async onSchemaDifferences(
    _sourceSchema: Schema,
    _targetSchema: Schema,
    differences: SchemaDifferenceResult
  ): Promise<SchemaDifferenceResult> {
    return differences;
  }

  /** Select ordinary schemas and union compatible dynamic schema differences. */
  public async processSchemas(
    context: SchemaProcessingContext
  ): Promise<SchemaProcessingResult[]> {
    const results: SchemaProcessingResult[] = [];
    const errors: Error[] = [];
    const targetSchemaLoader = new SchemaLoader((name: string) =>
      context.targetDb.getSchemaProps(name)
    );
    const schemaEditingContext = new SchemaContext();
    const schemaMerger = new SchemaMerger(schemaEditingContext);
    const existingSchemas = new Map<
      string,
      { schema: Schema; version: ECVersion }
    >();
    const schemaCandidates = new Map<
      string,
      { schema: Schema; isTargetRoot: boolean }
    >();
    const dynamicSchemaDifferences = new Map<string, SchemaDifferenceResult>();
    const noOpDynamicSchemas = new Map<string, Schema>();

    for (const sourceSchema of context.sourceSchemas) {
      const schemaName = sourceSchema.name.toLowerCase();
      const targetVersionString = context.targetDb.querySchemaVersion(
        sourceSchema.name
      );
      if (targetVersionString === undefined) continue;

      let targetSchema: Schema;
      let targetVersion: ECVersion;
      try {
        const loadedTargetSchema = targetSchemaLoader.getSchema(
          sourceSchema.name
        );
        if (loadedTargetSchema === undefined) {
          throw new Error(
            `Target schema '${sourceSchema.name}' could not be loaded`
          );
        }
        targetSchema = loadedTargetSchema;
        targetVersion = ECVersion.fromString(targetVersionString);
        existingSchemas.set(schemaName, {
          schema: targetSchema,
          version: targetVersion,
        });
      } catch (error: unknown) {
        errors.push(
          createSchemaProcessingError({
            schema: sourceSchema,
            operation: "Schema context setup failed",
            cause: error,
          })
        );
        continue;
      }

      if (!sourceSchema.isDynamic || !targetSchema.isDynamic) continue;
      const sourceVersion = sourceSchema.schemaKey.version;
      if (
        sourceVersion.read !== targetVersion.read ||
        sourceVersion.write !== targetVersion.write
      )
        continue;

      try {
        const differences = await this.onSchemaDifferences(
          sourceSchema,
          targetSchema,
          await getSchemaDifferences(targetSchema, sourceSchema)
        );
        const unresolvedConflicts = (differences.conflicts ?? []).filter(
          (conflict) => !isCompatibleReferenceVersionConflict(conflict)
        );
        if (unresolvedConflicts.length > 0) {
          throw createSchemaProcessingError({
            schema: sourceSchema,
            operation: "Schema conflict",
            cause: new Error(
              formatSchemaConflicts(sourceSchema, unresolvedConflicts)
            ),
            key: SchemaProcessingErrorKey.SchemaConflict,
          });
        }
        dynamicSchemaDifferences.set(schemaName, differences);
        if (differences.differences.length === 0) {
          noOpDynamicSchemas.set(schemaName, sourceSchema);
        }
        if (differences.differences.length > 0) {
          collectSchemaCandidates(targetSchema, schemaCandidates, true);
        } else {
          collectSchemaCandidates(sourceSchema, schemaCandidates, false);
        }
        collectSchemaReferences(sourceSchema, schemaCandidates);
      } catch (error: unknown) {
        errors.push(
          createSchemaProcessingError({
            schema: sourceSchema,
            operation: "Schema processing failed",
            cause: error,
          })
        );
      }
    }

    throwIfSchemaProcessingErrors(errors);
    for (const { schema } of schemaCandidates.values()) {
      try {
        await schemaEditingContext.addSchema(schema);
      } catch (error: unknown) {
        errors.push(
          createSchemaProcessingError({
            schema,
            operation: "Schema context setup failed",
            cause: error,
          })
        );
      }
    }
    throwIfSchemaProcessingErrors(errors);

    for (const sourceSchema of context.sourceSchemas) {
      try {
        const existingSchema = existingSchemas.get(
          sourceSchema.name.toLowerCase()
        );
        if (existingSchema === undefined) {
          if (
            sourceSchema.isDynamic ||
            (await context.shouldExportSchema(sourceSchema.schemaKey))
          ) {
            results.push({ kind: "source", schema: sourceSchema });
          }
          continue;
        }

        const { schema: targetSchema, version: targetVersion } = existingSchema;

        if (sourceSchema.isDynamic !== targetSchema.isDynamic) {
          throw createSchemaProcessingError({
            schema: sourceSchema,
            operation: "Schema DynamicSchema marker changed",
            cause: new Error(
              "The source and target DynamicSchema markers differ."
            ),
            key: SchemaProcessingErrorKey.SchemaConflict,
          });
        }

        if (!sourceSchema.isDynamic) {
          if (await context.shouldExportSchema(sourceSchema.schemaKey)) {
            results.push({ kind: "source", schema: sourceSchema });
          }
          continue;
        }

        const sourceVersion = sourceSchema.schemaKey.version;
        if (
          sourceVersion.read !== targetVersion.read ||
          sourceVersion.write !== targetVersion.write
        ) {
          throw createSchemaProcessingError({
            schema: sourceSchema,
            operation: "Dynamic schema has incompatible root versions",
            cause: new Error(
              `source ${sourceVersion.toString(false)}, target ${targetVersion.toString(false)}`
            ),
            key: SchemaProcessingErrorKey.SchemaConflict,
          });
        }

        const differences = dynamicSchemaDifferences.get(
          sourceSchema.name.toLowerCase()
        );
        if (differences === undefined) {
          throw new Error(
            `No differencing result was produced for dynamic schema '${sourceSchema.name}'`
          );
        }
        if (differences.differences.length === 0) continue;

        const mergedSchema = await schemaMerger.merge({
          ...differences,
          conflicts: undefined,
        });
        mergedSchema.setVersion(
          sourceVersion.read,
          sourceVersion.write,
          incrementMinorVersion(sourceVersion, targetVersion)
        );
        results.push({ kind: "generated", schema: mergedSchema });
      } catch (error: unknown) {
        errors.push(
          createSchemaProcessingError({
            schema: sourceSchema,
            operation: "Schema processing failed",
            cause: error,
          })
        );
      }
    }

    const requiredNoOpSchemas = new Set<string>();
    const schemasToVisit = results.map((result) => result.schema);
    for (const schema of schemasToVisit) {
      for (const reference of schema.references) {
        const schemaName = reference.name.toLowerCase();
        const noOpSchema = noOpDynamicSchemas.get(schemaName);
        if (noOpSchema === undefined || requiredNoOpSchemas.has(schemaName))
          continue;

        requiredNoOpSchemas.add(schemaName);
        schemasToVisit.push(noOpSchema);
      }
    }
    for (const [schemaName, schema] of noOpDynamicSchemas) {
      if (requiredNoOpSchemas.has(schemaName)) {
        results.push({ kind: "source", schema });
      }
    }

    throwIfSchemaProcessingErrors(errors);
    return results;
  }
}

function incrementMinorVersion(
  sourceVersion: ECVersion,
  targetVersion: ECVersion
): number {
  const maximumVersion =
    sourceVersion.compare(targetVersion) >= 0 ? sourceVersion : targetVersion;
  if (maximumVersion.minor >= 9_999_999) {
    throw new Error(
      `Cannot increment schema version ${maximumVersion.toString(false)}`
    );
  }
  return maximumVersion.minor + 1;
}

function isCompatibleReferenceVersionConflict(
  conflict: AnySchemaDifferenceConflict
): boolean {
  if (conflict.code !== ConflictCode.ConflictingReferenceVersion) return false;
  if (
    typeof conflict.source !== "string" ||
    typeof conflict.target !== "string"
  )
    return false;

  const sourceVersion = SchemaKey.parseString(conflict.source).version;
  const targetVersion = SchemaKey.parseString(conflict.target).version;
  return (
    sourceVersion.read === targetVersion.read &&
    sourceVersion.write === targetVersion.write
  );
}

function formatSchemaConflicts(
  schema: Schema,
  conflicts: AnySchemaDifferenceConflict[]
): string {
  const details = conflicts.map((conflict) => {
    const itemName =
      "itemName" in conflict.difference
        ? ` item '${conflict.difference.itemName}'`
        : "";
    const path =
      "path" in conflict.difference
        ? ` path '${conflict.difference.path}'`
        : "";
    return (
      `${conflict.code}${itemName}${path}: ${conflict.description} ` +
      `(source: ${String(conflict.source)}, target: ${String(conflict.target)})`
    );
  });
  return `Schema '${schema.name}' has unresolved conflicts: ${details.join(
    "; "
  )}`;
}

function throwIfSchemaProcessingErrors(errors: Error[]): void {
  if (errors.length === 1) throw errors[0];
  if (errors.length > 1)
    throw new AggregateError(errors, "Schema processing failed");
}

function collectSchemaCandidates(
  schema: Schema,
  candidates: Map<string, { schema: Schema; isTargetRoot: boolean }>,
  isTargetRoot: boolean,
  visited: Set<string> = new Set<string>()
): void {
  const traversalKey = schema.schemaKey.toString();
  if (visited.has(traversalKey)) return;
  visited.add(traversalKey);

  const schemaName = schema.name.toLowerCase();
  const existing = candidates.get(schemaName);
  if (existing === undefined) {
    candidates.set(schemaName, { schema, isTargetRoot });
  } else if (
    existing.schema.schemaKey.version.read !== schema.schemaKey.version.read ||
    existing.schema.schemaKey.version.write !== schema.schemaKey.version.write
  ) {
    throw createSchemaProcessingError({
      schema,
      operation: "Schema reference has incompatible read/write versions",
      cause: new Error(
        `${existing.schema.schemaKey.version.read}.${existing.schema.schemaKey.version.write} ` +
          `and ${schema.schemaKey.version.read}.${schema.schemaKey.version.write} differ. ` +
          "Only minor-version reference differences are supported."
      ),
      key: SchemaProcessingErrorKey.SchemaConflict,
    });
  } else if (
    isTargetRoot ||
    (!existing.isTargetRoot &&
      schema.schemaKey.version.compare(existing.schema.schemaKey.version) > 0)
  ) {
    candidates.set(schemaName, { schema, isTargetRoot });
  }

  for (const reference of schema.references) {
    collectSchemaCandidates(reference, candidates, false, visited);
  }
}

function collectSchemaReferences(
  schema: Schema,
  candidates: Map<string, { schema: Schema; isTargetRoot: boolean }>
): void {
  for (const reference of schema.references) {
    collectSchemaCandidates(reference, candidates, false);
  }
}
