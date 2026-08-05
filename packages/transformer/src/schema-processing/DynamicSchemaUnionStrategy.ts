/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

import { ITwinError } from "@itwin/core-bentley";
import {
  AnySchemaDifferenceConflict,
  ConflictCode,
  getSchemaDifferences,
  SchemaConflictsError,
  SchemaDifferenceResult,
  SchemaMerger,
} from "@itwin/ecschema-editing";
import {
  ECVersion,
  Schema,
  SchemaContext,
  SchemaKey,
} from "@itwin/ecschema-metadata";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "../IModelTransformerError";
import {
  ReadonlySchemaAccessor,
  SchemaProcessingContext,
  SchemaProcessingResult,
  SchemaProcessingStrategy,
} from "./SchemaProcessingStrategy";

type SchemaPlan =
  | { kind: "new"; source: Schema }
  | { kind: "ordinary"; source: Schema; target: Schema }
  | { kind: "dynamic-noop"; source: Schema; target: Schema }
  | {
      kind: "dynamic-merge";
      source: Schema;
      target: Schema;
      differences: SchemaDifferenceResult;
    };

interface SchemaCandidate {
  schema: Schema;
  isTargetRoot: boolean;
}

interface SchemaConflictError extends ITwinError {
  schemaKey: string;
}

/**
 * Processes ordinary schemas with newer-version selection and unions compatible
 * differences for existing dynamic schemas. Dynamic schema root read and write
 * versions must match; only the minor version may differ.
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
    const plans: SchemaPlan[] = [];
    const conflicts: Error[] = [];
    for (const sourceSchema of context.sourceSchemas) {
      try {
        plans.push(await this.createPlan(sourceSchema, context.targetSchemas));
      } catch (error: unknown) {
        if (
          ITwinError.isError(
            error,
            IModelTransformerErrorScope,
            IModelTransformerError.SchemaConflict
          )
        ) {
          conflicts.push(error);
          continue;
        }
        throw error;
      }
    }
    throwSchemaConflicts(conflicts);

    const schemaEditingContext = new SchemaContext();
    const schemaCandidates = new Map<string, SchemaCandidate>();
    for (const plan of plans) {
      if (plan.kind === "dynamic-merge") {
        collectSchemaCandidates(plan.target, schemaCandidates, true);
        collectSchemaReferences(plan.source, schemaCandidates);
      } else if (plan.kind === "dynamic-noop") {
        collectSchemaCandidates(plan.source, schemaCandidates, false);
        collectSchemaReferences(plan.source, schemaCandidates);
      }
    }
    for (const { schema } of schemaCandidates.values()) {
      await schemaEditingContext.addSchema(schema);
    }

    const schemaMerger = new SchemaMerger(schemaEditingContext);
    const results: SchemaProcessingResult[] = [];
    for (const plan of plans) {
      switch (plan.kind) {
        case "new":
          if (
            plan.source.isDynamic ||
            (await context.shouldExportSchema(plan.source.schemaKey))
          ) {
            results.push({ kind: "source", schema: plan.source });
          }
          break;
        case "ordinary":
          if (await context.shouldExportSchema(plan.source.schemaKey)) {
            results.push({ kind: "source", schema: plan.source });
          }
          break;
        case "dynamic-noop":
          break;
        case "dynamic-merge": {
          const mergedSchema = await schemaMerger.merge({
            ...plan.differences,
            conflicts: undefined,
          });
          const sourceVersion = plan.source.schemaKey.version;
          mergedSchema.setVersion(
            sourceVersion.read,
            sourceVersion.write,
            incrementMinorVersion(sourceVersion, plan.target.schemaKey.version)
          );
          results.push({ kind: "generated", schema: mergedSchema });
          break;
        }
      }
    }

    appendRequiredNoOpSchemas(results, plans);
    return results;
  }

  private async createPlan(
    source: Schema,
    targetSchemas: ReadonlySchemaAccessor
  ): Promise<SchemaPlan> {
    const target = await targetSchemas.getSchema(source.name);
    if (target === undefined) return { kind: "new", source };

    if (source.isDynamic !== target.isDynamic) {
      throw createSchemaConflictError(
        source,
        "Schema DynamicSchema marker changed",
        new Error("The source and target DynamicSchema markers differ.")
      );
    }
    if (!source.isDynamic) return { kind: "ordinary", source, target };

    const sourceVersion = source.schemaKey.version;
    const targetVersion = target.schemaKey.version;
    if (
      sourceVersion.read !== targetVersion.read ||
      sourceVersion.write !== targetVersion.write
    ) {
      throw createSchemaConflictError(
        source,
        "Dynamic schema has incompatible root versions",
        new Error(
          `source ${sourceVersion.toString(false)}, target ${targetVersion.toString(false)}`
        )
      );
    }

    const differences = await this.onSchemaDifferences(
      source,
      target,
      await getSchemaDifferences(target, source)
    );
    const unresolvedConflicts = (differences.conflicts ?? []).filter(
      (conflict) => !isCompatibleReferenceVersionConflict(conflict)
    );
    if (unresolvedConflicts.length > 0) {
      throw createSchemaConflictError(
        source,
        "Schema conflict",
        new SchemaConflictsError(
          `Schema '${source.name}' has unresolved conflicts`,
          unresolvedConflicts,
          source.schemaKey,
          target.schemaKey
        )
      );
    }

    return differences.differences.length === 0
      ? { kind: "dynamic-noop", source, target }
      : { kind: "dynamic-merge", source, target, differences };
  }
}

function createSchemaConflictError(
  schema: Schema,
  operation: string,
  cause: Error
): Error {
  return ITwinError.create<SchemaConflictError>({
    message: `${operation} for '${schema.schemaKey.toString(false)}': ${cause.message}`,
    iTwinErrorId: {
      scope: IModelTransformerErrorScope,
      key: IModelTransformerError.SchemaConflict,
    },
    cause,
    schemaKey: schema.schemaKey.toString(false),
  });
}

function throwSchemaConflicts(conflicts: Error[]): void {
  if (conflicts.length === 1) throw conflicts[0];
  if (conflicts.length > 1)
    throw new AggregateError(conflicts, "Schema processing failed");
}

function appendRequiredNoOpSchemas(
  results: SchemaProcessingResult[],
  plans: SchemaPlan[]
): void {
  const noOpSchemas = new Map(
    plans
      .filter((plan) => plan.kind === "dynamic-noop")
      .map((plan) => [plan.source.name.toLowerCase(), plan.source])
  );
  const requiredNoOpSchemas = new Set<string>();
  const schemasToVisit = results.map((result) => result.schema);
  for (const schema of schemasToVisit) {
    for (const reference of schema.references) {
      const schemaName = reference.name.toLowerCase();
      const noOpSchema = noOpSchemas.get(schemaName);
      if (noOpSchema === undefined || requiredNoOpSchemas.has(schemaName))
        continue;

      requiredNoOpSchemas.add(schemaName);
      schemasToVisit.push(noOpSchema);
    }
  }
  for (const schemaName of requiredNoOpSchemas) {
    const schema = noOpSchemas.get(schemaName);
    if (schema !== undefined) results.push({ kind: "source", schema });
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

function collectSchemaCandidates(
  schema: Schema,
  candidates: Map<string, SchemaCandidate>,
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
    throw createSchemaConflictError(
      schema,
      "Schema reference has incompatible read/write versions",
      new Error(
        `${existing.schema.schemaKey.version.read}.${existing.schema.schemaKey.version.write} ` +
          `and ${schema.schemaKey.version.read}.${schema.schemaKey.version.write} differ. ` +
          "Only minor-version reference differences are supported."
      )
    );
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
  candidates: Map<string, SchemaCandidate>
): void {
  for (const reference of schema.references) {
    collectSchemaCandidates(reference, candidates, false);
  }
}
