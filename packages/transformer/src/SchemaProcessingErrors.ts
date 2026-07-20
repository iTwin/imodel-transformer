/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
/** @packageDocumentation
 * @module iModels
 */

import { ITwinError } from "@itwin/core-bentley";
import { Schema } from "@itwin/ecschema-metadata";

/**
 * Scope assigned to errors raised while processing schemas.
 * @public
 */
export const schemaProcessingErrorScope = "@itwin/imodel-transformer";

/**
 * Stable identifiers for schema-processing failures.
 * @public
 */
export enum SchemaProcessingErrorKey {
  /** A schema conflict or incompatible schema version prevented processing. */
  SchemaConflict = "schema-conflict",
  /** A schema-processing operation failed without a more specific key. */
  SchemaProcessing = "schema-processing",
  /** The source schema dependency graph contains a cycle. */
  SchemaDependencyCycle = "schema-dependency-cycle",
}

/**
 * An error raised while processing a schema or schema-processing operation.
 * @beta
 */
export interface SchemaProcessingError extends ITwinError {
  /** The source schema key associated with the error, when one exists. */
  readonly schemaKey?: string;
  /** The schema names participating in a dependency-cycle error. */
  readonly schemaNames?: readonly string[];
}

/**
 * Test whether an unknown value is a schema-processing error.
 * @beta
 */
export function isSchemaProcessingError(
  error: unknown,
  key?: SchemaProcessingErrorKey
): error is SchemaProcessingError {
  return ITwinError.isError(error, schemaProcessingErrorScope, key);
}

/** @internal */
interface CreateSchemaProcessingErrorArgs {
  schema?: Schema;
  operation: string;
  cause: unknown;
  key?: SchemaProcessingErrorKey;
  schemaNames?: readonly string[];
}

/** @internal */
export function createSchemaProcessingError(
  args: CreateSchemaProcessingErrorArgs
): SchemaProcessingError {
  const {
    schema,
    operation,
    cause,
    key = SchemaProcessingErrorKey.SchemaProcessing,
    schemaNames,
  } = args;
  if (isSchemaProcessingError(cause)) return cause;

  const schemaKey = schema?.schemaKey.toString(false);
  const context = schemaKey === undefined ? "" : ` for '${schemaKey}'`;
  return ITwinError.create<SchemaProcessingError>({
    message: `${operation}${context}: ${
      cause instanceof Error ? cause.message : String(cause)
    }`,
    iTwinErrorId: {
      scope: schemaProcessingErrorScope,
      key,
    },
    cause,
    schemaKey,
    schemaNames,
  });
}
