/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import * as path from "node:path";
import { ITwinError } from "@itwin/core-bentley";
import { ECSchemaXmlContext, IModelDb, IModelJsFs } from "@itwin/core-backend";
import { Schema, SchemaKey, SchemaLoader } from "@itwin/ecschema-metadata";
import { ExportSchemaResult, IModelExporter } from "./IModelExporter";
import {
  IModelTransformerError,
  IModelTransformerErrorScope,
} from "./IModelTransformerError";
import {
  ReadonlySchemaAccessor,
  SchemaProcessingResult,
  SchemaProcessingStrategy,
} from "./SchemaProcessingStrategy";

interface SchemaProcessingCoordinatorArgs {
  exporter: IModelExporter;
  targetDb: IModelDb;
  getSchemaExportDirectory: () => string;
  shouldExportSchema: (schemaKey: SchemaKey) => Promise<boolean>;
  serializeSourceSchema: (schema: Schema) => Promise<void | ExportSchemaResult>;
}

/** @internal */
export class SchemaProcessingCoordinator {
  private readonly _longNamedSchemas = new Map<string, string>();

  public constructor(private readonly _args: SchemaProcessingCoordinatorArgs) {}

  public async process(strategy: SchemaProcessingStrategy): Promise<void> {
    let processingFailure: unknown;
    try {
      IModelJsFs.mkdirSync(this._schemaExportDirectory);
      this._longNamedSchemas.clear();

      const sourceSchemas: Schema[] = [];
      for await (const schema of this._args.exporter.enumerateSchemas()) {
        sourceSchemas.push(schema);
      }
      const targetSchemaLoader = new SchemaLoader((name) =>
        this._args.targetDb.getSchemaProps(name)
      );
      const targetSchemas: ReadonlySchemaAccessor = {
        getSchema: async (schemaName) =>
          targetSchemaLoader.tryGetSchema(schemaName),
      };
      const processedSchemas = await strategy.processSchemas({
        sourceSchemas: orderSchemasByDependencies(sourceSchemas),
        targetSchemas,
        shouldExportSchema: this._args.shouldExportSchema,
      });

      for (const result of processedSchemas) {
        await this.serializeSchema(result);
      }
      const exportedSchemaFiles = IModelJsFs.readdirSync(
        this._schemaExportDirectory
      );
      if (exportedSchemaFiles.length > 0) {
        await this._args.targetDb.importSchemas(
          exportedSchemaFiles.map((fileName) =>
            path.join(this._schemaExportDirectory, fileName)
          ),
          {
            ecSchemaXmlContext:
              this._longNamedSchemas.size > 0
                ? this.createLongNameResolvingSchemaContext()
                : undefined,
          }
        );
      }
    } catch (error: unknown) {
      processingFailure = error;
    }

    let cleanupFailure: unknown;
    try {
      IModelJsFs.removeSync(this._schemaExportDirectory);
    } catch (error: unknown) {
      cleanupFailure = error;
    } finally {
      this._longNamedSchemas.clear();
    }

    if (processingFailure !== undefined && cleanupFailure !== undefined) {
      throw new AggregateError(
        [processingFailure, cleanupFailure],
        "Schema processing and cleanup failed",
        { cause: processingFailure }
      );
    }
    // Preserve failures from custom strategies and upstream libraries exactly.
    // eslint-disable-next-line @typescript-eslint/only-throw-error
    if (processingFailure !== undefined) throw processingFailure;
    // eslint-disable-next-line @typescript-eslint/only-throw-error
    if (cleanupFailure !== undefined) throw cleanupFailure;
  }

  public async serializeSourceSchema(
    schema: Schema
  ): Promise<ExportSchemaResult> {
    const schemaFileName = this.getSchemaExportFileName(schema.name);
    this._args.exporter.sourceDb.exportSchema({
      schemaName: schema.name,
      outputDirectory: this._schemaExportDirectory,
      outputFileName: schemaFileName,
    });
    return {
      schemaPath: path.join(this._schemaExportDirectory, schemaFileName),
    };
  }

  private get _schemaExportDirectory(): string {
    return this._args.getSchemaExportDirectory();
  }

  private async serializeSchema(result: SchemaProcessingResult): Promise<void> {
    if (result.kind === "source") {
      await this._args.serializeSourceSchema(result.schema);
      return;
    }

    const schemaLocaters = await import("@itwin/ecschema-locaters");
    const schemaFileName = this.getSchemaExportFileName(result.schema.name);
    const schemaXml = await schemaLocaters.SchemaXml.writeString(result.schema);
    IModelJsFs.writeFileSync(
      path.join(this._schemaExportDirectory, schemaFileName),
      schemaXml
    );
  }

  private getSchemaExportFileName(schemaName: string): string {
    const extension = ".ecschema.xml";
    let schemaFileName = schemaName + extension;
    const systemMaxPathSegmentSize = 255;
    const windowsMaxPathLimit = 260;
    if (
      schemaFileName.length > systemMaxPathSegmentSize ||
      path.join(this._schemaExportDirectory, schemaFileName).length >=
        windowsMaxPathLimit
    ) {
      schemaFileName = `${schemaName.slice(0, 100)}${
        this._longNamedSchemas.size
      }${extension}`;
      this._longNamedSchemas.set(schemaName, schemaFileName);
    }
    return schemaFileName;
  }

  private createLongNameResolvingSchemaContext(): ECSchemaXmlContext {
    const context = new ECSchemaXmlContext();
    context.setSchemaLocater((key) => {
      const schemaFileName = this._longNamedSchemas.get(key.name);
      return schemaFileName === undefined
        ? undefined
        : path.join(this._schemaExportDirectory, schemaFileName);
    });
    return context;
  }
}

interface SchemaDependencyCycleError extends ITwinError {
  schemaNames: readonly string[];
}

function orderSchemasByDependencies(schemas: Schema[]): Schema[] {
  const schemasByName = new Map(
    schemas.map((schema) => [schema.name.toLowerCase(), schema])
  );
  const state = new Map<string, "visiting" | "visited">();
  const orderedSchemas: Schema[] = [];
  const stack: string[] = [];

  const visit = (schema: Schema): void => {
    const schemaName = schema.name.toLowerCase();
    const currentState = state.get(schemaName);
    if (currentState === "visited") return;
    if (currentState === "visiting") {
      const cycleStart = stack.indexOf(schemaName);
      const schemaNames = stack.slice(cycleStart);
      throw ITwinError.create<SchemaDependencyCycleError>({
        message: `Schema dependency cycle detected: ${[
          ...schemaNames,
          schemaName,
        ].join(" -> ")}`,
        iTwinErrorId: {
          scope: IModelTransformerErrorScope,
          key: IModelTransformerError.SchemaDependencyCycle,
        },
        schemaNames,
      });
    }

    state.set(schemaName, "visiting");
    stack.push(schemaName);
    for (const reference of schema.references) {
      const referencedSchema = schemasByName.get(reference.name.toLowerCase());
      if (referencedSchema !== undefined) visit(referencedSchema);
    }
    stack.pop();
    state.set(schemaName, "visited");
    orderedSchemas.push(schema);
  };

  for (const schema of schemas) visit(schema);
  return orderedSchemas;
}
