/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/
import { IModelDb } from "@itwin/core-backend";
import { SchemaLoader } from "@itwin/ecschema-metadata";
import { SchemaXml } from "@itwin/ecschema-locaters";

export class SchemaTestUtils {
  public static async schemaToXmlString(
    schemaName: string,
    iModel: IModelDb
  ): Promise<string> {
    try {
      // Load the schema properties
      const schemaLoader = new SchemaLoader((name: string) =>
        iModel.getSchemaProps(name)
      );
      const schema = schemaLoader.getSchema(schemaName);

      if (!schema) {
        throw new Error(`Schema with name ${schemaName} not found.`);
      }

      // Writes a Schema to an xml string
      const schemaXmlString = await SchemaXml.writeString(schema);
      return schemaXmlString;
    } catch (error) {
      throw error;
    }
  }
}
