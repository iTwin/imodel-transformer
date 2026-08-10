/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import { EditTxn, IModelDb } from "@itwin/core-backend";

export async function importElementAspectTestSchema(
  db: IModelDb
): Promise<void> {
  await db.importSchemaStrings([
    `<?xml version="1.0" encoding="UTF-8"?>
<ECSchema schemaName="ExporterAspectTest" alias="eat" version="01.00.00" xmlns="http://www.bentley.com/schemas/Bentley.ECXML.3.1">
  <ECSchemaReference name="BisCore" version="01.00.04" alias="bis"/>
  <ECEntityClass typeName="UniqueAspect" modifier="Sealed">
    <BaseClass>bis:ElementUniqueAspect</BaseClass>
    <ECProperty propertyName="BinaryValue" typeName="binary"/>
  </ECEntityClass>
  <ECEntityClass typeName="MultiAspectA" modifier="Sealed">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
  </ECEntityClass>
  <ECEntityClass typeName="MultiAspectB" modifier="Sealed">
    <BaseClass>bis:ElementMultiAspect</BaseClass>
  </ECEntityClass>
</ECSchema>`,
  ]);
  const editTxn = new EditTxn(db, "import aspect test schema");
  editTxn.start();
  editTxn.saveChanges();
  editTxn.end();
}
