const { DbResult, DbResponseKind } = require("@itwin/core-common");
const { IModelHost, SnapshotDb } = require("@itwin/core-backend");
const assert = require("assert");

async function main() {
  await IModelHost.startup();
  const db = SnapshotDb.openFile("/home/mike/work/Juergen.Hofer.Bad.Normals.bim");
  console.log(
    db.withSqliteStatement(`
      SELECT GeometryStream
      FROM bis_GeometricElement3d
      LIMIT 1
    `, (s) => [...s])
  );

  db.withSqliteStatement(`
    CREATE TEMP TABLE font_remap(
      SourceId Integer PRIMARY KEY,
      TargetId Integer
    )
  `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE, db.nativeDb.getLastError()));

  db.withSqliteStatement(`
    CREATE TEMP TABLE elem_remap(
      SourceId Integer PRIMARY KEY,
      TargetId Integer
    )
  `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE, db.nativeDb.getLastError()));


  console.log(
    db.withSqliteStatement(`
      SELECT RemapGeom(GeometryStream, 'font_remap', 'elem_remap')
      FROM bis_GeometricElement3d
      LIMIT 1
    `, (s) => {
        let r;
        while ((r = s.step()) === DbResult.BE_SQLITE_ROW) {
          console.log(s.getRow());
        }
        assert(r === DbResult.BE_SQLITE_DONE, db.nativeDb.getLastError());
      })
  );

}

void main();

