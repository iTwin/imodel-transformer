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

  const font_remap = new Map();
  const elem_remap = new Map();

  for (let i = 0; i < 100; ++i) {
    try {
      db.withPreparedSqliteStatement(`
        SELECT RemapGeom(GeometryStream, 'font_remap', 'elem_remap')
        FROM bis_GeometricElement3d
        LIMIT 1
      `, (s) => {
        let r;
        while ((r = s.step()) === DbResult.BE_SQLITE_ROW) {
          console.log(s.getRow());
        }
        assert(r === DbResult.BE_SQLITE_DONE, db.nativeDb.getLastError());
      });
      console.log("DONE!");
      break;
    } catch (err) {
      const [_no, type, _remap, _found, _for, id] = err.message.split(/\s+/g);
      const decimal = parseInt(id, 16);
      if (type === 'element') {
        elem_remap.set(decimal, decimal + 1);
        db.withSqliteStatement(`
          INSERT INTO temp.elem_remap VALUES(${decimal}, ${decimal+1})
        `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE, db.nativeDb.getLastError()));
      } else if (type === 'font') {
        font_remap.set(decimal, decimal + 1);
        db.withSqliteStatement(`
          INSERT INTO temp.font_remap VALUES(${decimal}, ${decimal+1})
        `, (s) => assert(s.step() === DbResult.BE_SQLITE_DONE, db.nativeDb.getLastError()));
      } else {
        assert("Unknown type:", type)
      }
    }
  }

  console.log(font_remap, elem_remap)
  db.close();
}

void main();

