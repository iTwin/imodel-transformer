
import * as assert from "assert";

assert(require.main === module, "expected to be program entry point");

const targetDbPath = process.argv[2];
assert(targetDbPath, "expected first command line argument to be the path to the target")

const optionsJson = process.argv[3];
assert(optionsJson, "expected second command line argument to be the options encoded in json")
const options: MultiProcessImporterOptions = JSON.parse(optionsJson);

import * as os from "os";
import * as path from "path";
import * as v8 from "v8";
import { IModelImporter } from "./IModelImporter";
import { Messages, Message, MultiProcessImporterOptions } from "./MultiProcessIModelImporter";
import { ExternalSourceAspect, IModelDb, IModelHost, StandaloneDb } from "@itwin/core-backend";
import { CodeSpec } from "@itwin/core-common";

import "source-map-support/register";

const ipcPath = path.join(process.platform === "win32" ? "\\\\?\\pipe" : os.tmpdir(), `transformer-ipc-${process.ppid}`);

// HACK: can't log to stdout since master expects only ipc messages there
console.log = console.error;

export class MultiProcessIModelImporterWorker extends IModelImporter {
  private _serializer = new v8.Serializer();

  public onMsg = (msg: Message) => {
    switch (msg.type) {
      case Messages.CallMethod: {
        const thisArg
          = msg.target === "importer" ? this
          : msg.target === "targetDb" ? this.targetDb
          : msg.target === "targetDb.elements" ? this.targetDb.elements
          : msg.target === "targetDb.relationships" ? this.targetDb.relationships
          : msg.target === "targetDb.codeSpecs" ? this.targetDb.codeSpecs
          : msg.target === "targetDb.models" ? this.targetDb.models
          : assert(false, "unknown target") as never;
        // FIXME
        if (msg.target === "targetDb.codeSpecs" && msg.method === "insert") {
          const [codeSpec] = msg.args;
          return (thisArg as any)[msg.method].call(thisArg, CodeSpec.create(this.targetDb, codeSpec.name, codeSpec.scopeType, codeSpec.scopeReq));
        }
        if (msg.target === "importer" && msg.method === "importElementMultiAspects") {
          const cb: Parameters<this["importElementMultiAspects"]>[1] = (a) => {
            const isExternalSourceAspectFromTransformer = a instanceof ExternalSourceAspect
              && a.scope?.id === (this.options as MultiProcessImporterOptions).hackImportMultiAspectCbScope.targetScopeElementId;
            return !(this.options as MultiProcessImporterOptions).hackImportMultiAspectCbScope.optionsIncludeSourceProvenance || !isExternalSourceAspectFromTransformer;
          };
          return (thisArg as any)[msg.method].call(thisArg, msg.args[0], cb);
        }
        return (thisArg as any)[msg.method].call(thisArg, ...msg.args);
      }
      case Messages.SetOption: {
        return this.options[msg.key] = msg.value;
      }
      case Messages.Await: {
        const result = this.onMsg(msg.message)
        Promise.resolve(result).then((innerResult) => {
          if (process.env.DEBUG?.includes("multiproc"))
            console.log(`worker sending settler for (${msg.msgId})`);

          const serialized = v8.serialize({
            type: Messages.Settled,
            result: innerResult,
            msgId: msg.msgId,
          });
          const serializedLenBuf = Buffer.from([0, 0, 0, 0]);
          serializedLenBuf.writeUint32LE(serialized.byteLength);
          // FIXME: check result for this small write
          process.stdout.write(serializedLenBuf);
          const success = process.stdout.write(serialized);

          if (process.env.DEBUG?.includes("multiproc") && !success)
            console.log(`worker send error`);
        });
      }
    }
  }

}

let worker: MultiProcessIModelImporterWorker;

async function main() {
  const targetDb = await IModelHost.startup().then(() => StandaloneDb.open({ fileName: targetDbPath })),

  worker = new MultiProcessIModelImporterWorker(targetDb, options);

  let lastLen: number | undefined;

  process.stdin.on("readable", () => {
    while (true) {
      let len: number;
      if (lastLen) {
        len = lastLen;
        lastLen = undefined;
      } else {
        const lenBuf = process.stdin.read(4) as Buffer | null;
        if (lenBuf === null) return;
        len = lenBuf.readUint32LE();
      }

      const chunk = process.stdin.read(len) as Buffer | null;
      if (chunk === null) {
        lastLen = len;
        return;
      }
      assert(chunk.byteLength === len, `bad read size! (ended=${process.stdin.readableEnded}), ${chunk.byteLength}, ${len}`);
      const msg = v8.deserialize(chunk);
      if (process.env.DEBUG?.includes("multiproc"))
        console.log(`worker received (${msg.msgId}):`, JSON.stringify(msg, (_k, v) => v instanceof Uint8Array ? `<Uint8Array[${v.byteLength}]>` : v));
      worker.onMsg(msg);
      if (process.env.DEBUG?.includes("multiproc"))
        console.log(`worker finished (${msg.msgId}):`);
    }
  });
}

main().catch(console.error);

