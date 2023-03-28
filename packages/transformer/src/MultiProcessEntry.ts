
import * as assert from "assert";

assert(require.main === module, "expected to be program entry point");
assert(process.send, "expected to be spawned with an ipc channel");

const targetDbPath = process.argv[2];
assert(targetDbPath, "expected a single command line argument, the path to the target")

import { IModelImporter } from "./IModelImporter";
import { Messages, Message, MultiProcessImporterOptions } from "./MultiProcessIModelImporter";
import { IModelDb, IModelHost, StandaloneDb } from "@itwin/core-backend";
import { CodeSpec } from "@itwin/core-common";

import "source-map-support/register";

export class MultiProcessIModelImporterWorker extends IModelImporter {
  public constructor(targetDb: IModelDb, options: MultiProcessImporterOptions) {
    super(targetDb, options);

    const onMsg = (msg: Message, initial = true) => {
      if (initial) console.log("worker received:", JSON.stringify(msg));
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
          return (thisArg as any)[msg.method].call(thisArg, ...msg.args);
        }
        case Messages.SetOption: {
          return this.options[msg.key] = msg.value;
        }
        case Messages.Finalize: {
          return this.targetDb.close();
        }
        case Messages.Await: {
          const { id } = msg;
          const result = onMsg(msg.message, false)
          Promise.resolve(result).then((innerResult) => process.send!({
            type: Messages.Settled,
            result: innerResult,
            id,
          } as Message));
        }
      }
    }

    process.on("message", onMsg);
  }
}

let worker: MultiProcessIModelImporterWorker;

async function main() {
  await IModelHost.startup();

  // FIXME: allow user to provide a module in options to load this themselves
  const targetDb = StandaloneDb.open({ fileName: targetDbPath });

  // TODO: pass options as a base64 encoded JSON blob
  worker = new MultiProcessIModelImporterWorker(await targetDb, {});
}

main().catch(console.error);

