
assert(require.main === module, "expected to be program entry point");
assert(process.send, "expected to be spawned with an ipc channel");

const targetDbPath = process.argv[2];
assert(targetDbPath, "expected a single command line argument, the path to the target")

import { IModelImporter } from "./IModelImporter";
import { Messages, Message, MultiProcessImporterOptions } from "./MultiProcessIModelImporter";
import * as assert from "assert";
import { IModelDb, StandaloneDb } from "@itwin/core-backend";

export class MultiProcessIModelImporterWorker extends IModelImporter {
  public constructor(targetDb: IModelDb, options: MultiProcessImporterOptions) {
    super(targetDb, options);

    process.on("message", (msg: Message) => {
      if (msg.type === Messages.CallMethod) {
        // FIXME: why does typescript complain about `this` here
        (this as any)[msg.method].call(this, msg.args);
      } else if (msg.type === Messages.SetOption) {
        this.options[msg.key] = msg.value;
      } else if (msg.type === Messages.Finalize) {
        this.targetDb.close();
      }
    });
  }
}

let worker: MultiProcessIModelImporterWorker;

// FIXME: allow user to provide a module to load this
const targetDb = StandaloneDb.open({ fileName: targetDbPath });

const onInit = async (msg: Message) => {
  if (msg.type === Messages.Init) {
    worker = new MultiProcessIModelImporterWorker(await targetDb, msg.importerInitOptions);
    process.off("message", onInit);
  }
}

process.on("message", onInit);

