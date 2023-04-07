
import * as assert from "assert";

assert(require.main === module, "expected to be program entry point");

const targetDbPath = process.argv[2];
assert(targetDbPath, "expected first command line argument to be the path to the target")

const optionsJson = process.argv[3];
assert(optionsJson, "expected second command line argument to be the options encoded in json")
const options: MultiProcessImporterOptions = JSON.parse(optionsJson);

import * as os from "os";
import * as path from "path";
import * as net from "net";
import * as v8 from "v8";
import { IModelImporter } from "./IModelImporter";
import { Messages, Message, MultiProcessImporterOptions } from "./MultiProcessIModelImporter";
import { ExternalSourceAspect, IModelDb, IModelHost, StandaloneDb } from "@itwin/core-backend";
import { CodeSpec } from "@itwin/core-common";

import "source-map-support/register";

const ipcPath = path.join(process.platform === "win32" ? "\\\\?\\pipe" : os.tmpdir(), `transformer-ipc-${process.ppid}`);

export class MultiProcessIModelImporterWorker extends IModelImporter {
  private _serializer = new v8.Serializer();

  public constructor(
    targetDb: IModelDb,
    options: MultiProcessImporterOptions,
    private _client: net.Socket,
  ) {
    super(targetDb, options);
  }

  public onMsg = (msg: Message) => {
    if (process.env.DEBUG?.includes("multiproc"))
      console.log(`worker received (${msg.msgId}):`, JSON.stringify(msg, (_k, v) => v instanceof Uint8Array ? `<Uint8Array[${v.byteLength}]>` : v));
    this._onMsg(msg);
    if (process.env.DEBUG?.includes("multiproc"))
      console.log(`worker finished (${msg.msgId}):`);
  }


  private _onMsg = (msg: Message) => {
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
        const { msgId } = msg as Message & { msgId: string }; // TODO: fix message types
        const result = this._onMsg(msg.message)
        Promise.resolve(result).then((innerResult) => {
          if (process.env.DEBUG?.includes("multiproc"))
            console.log(`worker sending settler for (${msg.msgId})`, msg);
          this._client.write(v8.serialize({
            type: Messages.Settled,
            result: innerResult,
            msgId,
          }));
        });
      }
    }
  }

}

let worker: MultiProcessIModelImporterWorker;

async function main() {
  const client = net.createConnection(ipcPath);
  const [targetDb] = await Promise.all([
    // FIXME: allow user to provide a module in options to load this themselves
    IModelHost.startup().then(() => StandaloneDb.open({ fileName: targetDbPath })),
    new Promise<void>((resolve, reject) => client.on("connect", resolve).on("error", reject)),
  ]);

  worker = new MultiProcessIModelImporterWorker(targetDb, options, client);

  //client.allowHalfOpen = true;
  client.on("data", d => worker.onMsg(v8.deserialize(d)));
}

main().catch(console.error);

