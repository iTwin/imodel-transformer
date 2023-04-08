
import * as child_process from "child_process";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import * as net from "net";
import * as v8 from "v8";

import { IModelImporter, IModelImportOptions } from "./IModelImporter";
import { BriefcaseDb, IModelDb, StandaloneDb } from "@itwin/core-backend";
import { Id64String, IDisposable } from "@itwin/core-bentley";

export interface MultiProcessImporterOptions extends IModelImportOptions {
  // TODO: implement
  /** the path to a module with a default export of an IModelImporter class to load */
  importerClassModulePath?: string;
  hackImportMultiAspectCbScope: {
    targetScopeElementId: Id64String;
    optionsIncludeSourceProvenance: boolean;
  };
}

/** @internal */
export enum Messages {
  Init,
  SetOption,
  CallMethod,

  Await,
  Settled,
}

/** @internal */
export type Message =
  | {
      type: Messages.Init;
      importerInitOptions: MultiProcessImporterOptions;
      msgId?: number;
    }
  | {
      type: Messages.SetOption;
      key: keyof IModelImporter["options"];
      value: any;
      msgId?: number;
    }
  | {
      type: Messages.CallMethod;
      target: string;
      method: string; // TODO: make this typed based on what is handled?
      args: any;
      msgId?: number;
    }
  | {
      type: Messages.Await;
      message: Message;
      msgId?: number;
    }
  | {
      type: Messages.Settled;
      result: any;
      msgId: number;
    }
  ;

// TODO: promise the results for each individual call, atm not necessary
/** wrap a function with backoff upon a condition */
function backoff<F extends (...a: any[]) => any>(
  action: F,
  {
    checkResultForBackoff = (r: ReturnType<F>) => !r,
    dontRetryLastBackoff = false,
    waitMs = 200,
  } = {}
) {
  const callQueue: Parameters<F>[] = [];
  let drainQueueTimeout: NodeJS.Timer | undefined;

  const tryDrainQueue = () => {
    if (process.env.DEBUG?.includes("backoff"))
      console.log(new Date().toISOString(), "draining queue");

    drainQueueTimeout = undefined;

    let callArgs: Parameters<F>;
    while (callArgs = callQueue[callQueue.length - 1]) {
      const result = action(...callArgs);
      const shouldBackoff = checkResultForBackoff(result);
      if (!shouldBackoff || dontRetryLastBackoff)
        callQueue.pop();

      if (shouldBackoff) {
        if (process.env.DEBUG?.includes("backoff"))
          console.log(new Date().toISOString(), 'backing off for:', JSON.stringify(callArgs,(_k,v)=>v instanceof Uint8Array?`<U8Arr[${v.byteLength}]>`:v));
        drainQueueTimeout = setTimeout(() => {
          if (callQueue.length > 0) {
            tryDrainQueue();
          }
        }, waitMs);
        break;
      }
    }

    if (process.env.DEBUG?.includes("backoff"))
      console.log(`drained queue, ${callQueue.length} remaining`);
  };

  const backoffHandler = (...args: Parameters<F>) => {
    callQueue.unshift(args);
    if (!drainQueueTimeout)
      tryDrainQueue();
    else if (process.env.DEBUG?.includes("backoff"))
      console.log(`timeout exists, not draining`);
  };

  return backoffHandler;
}

export class MultiProcessIModelImporter extends IModelImporter implements IDisposable {
  private _nextId = 0;
  private _pendingResolvers = new Map<number, (v: any) => any>();

  private _send = (msg: Message): void => void (async () => {
      msg.msgId = msg.msgId ?? this._nextId++;

      const timeBefore = Date.now();
      if (process.env.DEBUG?.includes("multiproc"))
        console.log(`parent sending (${msg.msgId}):`, JSON.stringify(msg, ((_k,v)=> v instanceof Uint8Array ? `<Uint8Array[${v.byteLength}]>` : v)));

      // this makes it safe to void this promise since it will wait
      if (this._ipcSocket.isPaused()) {
        await new Promise<void>(resolve => this._ipcSocket.on("drain", () => {
          resolve();
        }));
      }

      await new Promise<void>(resolve => {
        const flushed = this._ipcSocket.write(v8.serialize(msg));
        if (flushed) {
          resolve();
        } else {
          if (process.env.DEBUG?.includes("multiproc") && !flushed)
            console.log(`parent error (${msg.msgId})`);
          this._ipcSocket.pause();
          this._ipcSocket.on("drain", () => {
            this._ipcSocket.resume()
            resolve();
          });
        }
      });

      const timeElapsedMs = Date.now() - timeBefore;
      if (timeElapsedMs > 500)
        console.log("Message took more than a second to send!", msg);
    })();

  private _promiseMessage(wrapperMsg: { type: Messages.Await, message: Message }): Promise<any> {
    const msgId = this._nextId++;
    let resolve!: (v: any) => void, reject: (v: any) => void;
    const promise = new Promise<any>((_res, _rej) => { resolve = _res; reject = _rej; });
    this._pendingResolvers.set(msgId, resolve);
    void this._send({ ...wrapperMsg, msgId } as Message);
    return promise;
  }

  public static async create(targetDb: IModelDb, options: MultiProcessImporterOptions): Promise<MultiProcessIModelImporter> {
    if (!targetDb.isReadonly) {
      const targetDbPath = targetDb.pathName;
      const targetDbType = targetDb.constructor as typeof BriefcaseDb | typeof StandaloneDb;
      targetDb.close(); // close it, the spawned process will need the write lock
      const readonlyTargetDb = await targetDbType.open({ fileName: targetDbPath, readonly: true });
      targetDb = readonlyTargetDb;

      // TODO use a library to do this
      for (const { target, key: targetKey, forwardedMethods, promisedMethods, set } of [
        { target: targetDb, key: "targetDb", promisedMethods: ["importSchemas", "saveChanges"], set: (v: any) => (targetDb = v) },
        { target: targetDb.elements, key: "targetDb.elements", forwardedMethods: ["insertAspect", "updateAspect", "updateElement"], set: (v: any) => ((targetDb as any).elements = v) },
        // TODO: does this really need to be a promised method?
        { target: targetDb.codeSpecs, key: "targetDb.codeSpecs", promisedMethods: ["insert"], set: (v: any) => ((targetDb as any)._codeSpecs = v) },
        { target: targetDb.relationships, key: "targetDb.relationships", forwardedMethods: ["insertRelationship"], set: (v: any) => ((targetDb as any)._relationships = v)  },
        { target: targetDb.models, key: "targetDb.relationships", forwardedMethods: ["insertModel", "updateModel"], set: (v: any) => ((targetDb as any).models = v)  },
      ] as const) {
        set(new Proxy(target, {
          get: (obj, key: string, recv) => {
            if ((forwardedMethods as readonly string[])?.includes(key)) {
              return (...args: any[]) => instance._send({
                type: Messages.CallMethod,
                target: targetKey,
                method: key,
                args,
              });
            } else if ((promisedMethods as readonly string[])?.includes(key)) {
              return (...args: any[]) => instance._promiseMessage({
                type: Messages.Await,
                message: {
                  type: Messages.CallMethod,
                  target: targetKey,
                  method: key,
                  args,
                },
              });
            } else
              return Reflect.get(obj, key, recv);
          }
        }));
      }
    }

    const _ipcPath = path.join(process.platform === "win32" ? "\\\\?\\pipe" : os.tmpdir(), `transformer-ipc-${process.pid}`);
    try {
      fs.unlinkSync(_ipcPath);
    } catch {}
    const _ipcServer = net.createServer()
    _ipcServer.listen(_ipcPath);

    const _worker = child_process.fork(require.resolve("./MultiProcessEntry"),
      // TODO: encode options? should be ok if we don't use shell
      [targetDb.pathName, JSON.stringify(options)],
      {
        stdio: ['ignore', 'pipe', 'inherit', 'ipc'],
        execArgv: [
          process.env.INSPECT_WORKER && `--inspect-brk=${process.env.INSPECT_WORKER}`,
        ].filter(Boolean) as string[],
        serialization: "advanced", // allow transferring of binary geometry efficiently
      }
    );

    const workerLogStream = fs.createWriteStream("worker.log");

    _worker.stdout!.pipe(workerLogStream);
    _worker.stdout!.on("close", () => workerLogStream.close());

    const onMsg = (msg: Message) => {
      console.log(`parent received`, msg);
      let resolver: ((v: any) => void) | undefined;
      if (msg.type === Messages.Settled && (resolver = instance._pendingResolvers.get(msg.msgId))) {
        if (process.env.DEBUG?.includes("multiproc"))
          console.log(`parent received settler for ${msg.msgId}`);
        resolver(msg.result);
      }
    };

    _worker.on("error", (err) => console.error(err));

    // FIXME: technically might not have to wait, depends if messages are queued before connect
    const _ipcSocket = await new Promise<net.Socket>((resolve, _reject) => {
      console.log(`parent waiting for connection on ${_ipcPath}`);
      _ipcServer.on("connection", resolve);
    });

    _ipcSocket.on("data", (d) => {
      const msg = v8.deserialize(d);
      onMsg(msg);
    });

    const instance = new MultiProcessIModelImporter(targetDb, options, _worker, _ipcPath, _ipcServer, _ipcSocket);
    return instance;
  }

  private constructor(
    targetDb: IModelDb,
    options: MultiProcessImporterOptions,
    private _worker: child_process.ChildProcess,
    private _ipcPath: string,
    private _ipcServer: net.Server,
    private _ipcSocket: net.Socket,
  ) {
    super(targetDb, options);

    (this as { options: IModelImportOptions }).options = new Proxy(this.options, {
      set: (obj, key, val, recv) => {
        this._send({
          type: Messages.SetOption,
          key: key,
          value: val,
        } as Message);
        if (process.env.DEBUG?.includes("multiproc")) console.log("parent set option:", JSON.stringify({ key, val }));
        return Reflect.set(obj, key, val, recv);
      }
    });

    /** @internal */
    const forwardedMethods = [
      "importModel",
      "importElement",
      "importRelationship",
      "importElementMultiAspects",
      "importElementUniqueAspect",
      "deleteElement",
      "deleteModel",
      "optimizeGeometry",
      "computeProjectExtents",
    ] as const;

    for (const key of forwardedMethods) {
      Object.defineProperty(this, key, {
        value: (...args: Parameters<IModelImporter[typeof key]>) => {
          const msg = {
            type: Messages.CallMethod,
            target: "importer",
            method: key,
            args,
          };
          // TODO: make each message decide whether it needs to be awaited rather than this HACK (also inline them manually?)
          return key === "importElement" || key === "importElementUniqueAspect" || key === "importRelationship"
            ? this._promiseMessage({ type: Messages.Await, message: msg as Message })
            : key === "importElementMultiAspects" // HACK: don't try to serialize the callback (second arg)
            ? this._promiseMessage({ type: Messages.Await, message: { ...msg, args: msg.args.slice(0, 1) } as Message })
            : this._send(msg as Message);
        },
        writable: false,
        enumerable: false,
        configurable: false,
      });
    }
  }

  public override dispose() {
    if (process.platform !== "win32")
      fs.unlinkSync(this._ipcPath);
    this._ipcSocket.end();
    this._ipcServer.close();
    this._worker.disconnect();
  }
}

