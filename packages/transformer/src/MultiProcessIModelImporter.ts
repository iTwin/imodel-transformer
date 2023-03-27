
import { IModelImporter, IModelImportOptions } from "./IModelImporter";
import * as child_process from "child_process";
import { BriefcaseDb, IModelDb, StandaloneDb } from "@itwin/core-backend";

export interface MultiProcessImporterOptions extends IModelImportOptions {
  // TODO: implement
  /** the path to a module with a default export of an IModelImporter class to load */
  importerClassModulePath?: string;
}

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

/** @internal */
export type ForwardedMethods = (typeof forwardedMethods)[number];

/** @internal */
export enum Messages {
  Init,
  SetOption,
  CallMethod,
  Finalize,

  Await,
  Settled,
}

/** @internal */
export type Message =
  | {
      type: Messages.Init;
      importerInitOptions: MultiProcessImporterOptions;
    }
  | {
      type: Messages.SetOption;
      key: keyof IModelImporter["options"];
      value: any;
    }
  | {
      type: Messages.CallMethod;
      target: string;
      method: string;
      args: any;
    }
  | {
      type: Messages.Finalize;
    }
  | {
      type: Messages.Await;
      id: number;
      message: Message;
    }
  | {
      type: Messages.Settled;
      result: any;
      id: number;
    }
  ;

export class MultiProcessIModelImporter extends IModelImporter {
  private _worker: child_process.ChildProcess;

  private _nextId = 0;
  private _promiseMessage(wrapperMsg: { type: Messages.Await, message: Message }): Promise<void> {
    // TODO: add timeout via race
    return new Promise((resolve) => {
      const id = this._nextId;
      this._nextId++;
      const onMsg = (msg: Message) => {
        if (msg.type === Messages.Settled && msg.id === id) {
          resolve(msg.result);
          this._worker.off("message", onMsg);
        }
      };
      this._worker.on("message", onMsg);
      this._worker.send({ ...wrapperMsg, id } as Message);
    });
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
              return (...args: any[]) => instance._worker.send({
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

    const instance = new MultiProcessIModelImporter(targetDb, options);
    return instance;
  }

  private constructor(targetDb: IModelDb, options: MultiProcessImporterOptions) {
    super(targetDb, options);

    this._worker = child_process.fork(require.resolve("./MultiProcessEntry"),
      [targetDb.pathName],
      {
        stdio: "inherit",
        execArgv: [
          process.env.INSPECT_WORKER && `--inspect-brk=${process.env.INSPECT_WORKER}`,
        ].filter(Boolean) as string[],
        serialization: "advanced", // allow transferring of binary geometry efficiently
      }
    );

    (this as { options: IModelImportOptions }).options = new Proxy(this.options, {
      set: (obj, key, val, recv) => {
        this._worker.send({
          type: Messages.SetOption,
          key: key,
          value: val,
        } as Message)
        console.log("parent set option:", key, val);
        return Reflect.set(obj, key, val, recv);
      }
    });

    for (const key of forwardedMethods) {
      Object.defineProperty(this, key, {
        value: (...args: Parameters<IModelImporter[typeof key]>) => {
          console.log("parent forwarding:", key, args);
          const msg: Message = {
            type: Messages.CallMethod,
            target: "importer",
            method: key,
            args,
          };
          return ["importElement"].includes(key)
            ? this._promiseMessage({
              type: Messages.Await,
              message: msg
            })
            : this._worker.send(msg);
        },
        writable: false,
        enumerable: false,
        configurable: false,
      });
    }
  }
}

