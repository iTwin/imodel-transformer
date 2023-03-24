
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
      target: "importer" | "targetDb" | "targetDb.elements",
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
        console.log("parent received settler:", JSON.stringify(msg));
        if (msg.type === Messages.Settled && msg.id === id)
          resolve();
      };
      this._worker.on("message", onMsg);
      this._worker.send({ ...wrapperMsg, id } as Message);
      console.log("parent send await", id, wrapperMsg);
    });
  }


  public static async create(targetDb: IModelDb, options: MultiProcessImporterOptions): Promise<MultiProcessIModelImporter> {
    if (!targetDb.isReadonly) {
      const targetDbPath = targetDb.pathName;
      const targetDbType = targetDb.constructor as typeof BriefcaseDb | typeof StandaloneDb;
      targetDb.close(); // close it, the spawned process will need the write lock
      const readonlyTargetDb = await targetDbType.open({ fileName: targetDbPath, readonly: true });
      targetDb = readonlyTargetDb;

      const targetDbElementsForwarded = [
        "insertAspect",
      ] as const;

      (targetDb.elements as IModelDb.Elements) = new Proxy(targetDb.elements, {
        get: (obj, key: (typeof targetDbElementsForwarded)[number], recv) =>
          targetDbElementsForwarded.includes(key)
            ? (...args: any[]) => instance._worker.send({
              type: Messages.CallMethod,
              target: "targetDb.elements",
              method: key,
              args,
            })
            : Reflect.get(obj, key, recv),
      });

      // TODO: use a library to do this
      const targetDbForwarded = [
        "importSchemas",
      ] as const;

      targetDb = new Proxy(readonlyTargetDb, {
        get: (obj, key: (typeof targetDbForwarded)[number], recv) =>
          targetDbForwarded.includes(key)
            ? (...args: any[]) => instance._promiseMessage({
              type: Messages.Await,
              message: {
                type: Messages.CallMethod,
                target: "targetDb",
                method: key,
                args,
              }
            })
            : Reflect.get(obj, key, recv),
      });
    }

    const instance = new MultiProcessIModelImporter(targetDb, options);
    return instance;
  }

  private constructor(targetDb: IModelDb, options: MultiProcessImporterOptions) {
    super(targetDb, options);

    this._worker = child_process.fork(require.resolve("./MultiProcessEntry"),
      [targetDb.pathName],
      {
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
        value: (...args: Parameters<IModelImporter[typeof key]>): void => {
          console.log("parent forwarding:", key, args);
          this._worker.send({
            type: Messages.CallMethod,
            target: "importer",
            method: key,
            args,
          } as Message);
        },
        writable: false,
        enumerable: false,
        configurable: false,
      });
    }
  }
}

