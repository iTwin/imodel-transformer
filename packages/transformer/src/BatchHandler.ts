import * as assert from "assert";

export interface BatchHandlerProps<T, R> {
  batchSize?: number;
  onBatchReady(ts: T[]): R;
}

/**
 * A utility for aggregating and batching operations
 */
export class BatchHandler<T, R> implements Required<BatchHandlerProps<T, R>> {
  public batchSize = 1000;
  public onBatchReady!: BatchHandlerProps<T, R>["onBatchReady"];

  private _batch: T[] = [];

  public constructor(props: BatchHandlerProps<T, R>) {
    Object.assign(this, props);
    assert(this.batchSize > 0, "batch size must be a positive integer");
  }

  public add(t: T): R | undefined {
    this._batch.push(t);
    if (this._batch.length >= this.batchSize) {
      return this._flush();
    }
    return undefined;
  }

  private _flush(): R {
    const result = this.onBatchReady(this._batch);
    this._batch = [];
    return result;
  }

  public complete(): R {
    return this._flush();
  }
}

