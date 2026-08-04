/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

import type { PairOrder } from "./logRatio.js";
import { SeededRandom } from "./SeededRandom.js";

export type ProcessPolicy =
  | {
      readonly kind: "one-process-per-arm";
      readonly restartBetweenPairs: boolean;
    }
  | {
      readonly kind: "one-process-per-sample";
    };

export type PairPolicy =
  | {
      readonly kind: "paired";
      readonly pairsPerJob: number;
    }
  | {
      readonly kind: "unpaired";
      readonly armAObservationsPerJob: number;
      readonly armBObservationsPerJob: number;
    };

export type OrderPolicy =
  | {
      readonly kind: "alternating";
      readonly first: PairOrder;
    }
  | {
      readonly kind: "fixed";
      readonly order: PairOrder;
    }
  | {
      readonly kind: "seeded-random";
      readonly seed: number;
    };

/**
 * Complete identity of the execution structure that shapes benchmark noise.
 *
 * A/A calibration and A/B comparison may share a pool only when this value is identical. The
 * runner owns the values; the statistics library only validates, keys, and reports them.
 */
export interface ExecutionFingerprint {
  readonly warmupSamplesPerArm: number;
  readonly measuredSamplesPerArm: number;
  readonly processPolicy: ProcessPolicy;
  readonly pairPolicy: PairPolicy;
  readonly orderPolicy: OrderPolicy;
}

function assertNonNegativeInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 0)
    throw new Error(`${label} must be a non-negative integer`);
}

function assertPositiveInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value < 1)
    throw new Error(`${label} must be a positive integer`);
}

export function validateExecutionFingerprint(
  fingerprint: ExecutionFingerprint
): void {
  if (!fingerprint || typeof fingerprint !== "object")
    throw new Error("Execution fingerprint must be an object");
  if (
    !fingerprint.processPolicy ||
    typeof fingerprint.processPolicy !== "object"
  )
    throw new Error("Execution process policy must be an object");
  if (!fingerprint.pairPolicy || typeof fingerprint.pairPolicy !== "object")
    throw new Error("Execution pair policy must be an object");
  if (!fingerprint.orderPolicy || typeof fingerprint.orderPolicy !== "object")
    throw new Error("Execution order policy must be an object");
  assertNonNegativeInteger(
    fingerprint.warmupSamplesPerArm,
    "Warm-up samples per arm"
  );
  assertPositiveInteger(
    fingerprint.measuredSamplesPerArm,
    "Measured samples per arm"
  );
  if (
    fingerprint.processPolicy.kind !== "one-process-per-arm" &&
    fingerprint.processPolicy.kind !== "one-process-per-sample"
  )
    throw new Error("Unknown process policy");
  if (
    fingerprint.processPolicy.kind === "one-process-per-arm" &&
    typeof fingerprint.processPolicy.restartBetweenPairs !== "boolean"
  )
    throw new Error("Restart-between-pairs policy must be boolean");
  if (fingerprint.pairPolicy.kind === "paired")
    assertPositiveInteger(fingerprint.pairPolicy.pairsPerJob, "Pairs per job");
  else {
    assertPositiveInteger(
      fingerprint.pairPolicy.armAObservationsPerJob,
      "Arm A observations per job"
    );
    assertPositiveInteger(
      fingerprint.pairPolicy.armBObservationsPerJob,
      "Arm B observations per job"
    );
  }
  if (fingerprint.orderPolicy.kind === "alternating") {
    if (
      fingerprint.orderPolicy.first !== "AB" &&
      fingerprint.orderPolicy.first !== "BA"
    )
      throw new Error("Alternating order must start with AB or BA");
  } else if (fingerprint.orderPolicy.kind === "fixed") {
    if (
      fingerprint.orderPolicy.order !== "AB" &&
      fingerprint.orderPolicy.order !== "BA"
    )
      throw new Error("Fixed order must be AB or BA");
  } else if (fingerprint.orderPolicy.kind === "seeded-random") {
    assertNonNegativeInteger(fingerprint.orderPolicy.seed, "Order seed");
    if (fingerprint.orderPolicy.seed > 0xffff_ffff)
      throw new Error("Order seed must be an unsigned 32-bit integer");
  } else throw new Error("Unknown order policy");
}

/** Stable, typed serialization used by calibration storage keys. */
export function executionFingerprintKey(
  fingerprint: ExecutionFingerprint
): string {
  validateExecutionFingerprint(fingerprint);
  const processPolicy =
    fingerprint.processPolicy.kind === "one-process-per-arm"
      ? {
          kind: fingerprint.processPolicy.kind,
          restartBetweenPairs: fingerprint.processPolicy.restartBetweenPairs,
        }
      : { kind: fingerprint.processPolicy.kind };
  const pairPolicy =
    fingerprint.pairPolicy.kind === "paired"
      ? {
          kind: fingerprint.pairPolicy.kind,
          pairsPerJob: fingerprint.pairPolicy.pairsPerJob,
        }
      : {
          kind: fingerprint.pairPolicy.kind,
          armAObservationsPerJob: fingerprint.pairPolicy.armAObservationsPerJob,
          armBObservationsPerJob: fingerprint.pairPolicy.armBObservationsPerJob,
        };
  const orderPolicy =
    fingerprint.orderPolicy.kind === "alternating"
      ? {
          kind: fingerprint.orderPolicy.kind,
          first: fingerprint.orderPolicy.first,
        }
      : fingerprint.orderPolicy.kind === "fixed"
        ? {
            kind: fingerprint.orderPolicy.kind,
            order: fingerprint.orderPolicy.order,
          }
        : {
            kind: fingerprint.orderPolicy.kind,
            seed: fingerprint.orderPolicy.seed,
          };
  return JSON.stringify({
    warmupSamplesPerArm: fingerprint.warmupSamplesPerArm,
    measuredSamplesPerArm: fingerprint.measuredSamplesPerArm,
    processPolicy,
    pairPolicy,
    orderPolicy,
  });
}

export function assertExecutionFingerprintMatches(
  actual: ExecutionFingerprint,
  expected: ExecutionFingerprint
): void {
  const actualKey = executionFingerprintKey(actual);
  const expectedKey = executionFingerprintKey(expected);
  if (actualKey !== expectedKey)
    throw new Error(
      `Execution structure does not match calibration: ${actualKey} != ${expectedKey}`
    );
}

export function expectedPairOrders(
  fingerprint: ExecutionFingerprint,
  count: number
): PairOrder[] {
  validateExecutionFingerprint(fingerprint);
  assertNonNegativeInteger(count, "Pair order count");
  const policy = fingerprint.orderPolicy;
  if (policy.kind === "fixed")
    return Array.from({ length: count }, () => policy.order);
  if (policy.kind === "alternating")
    return Array.from({ length: count }, (_, index) =>
      index % 2 === 0 ? policy.first : policy.first === "AB" ? "BA" : "AB"
    );
  const random = new SeededRandom(policy.seed);
  return Array.from({ length: count }, () =>
    random.next() < 0.5 ? "AB" : "BA"
  );
}

export function expectedPairOrder(
  fingerprint: ExecutionFingerprint,
  pair: number
): PairOrder {
  assertNonNegativeInteger(pair, "Pair index");
  return expectedPairOrders(fingerprint, pair + 1)[pair];
}
