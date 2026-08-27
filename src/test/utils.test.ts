import assert from "node:assert/strict";
import test from "node:test";
import { decodeJson, encodeJson, mapConcurrent, optionalCursor, rpcError } from "../utils.js";
import { RociaDbError } from "../types.js";

test("JSON helpers preserve structured values", () => {
  const value = { id: "sku-1", active: true, tags: ["one", "two"] };
  assert.deepEqual(decodeJson(encodeJson(value)), value);
});

test("empty pagination cursors are omitted", () => {
  assert.equal(optionalCursor(""), undefined);
  assert.equal(optionalCursor("next"), "next");
});

test("bounded mapping preserves input order", async () => {
  const result = await mapConcurrent([3, 1, 2], 2, async (value) => value * 2);
  assert.deepEqual(result, [6, 2, 4]);
});

test("mapConcurrent stops scheduling new work as soon as the first rejection is observed", async () => {
  const started: number[] = [];
  const values = Array.from({ length: 20 }, (_, i) => i);
  const unhandled: unknown[] = [];
  const onUnhandledRejection = (reason: unknown): void => {
    unhandled.push(reason);
  };
  process.on("unhandledRejection", onUnhandledRejection);
  try {
    await assert.rejects(
      mapConcurrent(values, 4, async (value, index) => {
        started.push(index);
        if (index === 1) {
          throw new Error("boom");
        }
        // Every non-failing worker parks on a macrotask, so by the time any of them
        // could claim a *new* index, the failure (a microtask away) has already been
        // observed. This makes the "index 4 onward must never start" assertion below
        // deterministic rather than a timing race.
        await new Promise<void>((resolve) => setImmediate(resolve));
        return value;
      }),
      /boom/,
    );
  } finally {
    process.off("unhandledRejection", onUnhandledRejection);
  }
  // Exactly `concurrency` (4) workers claim an index synchronously before the
  // rejection from index 1 is observed; none of them, nor any new worker, claims
  // index 4 or beyond afterward — scheduling stopped immediately, not after a full
  // drain of in-flight work.
  assert.deepEqual(started, [0, 1, 2, 3]);
  assert.deepEqual(unhandled, [], "no worker promise should ever reject unhandled");
});

test("mapConcurrent rejects with the first mapper error, not a later one", async () => {
  await assert.rejects(
    mapConcurrent([0, 1, 2], 1, async (value) => {
      if (value === 1) throw new Error("first");
      if (value === 2) throw new Error("second");
      return value;
    }),
    /first/,
  );
});

test("rpcError survives a null or undefined cause without throwing", () => {
  const fromNull = rpcError("Upload", null);
  assert.ok(fromNull instanceof RociaDbError);
  assert.equal(fromNull.message, "Upload failed");
  assert.equal(fromNull.code, undefined);
  assert.equal(fromNull.reason, undefined);
  assert.equal(fromNull.cause, null);

  const fromUndefined = rpcError("Download", undefined);
  assert.ok(fromUndefined instanceof RociaDbError);
  assert.equal(fromUndefined.message, "Download failed");
  assert.equal(fromUndefined.reason, undefined);
});

test("rpcError survives a cause with no code or metadata (a plain Error)", () => {
  const error = rpcError("PutDoc", new Error("socket hang up"));
  assert.equal(error.message, "PutDoc failed");
  assert.equal(error.code, undefined);
  assert.equal(error.reason, undefined);
});

test("rpcError extracts the server's grpc-metadata 'reason' trailer and the status code", () => {
  const serviceError = {
    code: 16, // grpc.status.UNAUTHENTICATED
    metadata: { get: (key: string) => (key === "reason" ? ["unauthenticated"] : []) },
  };
  const error = rpcError("GetDoc", serviceError);
  assert.equal(error.code, 16);
  assert.equal(error.reason, "unauthenticated");
});
