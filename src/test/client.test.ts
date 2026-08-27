import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import {
  computeOrValidateChecksum,
  endpointFromHost,
  pageRequest,
  rechunkToUploadSize,
  requireChecksumLength,
  requireStreamChecksum,
  validateFileSize,
} from "../client.js";
import { RociaDbError } from "../types.js";

const UPLOAD_CHUNK_BYTES = 1_048_576; // 1 MiB, mirrors the private constant in client.ts

/** An async source that yields `total` bytes in pieces of at most `readSize`, the way a
 * `createReadStream()` (64 KiB by default) or any other small-chunk source would. */
async function* sourceChunks(total: number, readSize: number): AsyncGenerator<Uint8Array> {
  let remaining = total;
  let fill = 0;
  while (remaining > 0) {
    const n = Math.min(readSize, remaining);
    yield new Uint8Array(n).fill(fill++ % 256);
    remaining -= n;
  }
}

async function collectSizes(total: number, readSize: number): Promise<number[]> {
  const sizes: number[] = [];
  for await (const piece of rechunkToUploadSize(sourceChunks(total, readSize), BigInt(total))) {
    sizes.push(piece.byteLength);
  }
  return sizes;
}

test("rechunkToUploadSize: empty file yields exactly one empty chunk", async () => {
  assert.deepEqual(await collectSizes(0, 65_536), [0]);
});

test("rechunkToUploadSize: a single byte yields one short chunk", async () => {
  assert.deepEqual(await collectSizes(1, 65_536), [1]);
});

test("rechunkToUploadSize: exactly 1 MiB from 64 KiB reads yields one full chunk", async () => {
  // This is the regression scenario: 16 x 64 KiB reads of a 1 MiB file must be
  // re-assembled into exactly one 1 MiB message, not read back and re-split as
  // 64 KiB chunks (which is what silently truncated downloads before the fix).
  assert.deepEqual(await collectSizes(UPLOAD_CHUNK_BYTES, 65_536), [UPLOAD_CHUNK_BYTES]);
});

test("rechunkToUploadSize: 1 MiB + 1 byte from 64 KiB reads yields a full chunk plus a 1-byte remainder", async () => {
  assert.deepEqual(await collectSizes(UPLOAD_CHUNK_BYTES + 1, 65_536), [UPLOAD_CHUNK_BYTES, 1]);
});

test("rechunkToUploadSize: ~2.5 MiB from 64 KiB reads yields two full chunks plus a remainder", async () => {
  const total = 2_500_000; // just under 2.5 MiB
  const sizes = await collectSizes(total, 65_536);
  assert.deepEqual(sizes, [UPLOAD_CHUNK_BYTES, UPLOAD_CHUNK_BYTES, total - 2 * UPLOAD_CHUNK_BYTES]);
});

test("rechunkToUploadSize: no chunk (besides possibly the last) is ever anything but exactly 1 MiB", async () => {
  const total = 5 * UPLOAD_CHUNK_BYTES + 12_345;
  const sizes = await collectSizes(total, 4_096); // an even smaller, more awkward read size
  for (const size of sizes.slice(0, -1)) {
    assert.equal(size, UPLOAD_CHUNK_BYTES);
  }
  assert.ok(sizes.at(-1)! <= UPLOAD_CHUNK_BYTES);
});

test("rechunkToUploadSize: an exact multiple of 1 MiB produces no trailing empty chunk", async () => {
  const total = 3 * UPLOAD_CHUNK_BYTES;
  const sizes = await collectSizes(total, 512 * 1024);
  assert.deepEqual(sizes, [UPLOAD_CHUNK_BYTES, UPLOAD_CHUNK_BYTES, UPLOAD_CHUNK_BYTES]);
});

test("rechunkToUploadSize: the sum of yielded chunk bytes always equals sizeBytes", async () => {
  for (const total of [0, 1, 65_535, UPLOAD_CHUNK_BYTES, UPLOAD_CHUNK_BYTES + 1, 2_500_000, 7_000_001]) {
    const sizes = await collectSizes(total, 65_536);
    const sum = sizes.reduce((a, b) => a + b, 0);
    assert.equal(sum, total, `sum of chunks for a ${total}-byte source should equal ${total}`);
  }
});

test("rechunkToUploadSize: throws before yielding data that would exceed sizeBytes", async () => {
  const drain = async () => {
    for await (const _ of rechunkToUploadSize(sourceChunks(2_000, 500), 1_000n)) {
      // draining is enough to trigger the mid-stream throw
    }
  };
  await assert.rejects(drain, (error: unknown) => {
    assert.ok(error instanceof RociaDbError);
    assert.match(error.message, /received more data than sizeBytes \(1000 bytes\) declared/);
    return true;
  });
});

test("rechunkToUploadSize: throws once the source is exhausted short of the declared size", async () => {
  const drain = async () => {
    for await (const _ of rechunkToUploadSize(sourceChunks(500, 500), 1_000n)) {
      // draining is enough to trigger the end-of-stream throw
    }
  };
  await assert.rejects(drain, (error: unknown) => {
    assert.ok(error instanceof RociaDbError);
    assert.match(error.message, /sent 500 bytes but sizeBytes declared 1000/);
    return true;
  });
});

test("computeOrValidateChecksum: computes a 32-byte SHA-256 digest by default", () => {
  const bytes = new TextEncoder().encode("rocia-db test payload");
  const checksum = computeOrValidateChecksum(bytes);
  assert.equal(checksum.byteLength, 32);
  const expected = createHash("sha256").update(bytes).digest();
  assert.equal(Buffer.compare(checksum, expected), 0);
});

test("computeOrValidateChecksum: computed digest changes with the input", () => {
  const a = computeOrValidateChecksum(new TextEncoder().encode("a"));
  const b = computeOrValidateChecksum(new TextEncoder().encode("b"));
  assert.notEqual(Buffer.compare(a, b), 0);
});

test("computeOrValidateChecksum: passes through a valid 32-byte supplied checksum unchanged", () => {
  const supplied = new Uint8Array(32).fill(9);
  const result = computeOrValidateChecksum(new Uint8Array([1, 2, 3]), supplied);
  assert.equal(Buffer.compare(result, Buffer.from(supplied)), 0);
});

test("computeOrValidateChecksum: rejects a wrong-length supplied checksum, no network involved", () => {
  assert.throws(
    () => computeOrValidateChecksum(new Uint8Array([1]), new Uint8Array(5)),
    (error: unknown) => {
      assert.ok(error instanceof RociaDbError);
      assert.match(error.message, /exactly 32 bytes/);
      return true;
    },
  );
});

test("requireChecksumLength: accepts exactly 32 bytes, rejects anything else", () => {
  assert.doesNotThrow(() => requireChecksumLength(new Uint8Array(32)));
  for (const length of [0, 1, 31, 33, 64]) {
    assert.throws(() => requireChecksumLength(new Uint8Array(length)), RociaDbError);
  }
});

test("requireStreamChecksum: rejects a missing checksum with a clear explanation", () => {
  assert.throws(
    () => requireStreamChecksum(undefined),
    (error: unknown) => {
      assert.ok(error instanceof RociaDbError);
      assert.match(error.message, /precomputed 32-byte SHA-256 checksum/);
      return true;
    },
  );
});

test("requireStreamChecksum: rejects a wrong-length checksum before any network call", () => {
  assert.throws(() => requireStreamChecksum(new Uint8Array(10)), RociaDbError);
});

test("validateFileSize: rejects negative sizes and sizes over the 5 GiB server default", () => {
  assert.doesNotThrow(() => validateFileSize(0n));
  assert.doesNotThrow(() => validateFileSize(5n * 1024n * 1024n * 1024n));
  assert.throws(() => validateFileSize(-1n), RociaDbError);
  assert.throws(() => validateFileSize(5n * 1024n * 1024n * 1024n + 1n), RociaDbError);
});

test("pageRequest: limit 0 is rejected client-side", () => {
  assert.throws(
    () => pageRequest({ limit: 0 }),
    (error: unknown) => {
      assert.ok(error instanceof RociaDbError);
      assert.match(error.message, /positive integer/);
      return true;
    },
  );
});

test("pageRequest: negative and non-integer limits are also rejected", () => {
  assert.throws(() => pageRequest({ limit: -5 }), RociaDbError);
  assert.throws(() => pageRequest({ limit: 1.5 }), RociaDbError);
});

test("pageRequest: no limit defaults to the SDK's page size, and the cursor is passed through", () => {
  assert.deepEqual(pageRequest({}), { limit: 20, cursor: "" });
  assert.deepEqual(pageRequest({ limit: 5, cursor: "abc" }), { limit: 5, cursor: "abc" });
});

test("pageRequest: does not hardcode the server's 200 max-page-size default", () => {
  // The server's own max_page_size limit is configurable and enforced server-side
  // (normalize_limit); the client must only reject limit 0, not second-guess the max.
  assert.deepEqual(pageRequest({ limit: 5_000 }), { limit: 5_000, cursor: "" });
});

test("endpointFromHost: https:// with no port deduces the scheme's default port 443", () => {
  const { address, credentials } = endpointFromHost("https://db.example.com");
  assert.equal(address, "db.example.com:443");
  assert.equal(credentials._isSecure(), true);
});

test("endpointFromHost: https://host:443 (the TLS-behind-a-proxy production path) is accepted", () => {
  const { address, credentials } = endpointFromHost("https://db.example.com:443");
  assert.equal(address, "db.example.com:443");
  assert.equal(credentials._isSecure(), true);
});

test("endpointFromHost: http://host:80 is accepted as plaintext", () => {
  const { address, credentials } = endpointFromHost("http://db.example.com:80");
  assert.equal(address, "db.example.com:80");
  assert.equal(credentials._isSecure(), false);
});

test("endpointFromHost: http://host:50051 (the local dev default) is accepted as plaintext", () => {
  const { address, credentials } = endpointFromHost("http://127.0.0.1:50051");
  assert.equal(address, "127.0.0.1:50051");
  assert.equal(credentials._isSecure(), false);
});

test("endpointFromHost: a URL carrying a path is rejected", () => {
  assert.throws(
    () => endpointFromHost("https://db.example.com/some/path"),
    (error: unknown) => {
      assert.ok(error instanceof RociaDbError);
      assert.match(error.message, /must contain only a hostname and port/);
      return true;
    },
  );
});
