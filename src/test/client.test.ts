import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import {
  buildRawUploadRequests,
  buildUploadStreamRequests,
  computeOrValidateChecksum,
  DEFAULT_CONNECT_TIMEOUT_MS,
  endpointFromHost,
  pageRequest,
  parseUint64,
  rechunkToUploadSize,
  requireChecksumLength,
  requireStreamChecksum,
  RociaDbBuilder,
  RociaDbClient,
  validateFileSize,
} from "../client.js";
import type { FileStreamUpload, RawUploadMessage } from "../types.js";
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
    assert.equal(error.kind, "validation");
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
    assert.equal(error.kind, "validation");
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
      assert.equal(error.kind, "validation");
      assert.match(error.message, /exactly 32 bytes/);
      return true;
    },
  );
});

test("requireChecksumLength: accepts exactly 32 bytes, rejects anything else", () => {
  assert.doesNotThrow(() => requireChecksumLength(new Uint8Array(32)));
  for (const length of [0, 1, 31, 33, 64]) {
    assert.throws(
      () => requireChecksumLength(new Uint8Array(length)),
      (error: unknown) => {
        assert.ok(error instanceof RociaDbError);
        assert.equal(error.kind, "validation");
        return true;
      },
    );
  }
});

test("requireStreamChecksum: rejects a missing checksum with a clear explanation", () => {
  assert.throws(
    () => requireStreamChecksum(undefined),
    (error: unknown) => {
      assert.ok(error instanceof RociaDbError);
      assert.equal(error.kind, "validation");
      assert.match(error.message, /precomputed 32-byte SHA-256 checksum/);
      return true;
    },
  );
});

test("requireStreamChecksum: rejects a wrong-length checksum before any network call", () => {
  assert.throws(
    () => requireStreamChecksum(new Uint8Array(10)),
    (error: unknown) => {
      assert.ok(error instanceof RociaDbError);
      assert.equal(error.kind, "validation");
      return true;
    },
  );
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
      assert.equal(error.kind, "connection");
      assert.match(error.message, /must contain only a hostname and port/);
      return true;
    },
  );
});

test("endpointFromHost: an unparseable URL is rejected as kind \"connection\", not thrown raw", () => {
  assert.throws(
    () => endpointFromHost("http://not a valid host!!! ??"),
    (error: unknown) => {
      assert.ok(error instanceof RociaDbError);
      assert.equal(error.kind, "connection");
      assert.match(error.message, /not a valid URL/);
      assert.ok(error.cause instanceof Error);
      return true;
    },
  );
});

test("endpointFromHost: a scheme with no default port (neither http nor https) is rejected", () => {
  assert.throws(
    () => endpointFromHost("ftp://db.example.com"),
    (error: unknown) => {
      assert.ok(error instanceof RociaDbError);
      assert.equal(error.kind, "connection");
      return true;
    },
  );
});

test("DEFAULT_CONNECT_TIMEOUT_MS is 10 000 ms, matching the Rust SDK's Duration::from_secs(10) default", () => {
  // The parity spec requires both SDKs to apply the same unconditional connect
  // deadline when the caller never configures one; this locks the shared value.
  assert.equal(DEFAULT_CONNECT_TIMEOUT_MS, 10_000);
});

test("RociaDbBuilder.connectTimeout(): accepts a positive finite value and stays chainable (success case)", () => {
  const builder = new RociaDbBuilder();
  assert.equal(builder.connectTimeout(5_000), builder);
  // Chainable with the rest of the builder, exactly like every other setter.
  assert.equal(builder.host("http://127.0.0.1:50051"), builder);
});

test("RociaDbBuilder.connectTimeout(): rejects zero, negative, and non-finite values (failure case)", () => {
  for (const bad of [0, -1, NaN, Infinity, -Infinity]) {
    assert.throws(
      () => new RociaDbBuilder().connectTimeout(bad),
      (error: unknown) => {
        assert.ok(error instanceof RociaDbError);
        assert.equal(error.kind, "connection");
        assert.match(error.message, /greater than zero/);
        return true;
      },
      `connectTimeout(${bad}) should have thrown`,
    );
  }
});

test("parseUint64: parses decimal wire strings, preserving the full uint64 range", () => {
  assert.equal(parseUint64("0", "count"), 0n);
  assert.equal(parseUint64("18446744073709551615", "count"), 18446744073709551615n); // u64::MAX
});

test("parseUint64: an unparseable value raises kind \"decode\" and names the field", () => {
  assert.throws(
    () => parseUint64("not-a-number", "collection document count"),
    (error: unknown) => {
      assert.ok(error instanceof RociaDbError);
      assert.equal(error.kind, "decode");
      assert.match(error.message, /Invalid protobuf uint64 value for collection document count/);
      return true;
    },
  );
});

test("validateFileSize / requireChecksumLength / pageRequest / rechunkToUploadSize failures all carry kind \"validation\"", () => {
  assert.throws(() => validateFileSize(-1n), (e: unknown) => (e as RociaDbError).kind === "validation");
  assert.throws(
    () => requireChecksumLength(new Uint8Array(5)),
    (e: unknown) => (e as RociaDbError).kind === "validation",
  );
  assert.throws(
    () => pageRequest({ limit: 0 }),
    (e: unknown) => (e as RociaDbError).kind === "validation",
  );
});

const UPLOAD_CHECKSUM = Buffer.alloc(32, 7);

async function drainUploadRequests(
  generator: AsyncGenerator<{
    tenant_id: string;
    bucket: string;
    file_id: string;
    size_bytes: string;
    content_type: string;
    checksum: Buffer;
    chunk: Buffer;
    request_id: string;
  }>,
) {
  const messages = [];
  for await (const message of generator) messages.push(message);
  return messages;
}

test("buildUploadStreamRequests: only the first message carries file metadata; later messages carry only the chunk", async () => {
  const file: FileStreamUpload = {
    tenantId: "tenant-1",
    bucket: "bucket-1",
    fileId: "file-1",
    sizeBytes: BigInt(2 * UPLOAD_CHUNK_BYTES + 10),
    contentType: "text/plain",
    checksum: UPLOAD_CHECKSUM,
  };
  const chunks = sourceChunks(2 * UPLOAD_CHUNK_BYTES + 10, 65_536);

  const messages = await drainUploadRequests(
    buildUploadStreamRequests(file, UPLOAD_CHECKSUM, "req-42", chunks),
  );

  assert.equal(messages.length, 3, "2 full 1 MiB chunks plus a 10-byte remainder");

  const [first, ...rest] = messages;
  assert.equal(first!.tenant_id, "tenant-1");
  assert.equal(first!.bucket, "bucket-1");
  assert.equal(first!.file_id, "file-1");
  assert.equal(first!.size_bytes, String(2 * UPLOAD_CHUNK_BYTES + 10));
  assert.equal(first!.content_type, "text/plain");
  assert.equal(Buffer.compare(first!.checksum, UPLOAD_CHECKSUM), 0);
  assert.equal(first!.request_id, "req-42");
  assert.equal(first!.chunk.byteLength, UPLOAD_CHUNK_BYTES);

  for (const message of rest) {
    assert.equal(message.tenant_id, "");
    assert.equal(message.bucket, "");
    assert.equal(message.file_id, "");
    assert.equal(message.size_bytes, "0");
    assert.equal(message.content_type, "");
    assert.equal(message.checksum.byteLength, 0);
    assert.equal(message.request_id, "");
  }
  assert.equal(rest[0]!.chunk.byteLength, UPLOAD_CHUNK_BYTES);
  assert.equal(rest[1]!.chunk.byteLength, 10);
});

test("buildUploadStreamRequests: defaults content_type to application/octet-stream when omitted", async () => {
  const file: FileStreamUpload = {
    tenantId: "t",
    bucket: "b",
    fileId: "f",
    sizeBytes: 3n,
    checksum: UPLOAD_CHECKSUM,
  };
  const messages = await drainUploadRequests(
    buildUploadStreamRequests(file, UPLOAD_CHECKSUM, "req", sourceChunks(3, 3)),
  );
  assert.equal(messages.length, 1);
  assert.equal(messages[0]!.content_type, "application/octet-stream");
});

test("buildRawUploadRequests: forwards every message field exactly as given, with no re-chunking and no first-message distinction", async () => {
  const oversizedChunk = new Uint8Array(UPLOAD_CHUNK_BYTES + 1).fill(1); // deliberately over the 1 MiB server limit
  const raw: RawUploadMessage[] = [
    {
      tenantId: "tenant-1",
      bucket: "bucket-1",
      fileId: "file-1",
      sizeBytes: BigInt(oversizedChunk.byteLength + 4),
      contentType: "application/pdf",
      checksum: UPLOAD_CHECKSUM,
      chunk: oversizedChunk,
      requestId: "raw-req-1",
    },
    {
      // A second message that deliberately still carries non-empty metadata: the raw
      // escape hatch must not overwrite or blank it out the way the assisted path does.
      tenantId: "should-be-ignored-by-the-server-but-not-by-us",
      bucket: "also-ignored",
      fileId: "also-ignored",
      sizeBytes: 999n,
      contentType: "also-ignored",
      checksum: new Uint8Array(32).fill(9),
      chunk: new Uint8Array([1, 2, 3, 4]),
      requestId: "raw-req-2",
    },
  ];

  const messages = await drainUploadRequests(buildRawUploadRequests(raw));

  assert.equal(messages.length, 2);
  // No size validation, no 1 MiB chunk-size ceiling, no checksum-length check: the raw
  // escape hatch passes everything through untouched, unlike the assisted paths.
  assert.equal(messages[0]!.chunk.byteLength, oversizedChunk.byteLength);
  assert.equal(messages[0]!.size_bytes, String(oversizedChunk.byteLength + 4));
  assert.equal(messages[0]!.request_id, "raw-req-1");
  assert.equal(Buffer.compare(messages[0]!.checksum, UPLOAD_CHECKSUM), 0);

  assert.equal(messages[1]!.tenant_id, "should-be-ignored-by-the-server-but-not-by-us");
  assert.equal(messages[1]!.bucket, "also-ignored");
  assert.equal(messages[1]!.size_bytes, "999");
  assert.equal(messages[1]!.request_id, "raw-req-2");
  assert.deepEqual([...messages[1]!.chunk], [1, 2, 3, 4]);
});

/**
 * `RociaDbClient`'s constructor is `private` at the TypeScript level — only `connect()`/
 * `RociaDbBuilder.build()` are meant to produce one, and both dial the network. But the
 * constructor body itself does nothing but assign its two fields, so it is safe to call
 * directly (bypassing only the *compile-time* `private` check, not any real guard) to
 * unit test client methods that never touch `#services` — {@link
 * RociaDbClient.invalidateToken} and {@link RociaDbClient.refreshAuthToken} only ever
 * touch `#tokenManager}, so a fake token manager and a never-used `services` placeholder
 * are all that is needed, with no gRPC channel and no network involved.
 */
type TokenManagerLike = { invalidate(): void; refreshNow(): Promise<void> };
type TestableClientCtor = new (services: unknown, tokenManager?: TokenManagerLike) => RociaDbClient;
function clientWithTokenManager(tokenManager?: TokenManagerLike): RociaDbClient {
  return new (RociaDbClient as unknown as TestableClientCtor)({}, tokenManager);
}

test("RociaDbClient.invalidateToken(): delegates to the token manager's invalidate() (success case)", () => {
  let calls = 0;
  const client = clientWithTokenManager({ invalidate: () => { calls += 1; }, refreshNow: async () => {} });
  client.invalidateToken();
  assert.equal(calls, 1);
});

test("RociaDbClient.invalidateToken(): a no-op when auth is disabled (no token manager)", () => {
  const client = clientWithTokenManager(undefined);
  assert.doesNotThrow(() => client.invalidateToken());
});

test("RociaDbClient.refreshAuthToken(): delegates to and awaits the token manager's refreshNow() (success case)", async () => {
  let calls = 0;
  const client = clientWithTokenManager({
    invalidate: () => {},
    refreshNow: async () => {
      calls += 1;
    },
  });
  await client.refreshAuthToken();
  assert.equal(calls, 1);
});

test("RociaDbClient.refreshAuthToken(): a no-op resolved immediately when auth is disabled (no token manager)", async () => {
  const client = clientWithTokenManager(undefined);
  await assert.doesNotReject(client.refreshAuthToken());
});

test("RociaDbClient.refreshAuthToken(): propagates a refresh failure instead of swallowing it (failure case)", async () => {
  const client = clientWithTokenManager({
    invalidate: () => {},
    refreshNow: async () => {
      throw new Error("token endpoint down");
    },
  });
  await assert.rejects(client.refreshAuthToken(), /token endpoint down/);
});
