# RociaDB TypeScript SDK

Typed, Promise-based Node.js client for the RociaDB document, graph, file, and
tenant gRPC services. It covers all 22 RPCs from the packaged protobuf and adds
pagination helpers, JSON decoding, bounded batch operations, OAuth2 token
management, and ergonomic file streaming.

## What the SDK Manages

RociaDB organizes data around four identifiers:

- A `tenantId` segments data by customer or workspace. Every RPC except
  `listTenants` requires one.
- A document `collection` groups JSON documents addressed by `documentId`.
- A `graph` groups nodes and directed edges. Node IDs conventionally use
  `label:id`, for example `product:sku-123`.
- A file `bucket` groups binary objects addressed by `fileId`.

Document, node, and edge payloads are serialized as JSON. Generic return types
such as `getDocument<Product>()` provide compile-time typing; the SDK does not
perform runtime schema validation.

**`tenantId` is not a security boundary.** It is not derived from the
caller's identity, so any authenticated client can address any `tenantId` —
it exists to segment data for application-level purposes, not to isolate
customers from each other at the protocol level. Per-user or per-customer
authorization is the calling application's responsibility; see
[Authentication](#authentication) for what the server does check.

## Requirements and Installation

- Node.js 20 or newer
- A reachable RociaDB gRPC endpoint
- OAuth2 client credentials unless authentication is explicitly disabled

```bash
npm install rocia-db-sdk
```

The SDK uses `@grpc/grpc-js` and is designed for Node.js services, workers, and
CLI applications. It does not run directly in a browser.

## Connecting

Use the builder when you want fluent configuration:

```ts
import { RociaDbBuilder } from "rocia-db-sdk";

const client = await new RociaDbBuilder()
  .host("https://db.example.com:443")
  .authClientCredentials(
    "https://auth.example.com/oauth/token",
    "client-id",
    "client-secret",
  )
  .connectTimeout(15_000)
  .build();
```

`http://` creates an insecure gRPC channel and `https://` enables TLS.
RociaDB servers do not terminate TLS themselves — they listen in clear text —
so `https://` is only meaningful when it points at a reverse proxy sitting in
front of the server; that proxy is the normal production endpoint, typically
on port 443 (a bare `https://host` with no explicit port defaults to it, the
same way `http://host` defaults to port 80). Point `http://host:50051` (the
default gRPC port) directly at a RociaDB process for a local or unproxied
connection instead.

When `authClientCredentials` is omitted, the builder reads:

```text
AUTH_TOKEN_URL
AUTH_CLIENT_ID
AUTH_CLIENT_SECRET
```

For a controlled local environment, authentication can be disabled explicitly:

```ts
const client = await new RociaDbBuilder()
  .host("http://127.0.0.1:50051")
  .disableAuth()
  .build();
```

You may also call `RociaDbClient.connect(options)` directly. Create one client
per upstream configuration, reuse it across requests, and call `client.close()`
during graceful shutdown.

`.connectTimeout(timeoutMs)` sets the deadline applied while every service
connects; call it on the builder before `.build()`, or pass
`connectTimeoutMs` directly to `RociaDbClient.connect(options)`. When
neither is supplied, the client falls back to its own default —
**10,000 ms (10 seconds)**, pinned to match the Rust SDK's own
`Duration::from_secs(10)` default so a connect-timeout choice behaves
identically from either SDK. `build()`/`connect()` always applies some
timeout, so a slow or unreachable DNS/TCP target fails after a bounded wait
instead of hanging the returned promise forever. Passing zero, a negative
number, or a non-finite value to `.connectTimeout()` throws a
`RociaDbError` (`kind: "connection"`) immediately, before any connection
attempt.

`.host(...)` must resolve to a bare hostname and port — no path component
beyond an absent one or a lone `/`. A mistyped host with a leftover path
(`http://127.0.0.1:50051/v1`, pasted from somewhere else) is rejected with
`RociaDbError` (`kind: "connection"`) before any connection attempt, rather
than silently dialing the host and dropping the path.

## Authentication

Every call carries a bearer token as gRPC metadata:

```text
authorization: Bearer <jwt>
```

The SDK obtains it from `tokenUrl` with a standard OAuth2 client-credentials
request (`POST`, `application/x-www-form-urlencoded`,
`grant_type=client_credentials&client_id=...&client_secret=...`) and expects
a response shaped like:

```json
{ "access_token": "...", "token_type": "Bearer", "expires_in": 600 }
```

**Tokens issued by RociaDB's identity provider are valid for 600 seconds (10
minutes), fixed server-side.** The SDK does not hardcode that number — it
reads `expires_in` from each response — but in practice it is always 600.
`TokenManager` refreshes automatically and transparently, by default 30
seconds before the cached token's expiry (`refreshSkewMs`, the fourth
argument to `authClientCredentials`); as long as the client keeps making
calls, you never need to schedule or poll for a refresh yourself.

### `UNAUTHENTICATED` vs `PERMISSION_DENIED`

These two statuses look similar but call for opposite handling:

- **`UNAUTHENTICATED`** — the token is missing, expired, malformed, or signed
  by a different issuer. Treat this as a renewal signal: call
  `client.invalidateToken()` to drop the cached token, then retry; the next
  call fetches a fresh one.
- **`PERMISSION_DENIED`** — the token is valid but its scope does not cover
  the operation. Retrying does not help, even after `invalidateToken()`,
  because a fresh token carries the same scope. Two causes exist:
  - a **read-only** scoped client called one of the 7 write RPCs:
    `putDocument`/`createDocument`, `deleteDocument`, `putNode`/`putNodes`,
    `addEdge`/`addEdges`, `deleteEdge`,
    `uploadFile`/`uploadFileStream`/`uploadFileRaw`, or `deleteFile`. A
    read-only token is not otherwise crippled — all 15 read RPCs remain
    available.
  - an **admin**-scoped token was presented — the credentials used to manage
    `rocia-idp` service accounts, not to read or write data. It is rejected
    on all 22 RPCs, reads included. If a read you expect to work returns
    `PERMISSION_DENIED`, check which `client_id` produced the token: the
    data-plane account and the administration account are different
    credentials.

```ts
import { status } from "@grpc/grpc-js";
import { RociaDbError } from "rocia-db-sdk";

try {
  await client.getDocument("tenant-1", "products", "sku-123");
} catch (error) {
  if (error instanceof RociaDbError && error.code === status.UNAUTHENTICATED) {
    client.invalidateToken();
    await client.getDocument("tenant-1", "products", "sku-123"); // retry once
  } else {
    throw error; // includes PERMISSION_DENIED: retrying will not help
  }
}
```

### Two ways to recover from `UNAUTHENTICATED`

`invalidateToken()` (shown above) is **lazy**: it is synchronous, returns
immediately, and does not itself make a network call — it only drops the
cached token, so the very next internal `metadata()` call, made just before
the next RPC (whichever call that turns out to be), sees no valid token and
fetches a fresh one as part of dispatching that call. `refreshAuthToken()` is
its **eager** counterpart: it `await`s the round trip to the identity
provider itself and only resolves once a fresh token is confirmed and
cached, or rejects with the fetch failure — the right choice immediately
before retrying the call that just failed, so the retry's own `metadata()`
call finds an already-fresh token instead of paying the refresh latency
inline:

```ts
try {
  await client.getDocument("tenant-1", "products", "sku-123");
} catch (error) {
  if (error instanceof RociaDbError && error.code === status.UNAUTHENTICATED) {
    await client.refreshAuthToken(); // await the round trip once, here...
    await client.getDocument("tenant-1", "products", "sku-123"); // ...then retry with it already cached
  } else {
    throw error;
  }
}
```

Reach for `invalidateToken()` instead when you just want to mark the cached
token stale without blocking on a fresh one right now — a fire-and-forget
error handler that is not about to retry immediately, for example. Both are
no-ops when the client was built with `disableAuth()`. Neither ever discards
a still-valid cached token just because an opportunistic refresh attempt
failed: the internal `metadata()` call made before every RPC keeps injecting
the last known-good token until a replacement is confirmed — a refresh
hiccup inside the `refreshSkewMs` margin logs a warning and reuses the
cached token rather than failing the in-flight call outright.

### Auth Helpers

`TokenManager` and `fetchOAuthToken` are exported from the package root for
callers who need OAuth2 token handling outside of a `RociaDbClient` — for
example, to reuse the same access token against a different service:

```ts
import { TokenManager, fetchOAuthToken } from "rocia-db-sdk";

// One-off token exchange, no caching or refresh:
const token = await fetchOAuthToken({
  tokenUrl: "https://auth.example.com/oauth/token",
  clientId: "client-id",
  clientSecret: "client-secret",
});
console.log(token.accessToken, token.tokenType, token.expiresIn);

// Cached, self-refreshing token manager — the same one RociaDbClient uses internally:
const tokenManager = new TokenManager({
  tokenUrl: "https://auth.example.com/oauth/token",
  clientId: "client-id",
  clientSecret: "client-secret",
});
await tokenManager.initialize(); // fetch the first token eagerly, fail fast if it's wrong
const metadata = await tokenManager.metadata(); // grpc.Metadata with "authorization" set
```

`fetchOAuthToken` performs one token exchange and returns; it does not cache
or refresh anything. `new TokenManager(config)` is synchronous — call
`initialize()` (or let the first `metadata()` call do it lazily) before
using it. Unlike the Rust SDK's `TokenManager`, there is no separate
background refresh task to spawn or hold a guard for: `metadata()` itself
checks the cached token's age against `refreshSkewMs` (30 seconds by
default) every time it is called and refreshes inline when needed — the same
check `RociaDbClient` relies on internally before every RPC.

For a controlled local environment only, skip authentication entirely with
`disableAuth()` (see [Connecting](#connecting)).

## Pagination

Every listing method — documents, collections, graphs, nodes, neighbors,
buckets, files, and tenants — takes `{ limit?, cursor? }` (`PageOptions`) and
returns `{ items, nextCursor? }` (`Page<T>`, or `DocumentPage<T>` with an
added `totalCount`).

- **`limit` must be a positive integer.** The SDK rejects `0` and
  non-integers client-side, before any RPC, with `RociaDbError`. Omit it to
  use the SDK's own default of 20.
- **The server enforces its own ceiling**, `limits.max_page_size` (200 by
  default, operator-configurable per deployment). A `limit` above that
  ceiling is rejected server-side with `INVALID_ARGUMENT` — the SDK does
  **not** hardcode 200, or any other ceiling, client-side.
- **An empty `nextCursor` is the only end-of-list signal.** Loop while it is
  present:

  ```ts
  let cursor: string | undefined;
  do {
    const page = await client.listDocuments("tenant-1", "products", { limit: 100, cursor });
    for (const item of page.items) console.log(item);
    cursor = page.nextCursor;
  } while (cursor);
  ```

- **Do not stop because a page is short, or even empty.** `findDocumentsByField`,
  `listDocuments`, and `queryDocuments` in particular can return fewer items
  than `limit` — or none at all — in the middle of a paginated walk, when an
  index entry briefly survives the document it points to being deleted. A
  short or empty page with a non-empty `nextCursor` is not the end; only the
  cursor tells you that.
- **An exact-multiple total produces one extra, empty page.** If a
  collection holds exactly `limit` items, the final full page still carries
  a cursor — the server has no way to know it just emitted everything — so
  the next call returns an empty page with no cursor. This is expected, not
  a bug; the loop above handles it correctly by construction.
- Cursors are opaque: never construct, parse, or persist one across
  sessions. Their shape differs per RPC and may change.

`listGraphs`, `listBuckets`, and `listNodes` paginate over graph/bucket
names, not the items each one contains — a graph or bucket name appears once
no matter how many nodes or files it holds.

## Documents

### Create, read, and delete

```ts
interface Product {
  sku: string;
  label: string;
  active: boolean;
  price: number;
}

await client.putDocument("tenant-1", "products", "sku-123", {
  sku: "sku-123",
  label: "Widget",
  active: true,
  price: 19.9,
} satisfies Product);

const product = await client.getDocument<Product>(
  "tenant-1",
  "products",
  "sku-123",
);

await client.deleteDocument("tenant-1", "products", "sku-123");
```

`putDocument` replaces the document completely — there is no partial merge —
and recomputes its indexes (exact-match and trigram) in the same
transaction. Its JSON payload is capped at the server's `limits.max_doc_bytes`
(2 MiB by default); a larger payload is rejected with `INVALID_ARGUMENT`
before anything is written. `deleteDocument` is idempotent: deleting a
document that does not exist is not an error (contrast this with
`deleteEdge` in [Directed edges and neighbors](#directed-edges-and-neighbors),
which is not).

### Create a document and its graph reference

`createDocument` first stores the document, then optionally creates a graph node
whose payload points back to `{ collection, id }`:

```ts
await client.createDocument(
  "tenant-1",
  "products",
  "sku-123",
  product,
  {
    nodeLabel: "product",
    nodeGraph: "catalog",
  },
);
// Creates document products/sku-123 and node product:sku-123 in catalog.
```

`nodeLabel` and `nodeGraph` must be supplied together. This is a composite,
non-transactional operation: if node creation fails, the document remains
stored and the caller should retry or compensate.

### Search, query, and pagination

Use `findDocumentsByField` for one exact field lookup, `listDocuments` for an
unfiltered collection, and `queryDocuments` for multiple filters and sorting:

```ts
let cursor: string | undefined;

do {
  const page = await client.queryDocuments<Product>("tenant-1", "products", {
    filters: [
      { field: "active", operator: "eq", values: [true] },
      { field: "label", operator: "contains", values: ["Widget"] },
    ],
    sort: [{ field: "price", direction: "asc" }],
    limit: 50,
    cursor,
  });

  for (const item of page.items) console.log(item.sku);
  console.log("matching documents:", page.totalCount);
  cursor = page.nextCursor;
} while (cursor);
```

Supported query operators are `eq`, `in`, and `contains`. Filters are sent in
order and combined by the server with an implicit AND — there is no OR.
`contains` is a case-insensitive substring match (`"ALI"` matches `"Alice"`)
backed by a trigram index, with two restrictions: a `contains` term shorter
than 3 characters is not indexable, and a query where *no* filter is
indexable is rejected with `INVALID_ARGUMENT` rather than served by a full
scan — pair a short `contains` term with an `eq` or `in` filter on another
field. Cursors are opaque: pass `nextCursor` back unchanged. `totalCount` is
a `bigint` so protobuf `uint64` values never lose precision.

`findDocumentsByField`'s `value` must encode to a JSON **scalar** — a
string, number, boolean, or `null`. An object or array throws
`INVALID_ARGUMENT` server-side; it uses the same exact-match index as
`queryDocuments`'s `eq` operator.

`totalCount` is not uniformly cheap. `listDocuments`'s count is read from a
counter maintained on every write, so it costs nothing extra. `queryDocuments`'s
count is computed by evaluating the full filtered result set, so it scales
with the number of matching documents — prefer `listDocuments` (no filters)
when you just need a count, and avoid calling `queryDocuments` in a loop
purely to read `totalCount`.

### Discovering collections

`listCollections` returns the collections that hold at least one document,
each with its document count:

```ts
const collections = await client.listCollections("tenant-1", { limit: 50 });
for (const info of collections.items) {
  console.log(info.collection, info.count); // count is a bigint
}
```

## Graph

### Nodes and batches

```ts
await client.putNode("tenant-1", "catalog", "product:sku-123", product);
const node = await client.getNode<Product>(
  "tenant-1",
  "catalog",
  "product:sku-123",
);

await client.putNodes("tenant-1", "catalog", [
  { nodeId: "product:sku-124", value: { ...product, sku: "sku-124" } },
  { nodeId: "group:featured", value: { title: "Featured" } },
]);
```

`putNode`'s payload must encode to a JSON **object** — not a scalar or an
array — and, like `putDocument`, is capped at the server's
`limits.max_doc_bytes` (2 MiB by default).

Batch helpers issue at most 10 requests concurrently. They are not atomic: if
one item fails, earlier items may already have been stored.

### Directed edges and neighbors

An edge goes from `from` to `to`. For `product:sku-123 -> group:featured`, the
group is an outgoing neighbor of the product, while the product is an incoming
neighbor of the group.

```ts
await client.addEdge("tenant-1", "catalog", {
  edgeId: "membership-1",
  from: "product:sku-123",
  to: "group:featured",
  label: "belongs_to",
  value: { weight: 1 },
});

const outgoing = await client.neighborsOut(
  "tenant-1",
  "catalog",
  "product:sku-123",
  "belongs_to",
  { limit: 25 },
);

const products = await client.getIncomingNeighborNodes<Product>(
  "tenant-1",
  "catalog",
  "group:featured",
  "belongs_to",
);

await client.deleteEdge("tenant-1", "catalog", "membership-1");
```

`addEdge` fails with `NOT_FOUND` if either `from` or `to` does not already
exist as a node — create both endpoint nodes before the edge — and, like
`putDocument` and `putNode`, its JSON payload is capped at the server's
`limits.max_doc_bytes` (2 MiB by default). `deleteEdge` also fails with
`NOT_FOUND` if the edge itself does not exist; unlike `deleteDocument`,
deleting an edge is not idempotent.

`neighborsOut` and `neighborsIn` return one raw page containing `nodeId` and
`edgeId`. The `getOutgoingNeighborNodes<T>` and
`getIncomingNeighborNodes<T>` helpers follow every page and load each node's
JSON payload with bounded concurrency.

### Discovering graphs and nodes

```ts
const graphs = await client.listGraphs("tenant-1");
const nodes = await client.listNodes("tenant-1", "catalog", { limit: 100 });
```

Both return a `Page<string>` of names and node IDs. Use `getNode<T>` to load a
payload once the ID is known.

## Files

Three levels of upload help exist, from most to least hand-holding:
`uploadFile` (buffers the whole file in memory, computes the checksum for
you), `uploadFileStream` (streams arbitrarily-sized chunks without buffering
the whole file — you supply the checksum, but it still re-chunks and
validates everything else for you), and `uploadFileRaw` (a raw pass-through —
you build every protobuf message yourself, with zero validation). The wire
contract all three implement is worth understanding even if you only ever
call `uploadFile`.

### The upload wire contract

`Upload` is a client-streaming RPC:

- **The first message carries the file's metadata** — `tenantId`, `bucket`,
  `fileId`, `sizeBytes` (the exact total byte count), `contentType`,
  `checksum`, and `requestId`. Every later message is only read for its
  `chunk` field; metadata fields on those later messages are ignored.
- **Chunk size is the client's choice, capped at 1 MiB — not a fixed
  requirement.** The server stores each chunk verbatim at its position in
  the stream and, on download, reads chunks back until it has collected
  `sizeBytes` bytes in total — it does not assume any particular chunk size
  when replaying them. A single message's `chunk` larger than 1 MiB is
  rejected outright with `INVALID_ARGUMENT`; anything at or under that cap
  is fine, sliced however the client likes. `uploadFile` and
  `uploadFileStream` (below) both still always emit exactly-1-MiB messages
  (the last one may be shorter) — not because the server requires it, but
  because 1 MiB is the largest message the server allows, so it is also the
  fewest possible messages for a given file; this is also why neither
  `FileUploadOptions` nor `FileStreamUpload` has a `chunkSize` option. See
  [Migrating to 0.3.0](#migrating-to-030) for why a caller-chosen chunk size
  used to be dangerous, and [Migrating to 0.6.0](#migrating-to-060) for why
  it no longer is against a current server.
- **`checksum` must be exactly 32 raw bytes — a SHA-256 digest.** Any other
  length, including empty, is rejected with `INVALID_ARGUMENT` before a
  single chunk is read. The server does not verify that the checksum
  actually matches the uploaded bytes, only that its length is correct.
- **The sum of every `chunk`'s bytes across the stream must equal
  `sizeBytes` exactly**, or the server rejects the upload with
  `INVALID_ARGUMENT` at the end of the stream — this is what makes
  `sizeBytes` a value the SDK, and the server on download, can trust, rather
  than just a caller-supplied claim.
- **Re-uploading an existing `fileId` replaces it, with no error for the
  duplicate** — no separate delete-then-upload dance is required.
  `downloadFile`/`statFile` afterward always serve the newest upload.
- **Files are capped at the server's `limits.max_file_bytes`** (5 GiB by
  default). `uploadFile` and `uploadFileStream` check this client-side,
  before any RPC, and throw a `RociaDbError` (`kind: "validation"`) if it is
  exceeded.
- **An empty file is valid and common**: it needs exactly one message
  (metadata only, empty `chunk`) and no data messages. `uploadFile` and
  `uploadFileStream` both send it automatically.
- **The file only becomes visible** (in `listFiles`, `statFile`,
  `downloadFile`) once the whole stream has been received and validated. An
  interrupted stream leaves orphaned chunks that a background GC eventually
  reclaims; the partial file never appears anywhere, so retrying (with a
  fresh `requestId`) is always safe.
- **`deleteFile` removes a whole file by prefix**, regardless of how many
  chunks it was stored in or what chunk size wrote them.

Server `1.0.0-rc.16` changed the *download* side of this contract, which is
why the bullets above no longer mention a fixed chunk size: **before
`rc.16`, the server derived how many chunks to read back on download as
`ceil(sizeBytes / 1 MiB)`, so any upload chunk size other than exactly 1 MiB
made a later download silently return truncated or garbled data, with no
error at all, at upload or download time.** That is why this SDK has always
defaulted to exactly-1-MiB chunking in `uploadFile`/`uploadFileStream`, and
why it still does: this chunking remains correct, and is still the most
efficient choice, against `rc.16`, and it is the only chunking that stays
safe against a pre-`rc.16` server. The same guessed-chunk-count assumption
affected `Delete` before `rc.16` too (it stopped after the same assumed
chunk count, leaving a tail of orphaned chunks behind for any file that had
used a different chunk size); `Delete` removing by prefix, as described
above, is also new as of `rc.16`. See
[Migrating to 0.6.0](#migrating-to-060) for the full correction and
[Migrating to 0.3.0](#migrating-to-030) for the historical bug this SDK
originally shipped a fix for.

### Buffered files

Use the buffered helpers for reasonably sized objects. `uploadFile` computes
a SHA-256 checksum of `bytes` automatically when `options.checksum` is
omitted:

```ts
const payload = Buffer.from("hello RociaDB");

await client.uploadFile(
  "tenant-1",
  "assets",
  "manual.txt",
  payload,
  { contentType: "text/plain; charset=utf-8" },
);

const metadata = await client.statFile("tenant-1", "assets", "manual.txt");
console.log(metadata.sizeBytes, metadata.contentType);
console.log(Buffer.from(metadata.checksum).toString("hex")); // sha256 hex

const bytes = await client.downloadFile("tenant-1", "assets", "manual.txt");
await client.deleteFile("tenant-1", "assets", "manual.txt");
```

`sizeBytes` is returned as `bigint`. `metadata.checksum` is the raw 32-byte
digest; hex-encode it for display or comparison, as shown above. If you
already have a checksum computed elsewhere, pass it as `options.checksum`
instead — it must be exactly 32 bytes, checked before any network call.

### Streaming an upload without buffering the whole file

Streaming avoids holding the complete object in memory. The caller must know
the total upload size **and a precomputed SHA-256 checksum** before starting
the RPC — unlike `uploadFile`, `uploadFileStream` cannot hash the data for
you, because the checksum has to travel on the first message, before the SDK
has read anything from your source:

```ts
import { createHash } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { stat } from "node:fs/promises";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

const source = "./large.bin";
const info = await stat(source);

const checksum = await new Promise<Buffer>((resolve, reject) => {
  const hash = createHash("sha256");
  createReadStream(source)
    .on("data", (chunk) => hash.update(chunk))
    .on("end", () => resolve(hash.digest()))
    .on("error", reject);
});

await client.uploadFileStream(
  {
    tenantId: "tenant-1",
    bucket: "assets",
    fileId: "large.bin",
    sizeBytes: BigInt(info.size),
    contentType: "application/octet-stream",
    checksum,
  },
  createReadStream(source),
);

await pipeline(
  Readable.from(client.downloadFileStream("tenant-1", "assets", "large.bin")),
  createWriteStream("./downloaded.bin"),
);
```

The source iterable may yield chunks of any size — the second
`createReadStream` call above reads the file again at its default 64 KiB —
because `uploadFileStream` re-buffers internally and only ever writes 1 MiB
per outgoing message, per
[The upload wire contract](#the-upload-wire-contract) above.

**Naming trap when porting code between SDKs:** despite doing this
re-chunking and validation, this method is not the raw, zero-validation
escape hatch — that is `uploadFileRaw`, covered next. The Rust SDK's
`upload_file_stream` *is* that raw escape hatch there; this method's
counterpart on the Rust side is instead named `upload_file_chunked`. See
[Parity with the Rust SDK](#parity-with-the-rust-sdk) for the full naming
table before porting upload code between the two SDKs by name alone.

### Raw streaming upload escape hatch

`uploadFileRaw` passes every message straight through to the gRPC call
exactly as given — **no re-chunking, no checksum validation, and no
distinction between the first message and the rest.** You are fully
responsible for the wire contract described in
[The upload wire contract](#the-upload-wire-contract) above: the first
`RawUploadMessage` must carry `tenantId`, `bucket`, `fileId`, the exact
total `sizeBytes`, and a 32-byte SHA-256 `checksum`; every message's
`chunk` must be at most 1 MiB, or the upload fails with
`INVALID_ARGUMENT`; `sizeBytes` must equal the exact sum of every `chunk`
sent; and the metadata fields on messages after the first are ignored by
the server and may be left empty.

```ts
import { createHash, randomUUID } from "node:crypto";
import type { RawUploadMessage } from "rocia-db-sdk";

async function* rawUpload(): AsyncGenerator<RawUploadMessage> {
  const payload = Buffer.from("hello RociaDB");
  yield {
    tenantId: "tenant-1",
    bucket: "assets",
    fileId: "manual.txt",
    sizeBytes: BigInt(payload.byteLength),
    contentType: "text/plain",
    checksum: createHash("sha256").update(payload).digest(),
    chunk: payload,
    requestId: `upload_file:${randomUUID()}`,
  };
  // A larger file would yield further messages here, chunk <= 1 MiB each;
  // metadata fields on those later messages are ignored by the server and
  // can be left blank.
}

await client.uploadFileRaw(rawUpload());
```

As of server `rc.16`, getting the chunk *size* wrong here fails fast with
`INVALID_ARGUMENT` rather than silently corrupting a later download — but a
wrong `sizeBytes` total, or a `checksum` that does not actually match the
bytes (the server only checks its length, never its content), can still
slip through as an upload that looks successful while carrying bad data.
Prefer `uploadFile` or `uploadFileStream` above unless you specifically need
to hand-build the message stream — for example to interleave upload
messages with transport-level logic that neither assisted helper exposes.

### Discovering buckets and files

```ts
const buckets = await client.listBuckets("tenant-1");
const files = await client.listFiles("tenant-1", "assets", { limit: 100 });
```

## Tenants

`listTenants` is the only RPC that takes no `tenantId`: it enumerates every
tenant known to the deployment (the registry is filled in implicitly — a
tenant appears the first time any RPC mentions it). It lives on its own
service so a dedicated authorization policy could be attached to it
independently of the data-plane services; today, any authenticated client
can call it, including a read-only one. The one credential that cannot is an
admin-scoped token — like every other RPC, it gets `PERMISSION_DENIED` (see
[Authentication](#authentication)).

```ts
let cursor: string | undefined;
do {
  const page = await client.listTenants({ limit: 100, cursor });
  for (const tenantId of page.items) console.log(tenantId);
  cursor = page.nextCursor;
} while (cursor);
```

## Idempotence and Retries

Every mutation creates a unique request ID by default. When retrying an
operation after a timeout, reuse the same request ID so the server can recognize
the attempt as the same logical mutation:

```ts
import { randomUUID } from "node:crypto";

const requestId = randomUUID();
await client.putDocument(
  "tenant-1",
  "products",
  "sku-123",
  product,
  requestId,
);
```

A `requestId` is scoped to `(tenantId, operation, requestId)` — replaying
the same ID against a *different* operation is not treated as the same
mutation. Reusing a `putDocument` call's `requestId` on a later
`deleteDocument` call, for instance, does not cancel or replace the earlier
write; it is simply a different idempotency key. Markers expire after the
server's `gc.request_ttl_secs` (24 hours by default); a retry older than
that window executes again rather than being deduplicated.

For nodes, documents, edges, and deletes, request IDs are optional method
arguments or fields on `NodeInput` and `EdgeInput`. Batch helpers generate one
request ID per item when none is supplied.

## Error Handling

All SDK failures use `RociaDbError`. Every instance carries a
`kind: RociaDbErrorKind` field — one of six values — that discriminates
*why* the call failed without requiring a class hierarchy (which would break
any existing `error instanceof RociaDbError` check):

| `kind` | Meaning |
|---|---|
| `"status"` | A gRPC call returned a non-OK status, or an unexpectedly empty response. `code`/`reason` (below) narrow this further. |
| `"connection"` | Failed to connect to, or configure, the endpoint — invalid host, TLS setup, connection refused, missing builder configuration, a non-positive connect timeout. |
| `"auth"` | Failed to obtain or refresh the OAuth2 bearer token (`fetchOAuthToken`/`TokenManager`). |
| `"encode"` | Failed to `JSON.stringify` a document, node, or edge payload before sending it. |
| `"decode"` | Failed to `JSON.parse` a payload received from the server, or to parse a wire `uint64` into a `bigint`. |
| `"validation"` | A client-side rule was rejected before any network call — a non-positive page `limit`, a checksum of the wrong length, a file size out of bounds, a partial `nodeLabel`/`nodeGraph` pair, and so on. |

For gRPC failures (`kind === "status"`), `code` contains a standard
`@grpc/grpc-js` status code, `reason` carries the server's `reason` gRPC
trailing metadata (finer-grained than `code` alone), and `cause` retains the
original error. The other five kinds usually carry no `code`/`reason` at
all, since they are raised client-side before any RPC. Narrow on `kind`
first to handle a whole category (every validation error alike, say), and
on `code`/`reason` for gRPC-specific branching:

```ts
import { status } from "@grpc/grpc-js";
import { RociaDbError } from "rocia-db-sdk";

try {
  await client.getDocument("tenant-1", "products", "missing");
} catch (error) {
  if (!(error instanceof RociaDbError)) throw error;
  if (error.kind === "status" && error.code === status.NOT_FOUND) {
    console.log("document does not exist");
  } else {
    throw error;
  }
}
```

| gRPC `code` | `reason` | When |
|---|---|---|
| `INVALID_ARGUMENT` | `invalid_argument` | Missing/malformed field, `limit` out of bounds, unreadable cursor, invalid JSON |
| `NOT_FOUND` | `not_found` | Document, node, edge, or file does not exist |
| `ALREADY_EXISTS` | `already_exists` | A uniqueness conflict |
| `PERMISSION_DENIED` | `permission_denied` | Insufficient scope — see [Authentication](#authentication) |
| `UNAUTHENTICATED` | `unauthenticated` | Token missing, expired, malformed, or from another issuer — see [Authentication](#authentication) |
| `INTERNAL` | `internal` | Storage-layer failure |

OAuth failures (`kind: "auth"`), invalid configuration (`kind:
"connection"`), JSON encoding/decoding errors (`kind: "encode"`/`"decode"`),
and invalid pagination arguments or file sizes/checksums (`kind:
"validation"`) also use `RociaDbError`, usually without a gRPC `code` or
`reason` since they are caught client-side before any RPC is made.

## Advanced: Raw Protobuf Access

`rocia-db-sdk/proto` exports the generated request/response shapes and gRPC
service-client interfaces backing every `RociaDbClient` method, for callers
who need to build a custom gRPC client against the same `.proto` file
instead of going through `RociaDbClient`:

```ts
import { createServiceClients } from "rocia-db-sdk/proto";
```

This module mirrors the Rust SDK's `#[doc(hidden)] pub mod pb` — it is
**not** part of this package's semver contract. A routine
`@grpc/proto-loader` upgrade can reshape it without the rest of the SDK's
own API changing at all. Depend on it only if you accept that its shapes can
change between any two versions of `rocia-db-sdk`.

## API Coverage

| Service | RPC | SDK method |
|---|---|---|
| Document | `PutDoc` | `putDocument`, `createDocument` |
| Document | `GetDoc` | `getDocument<T>` |
| Document | `DeleteDoc` | `deleteDocument` |
| Document | `FindByField` | `findDocumentsByField<T>` |
| Document | `ListDoc` | `listDocuments<T>` |
| Document | `QueryDoc` | `queryDocuments<T>` |
| Document | `ListCollections` | `listCollections` |
| Graph | `PutNode` | `putNode`, `putNodes` |
| Graph | `GetNode` | `getNode<T>` |
| Graph | `AddEdge` | `addEdge`, `addEdges` |
| Graph | `DeleteEdge` | `deleteEdge` |
| Graph | `NeighborsOut` | `neighborsOut`, `getOutgoingNeighborNodes<T>` |
| Graph | `NeighborsIn` | `neighborsIn`, `getIncomingNeighborNodes<T>` |
| Graph | `ListGraphs` | `listGraphs` |
| Graph | `ListNodes` | `listNodes` |
| File | `Upload` | `uploadFile`, `uploadFileStream`, `uploadFileRaw` |
| File | `Download` | `downloadFile`, `downloadFileStream` |
| File | `Stat` | `statFile` |
| File | `Delete` | `deleteFile` |
| File | `ListBuckets` | `listBuckets` |
| File | `ListFiles` | `listFiles` |
| Tenant | `ListTenants` | `listTenants` |

## Parity with the Rust SDK

This SDK and the Rust SDK
([`rociadb-core-sdk-rust`](https://github.com/RociaDBSebastienS/rociadb-core-sdk-rust))
cover the same 22 RPCs against the same server, and are maintained to the
same standard: **every capability available in one is available in the
other.** Neither imitates the other's syntax — this package stays
camelCase/exception-idiomatic TypeScript, the Rust crate stays
snake_case/`Result`-idiomatic Rust — but a piece of client code should
always have a mechanical translation from one SDK to the other. Parity is
about what you can *do*, not about matching method names character for
character, and most names do translate mechanically (`putNodes` ↔
`put_nodes`, `getOutgoingNeighborNodes` ↔ `get_outgoing_neighbor_nodes`, and
so on). The handful of places where a name does **not** translate
mechanically — where translating a call by ear lands you on the wrong
method — are the naming table below.

| Capability | TypeScript (this SDK) | Rust ([`rociadb-core-sdk-rust`](https://github.com/RociaDBSebastienS/rociadb-core-sdk-rust)) | Note |
|---|---|---|---|
| Assisted streaming upload — re-chunks to the 1 MiB wire contract, validates the total, caller supplies the checksum | `uploadFileStream` | `upload_file_chunked` | Names do **not** correspond — see the naming trap below. |
| Raw streaming upload — zero validation, caller builds every protobuf message | `uploadFileRaw` | `upload_file_stream` | Names do **not** correspond — the mirror image of the row above. |
| Idempotency key scoped to a `createDocument` call's document write only (the graph node binding keeps its own auto-generated key) | `createDocument(..., { requestId })` — an options-object field | `create_document_with_request_id` — a sibling method, `request_id: impl Into<String>` | Same capability, different shape: an options field vs. a sibling method, the established pattern on each side. |
| Releasing the connection and any cached auth state | `client.close()` | Drop the last live `RociaDbClient` clone | No Rust method by design — see below. |
| Lazy token invalidation at the level of the token-caching type itself (not the client-level wrapper, which *does* translate mechanically: `invalidateToken` ↔ `invalidate_auth_token`) | `TokenManager.invalidate()` | `TokenManager::request_refresh` | Different verb chosen independently on each side for the same "mark it stale, refresh on next use" idea. |
| Standalone OAuth2 token fetch, usable outside of `TokenManager` | `fetchOAuthToken` (exported from `auth.ts`, re-exported at the package root) | `auth::fetch_token` | TypeScript needed a name that does not collide with the `fetch` Web API it wraps; Rust has no such collision. |
| Discriminating why an error happened | `RociaDbError.kind: RociaDbErrorKind`, one class with a `"status" \| "connection" \| "auth" \| "encode" \| "decode" \| "validation"` field | `RociaDbError` — a `match`-able enum: `Status { .. }` / `Connection { .. }` / `Auth { .. }` / `Encode { .. }` / `Decode { .. }` / `Validation(String)` | Different shape, not just a different name — see below. |
| Escape hatch to the raw generated protobuf/gRPC types, to build a custom client against the same `.proto` | the `rocia-db-sdk/proto` subpath export (see [Advanced: Raw Protobuf Access](#advanced-raw-protobuf-access)) | the `pb` module (`#[doc(hidden)] pub mod pb`; the handful of generated types that reach a public signature are re-exported individually at the crate root instead) | Different mechanism, not just a different name: a separate `package.json` `exports` entry vs. an in-crate module. Neither is part of either package's semver contract. |

**The upload naming trap, spelled out:** `uploadFileStream` (TypeScript) and
`upload_file_chunked` (Rust) are the *same* capability — the middle tier
that re-chunks and validates for you (see
[Streaming an upload without buffering the whole file](#streaming-an-upload-without-buffering-the-whole-file)).
`uploadFileRaw` (TypeScript) and `upload_file_stream` (Rust) are also the
*same* capability — the raw, zero-validation escape hatch (see
[Raw streaming upload escape hatch](#raw-streaming-upload-escape-hatch)).
`uploadFileStream` and `upload_file_stream` are **not** each other's
counterpart, despite the near-identical name: the TypeScript one is the
validated middle tier, the Rust one is the raw escape hatch. Porting upload
code between the two SDKs by matching names alone silently swaps which tier
you land on — always cross-check against the table above, not against how
the name sounds.

**The error-kind trap, spelled out:** both sides recognize the exact same
six causes, in the same order, but represent the choice differently.
TypeScript keeps a single `RociaDbError` class (so an existing `error
instanceof RociaDbError` check never breaks) and puts the six-way choice in
a `kind` field — narrowing on `error.kind` gets the same exhaustiveness
check from `tsc` that Rust's `match` gets from `rustc`, just via a
discriminated union instead of an enum variant. Rust's `RociaDbError` is a
real sum type — matching on it is exhaustive, and the compiler flags a
missing arm. Neither representation is "the same code translated"; each is
the idiomatic way to express one closed set of causes in its own language.

**Why there is no Rust `close()`:** this SDK's `RociaDbClient` owns its gRPC
channels outright — `close()` tears them down, and the instance must not be
reused afterward. Rust's `RociaDbClient` is instead `Clone`, and every clone
shares one underlying channel and one background refresh task by design; a
`close(&self)` there would tear the channel down out from under every other
live clone. The idiomatic Rust equivalent already exists and gives the
identical guarantee: drop the last clone. Neither SDK is missing a
capability — each expresses "release the connection" the way its own
ownership model calls for.

Two capabilities are intentionally kept on one side without a mirror on the
other: `ApiKeyInterceptor` (Rust only — it validates an *incoming* API key,
so it serves building a server or a test double, not talking to RociaDB,
which puts it out of scope for a client SDK), and having both
`RociaDbBuilder.build()` and a direct `RociaDbClient.connect(options)` entry
point (this SDK only — the builder here is a thin wrapper with no
capability of its own beyond `connectTimeoutMs`, `authClientCredentials`,
and `disableAuth`, so duplicating a second entry point in Rust would add an
API to maintain for zero new capability).

## Migrating to 0.6.0

0.6.0 brings this SDK to full capability parity with the Rust SDK — see
[Parity with the Rust SDK](#parity-with-the-rust-sdk) above. **Every public
method, type, and option that existed in 0.3.0 still exists, with the same
signature and the same behavior — this release only adds.** Nothing is
removed, nothing already-shipped becomes an error, and every new capability
below is opt-in: existing call sites keep compiling and behaving exactly as
before without touching them.

**Documentation-only correction, no behavior change:** this README used to
state that the server required upload chunks of exactly 1 MiB and that
anything else silently corrupted a later download. That was accurate
against every server up to `1.0.0-rc.15`, and is no longer accurate against
`1.0.0-rc.16` and later — the server now reads a download back by
`sizeBytes`, not by an assumed chunk count. This SDK's own chunking never
needed to change (`uploadFile`/`uploadFileStream` already always emitted
exactly-1-MiB chunks, which remains correct and is still the most efficient
choice, and is still required for correctness against a pre-`rc.16` server)
— only the documentation explaining *why* was wrong, and has been
corrected. See [The upload wire contract](#the-upload-wire-contract) for
the current rules and [Migrating to 0.3.0](#migrating-to-030) for the
historical note.

New capabilities added in 0.6.0, each documented where linked:

- `RociaDbClient.refreshAuthToken()` — the eager counterpart to the existing
  `invalidateToken()` — see
  [Two ways to recover from `UNAUTHENTICATED`](#two-ways-to-recover-from-unauthenticated).
- `RociaDbClient.uploadFileRaw()` and the `RawUploadMessage` type — the raw,
  zero-validation escape hatch — see
  [Raw streaming upload escape hatch](#raw-streaming-upload-escape-hatch).
- `RociaDbError.kind: RociaDbErrorKind` — see
  [Error Handling](#error-handling).
- `fetchOAuthToken` and the `OAuthToken` type, now exported from the package
  root — see [Auth Helpers](#auth-helpers).
- The `rocia-db-sdk/proto` subpath export, exposing the generated
  protobuf/gRPC types — see
  [Advanced: Raw Protobuf Access](#advanced-raw-protobuf-access).

Also improved in 0.6.0, purely as internal hardening — no call site needs to
change for any of these:

- The connect-timeout default (10,000 ms) was already in effect before
  0.6.0; it is now backed by a single named internal constant, pinned to
  match the Rust SDK's own `Duration::from_secs(10)` default, instead of
  the same literal `10_000` repeated in two places — see
  [Connecting](#connecting). This constant is not part of the public API
  surface (it is not re-exported from the package root).
- `putNodes`, `addEdges`, and the neighbor-node helpers
  (`getOutgoingNeighborNodes`, `getIncomingNeighborNodes`) now actively
  cancel every other already-dispatched call in a batch as soon as one item
  fails, instead of merely letting them run to completion unobserved. This
  reduces wasted server-side work on a batch that is going to be reported as
  failed anyway; it does not change what "not atomic" means for these
  methods (see [Nodes and batches](#nodes-and-batches) and
  [Directed edges and neighbors](#directed-edges-and-neighbors)) or what a
  caller should do to resume — reuse the same `requestId` values on retry,
  exactly as before.
- `RociaDbError`'s constructor now requires its `options` argument
  (previously optional) and requires `kind` within it. This only affects
  code that constructs `RociaDbError` directly — uncommon, since almost all
  call sites only catch and read it; every existing
  `catch`/`instanceof`/`.code`/`.reason` read of a `RociaDbError` the SDK
  throws is unaffected.

0.6.0 is released together with, and version-numbered to match, the Rust
SDK's own 0.6.0 — a byproduct of bringing the two to capability parity in
the same pass, not a versioning scheme this changelog is committing either
SDK to for future releases.

## Migrating to 0.3.0

Two changes in this release fix data-loss and upload-rejection bugs; both
require caller changes.

**`FileUploadOptions.chunkSize` was removed.** Upload chunking now always
uses the server's fixed 1 MiB boundary internally and is no longer a caller
choice — delete `chunkSize` from any `uploadFile(...)` call:

```diff
 await client.uploadFile(tenant, bucket, fileId, payload, {
   contentType: "text/plain",
-  chunkSize: 64 * 1024,
 });
```

There is no replacement option: 1 MiB is the only chunk size the server
accepts, so nothing you could set `chunkSize` to would have been meaningful.

**A checksum is now required, and previously every default-configuration
upload was silently rejected.** Before this release, the SDK forwarded
whatever `checksum` you passed — including nothing at all — and the server
rejects any checksum that is not exactly 32 bytes, so uploads made without
an explicit checksum failed with `INVALID_ARGUMENT`. Two independent fixes:

- `uploadFile` now computes a SHA-256 digest of the buffer automatically
  when `options.checksum` is omitted — most callers need no code change
  beyond removing `chunkSize` as above.
- `uploadFileStream`'s `file.checksum` field changed from optional to
  **required**. Since the checksum has to travel on the RPC's first
  message, before the SDK has read anything from your source, it cannot be
  computed automatically the way `uploadFile` computes it for an in-memory
  buffer — hash your source ahead of time and pass the digest in. See
  [Streaming an upload without buffering the whole file](#streaming-an-upload-without-buffering-the-whole-file)
  for a worked example.

**Uploads made through `uploadFileStream` with a pre-0.3.0 SDK against a
source that yielded chunks smaller than 1 MiB — the common case, since
`fs.createReadStream()`'s default is 64 KiB — were stored correctly but
silently truncated on every subsequent download**, per the upload wire
contract of the day. This version fixes the re-chunking so newly uploaded
files are no longer affected, but it cannot repair files already stored
short. There is no way to detect the truncation from `statFile` alone:
`sizeBytes` reflects what was declared and validated at upload time (the
total bytes across all chunks summed to it), not what `downloadFile` can
actually reconstruct with the old chunk layout. Verify suspect files by
downloading and comparing the returned length against `metadata.sizeBytes`,
or simply re-upload the ones you still have the source for.

**This section was accurate at the time it was written, against every
server version up to `1.0.0-rc.15`.** Server `1.0.0-rc.16` (see
[Migrating to 0.6.0](#migrating-to-060)) changed the download side of this
contract: it now reads back exactly `sizeBytes` bytes instead of guessing
`ceil(sizeBytes / 1 MiB)` chunk indexes, so an upload chunked at anything
other than exactly 1 MiB no longer corrupts a later download. This SDK's
behavior described above did not need to change — `uploadFileStream`
already always emitted exactly-1-MiB chunks — but the *reason* it still
does is now efficiency and pre-`rc.16` compatibility, not correctness
against the current server. See
[The upload wire contract](#the-upload-wire-contract) for the current
rules.

## Development and Publication

```bash
mise install
mise run check
npm publish
```

Without mise, run `npm ci`, `npm run typecheck`, `npm test`, and
`npm pack --dry-run`. Before publishing, confirm that the package name is
available and that the npm account has permission to publish it.

Licensed under Apache-2.0.
