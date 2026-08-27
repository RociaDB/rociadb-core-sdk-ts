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
    `addEdge`/`addEdges`, `deleteEdge`, `uploadFile`/`uploadFileStream`, or
    `deleteFile`. A read-only token is not otherwise crippled — all 15 read
    RPCs remain available.
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

### The upload protocol

Understanding how uploads are transported explains every constraint below.
`Upload` is a client-streaming RPC: the **first** message carries the file's
metadata (`tenantId`, `bucket`, `fileId`, `sizeBytes`, `contentType`,
`checksum`, `requestId`); every message after that carries only a `chunk`.
The server stores one chunk per non-empty message, at a sequential index
starting from 0. On download, it computes `ceil(sizeBytes / 1 MiB)` and
reads back exactly that many stored chunks, concatenated in order.

That reconstruction rule is why chunk size is not a free choice: **every
outgoing message must carry exactly 1 MiB, except the last one, which
carries the remainder.** A client that instead wrote messages sized to
whatever it read from its source — for example the 64 KiB pieces
`fs.createReadStream` yields by default — would still have its upload
accepted (the total byte count still matches `sizeBytes`), but the file
would come back truncated on download, silently, with no error at upload or
download time. `uploadFile` and `uploadFileStream` regroup whatever pieces
you hand them into correctly sized 1 MiB messages internally, so a plain
`createReadStream()` is safe to pass directly; see
[Migrating to 0.3.0](#migrating-to-030) if you are upgrading from a version
that let you choose the chunk size yourself.

Other server-enforced limits:

- **Checksum**: `checksum` must be exactly 32 raw bytes — a SHA-256 digest.
  Any other length, including empty, is rejected with `INVALID_ARGUMENT`
  before a single chunk is read. The server does not verify that the
  checksum matches the uploaded bytes, only that its length is correct.
- **Size**: files are capped at the server's `limits.max_file_bytes` (5 GiB
  by default). The SDK checks this client-side, before any RPC, in both
  `uploadFile` and `uploadFileStream`.
- **Empty files work**: a zero-byte file still needs one message to deliver
  its metadata; the SDK sends it automatically.
- **An interrupted stream leaves orphaned chunks.** The file only becomes
  visible in `listFiles` and `statFile` once the upload completes and the
  server finishes validating it — a partial upload never appears, so there
  is nothing to clean up, and retrying (with a fresh `requestId`) is safe.

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

### Streaming large files

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
per outgoing message, per [The upload protocol](#the-upload-protocol) above.

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

All SDK failures use `RociaDbError`. For gRPC failures, `code` contains a
standard `@grpc/grpc-js` status code, `reason` carries the server's `reason`
gRPC trailing metadata (finer-grained than `code` alone), and `cause`
retains the original error:

```ts
import { status } from "@grpc/grpc-js";
import { RociaDbError } from "rocia-db-sdk";

try {
  await client.getDocument("tenant-1", "products", "missing");
} catch (error) {
  if (error instanceof RociaDbError && error.code === status.NOT_FOUND) {
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

OAuth failures, invalid configuration, JSON encoding/decoding errors, invalid
pagination arguments, and invalid file sizes or checksums also use
`RociaDbError`, usually without a gRPC `code` or `reason` since they are
caught client-side before any RPC is made.

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
| File | `Upload` | `uploadFile`, `uploadFileStream` |
| File | `Download` | `downloadFile`, `downloadFileStream` |
| File | `Stat` | `statFile` |
| File | `Delete` | `deleteFile` |
| File | `ListBuckets` | `listBuckets` |
| File | `ListFiles` | `listFiles` |
| Tenant | `ListTenants` | `listTenants` |

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
  [Streaming large files](#streaming-large-files) for a worked example.

**Uploads made through `uploadFileStream` with a pre-0.3.0 SDK against a
source that yielded chunks smaller than 1 MiB — the common case, since
`fs.createReadStream()`'s default is 64 KiB — were stored correctly but
silently truncated on every subsequent download**, per
[The upload protocol](#the-upload-protocol). This version fixes the
re-chunking so newly uploaded files are no longer affected, but it cannot
repair files already stored short. There is no way to detect the truncation
from `statFile` alone: `sizeBytes` reflects what was declared and validated
at upload time (the total bytes across all chunks summed to it), not what
`downloadFile` can actually reconstruct with the old chunk layout. Verify
suspect files by downloading and comparing the returned length against
`metadata.sizeBytes`, or simply re-upload the ones you still have the
source for.

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
