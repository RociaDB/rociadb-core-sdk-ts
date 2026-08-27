import type { status } from "@grpc/grpc-js";

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

/** Controls one cursor-based page. Cursors are opaque and must not be modified. */
export interface PageOptions {
  /** Maximum number of results requested from the server. Defaults to 20. */
  limit?: number;
  /** Cursor returned by the preceding page. Omit for the first page. */
  cursor?: string;
}

/** One page returned by a graph listing operation. */
export interface Page<T> {
  items: T[];
  nextCursor?: string;
}

/** One document page, including the total number of matching documents. */
export interface DocumentPage<T> extends Page<T> {
  /** A bigint is used to preserve the full protobuf uint64 range. */
  totalCount: bigint;
}

/** One document collection together with the number of documents it holds. */
export interface CollectionInfo {
  collection: string;
  /** A bigint is used to preserve the full protobuf uint64 range. */
  count: bigint;
}

export type DocumentQueryOperator = "eq" | "in" | "contains";
export type DocumentSortDirection = "asc" | "desc";

/** One server-side document filter. Filters in a query are combined by the server. */
export interface DocumentQueryFilter {
  field: string;
  operator: DocumentQueryOperator;
  values: unknown[];
}

/** One sort level. Sort entries are applied in array order. */
export interface DocumentQuerySort {
  field: string;
  direction: DocumentSortDirection;
}

/** Filters, sorting, and pagination for {@link RociaDbClient.queryDocuments}. */
export interface DocumentQuery extends PageOptions {
  filters?: readonly DocumentQueryFilter[];
  sort?: readonly DocumentQuerySort[];
}

/** A node mutation used by the bounded-concurrency batch helper. */
export interface NodeInput<T = unknown> {
  nodeId: string;
  value: T;
  requestId?: string;
}

/** A directed edge mutation used by single and batch edge helpers. */
export interface EdgeInput<T = unknown> {
  edgeId: string;
  from: string;
  to: string;
  label: string;
  value: T;
  requestId?: string;
}

/** A raw graph neighbor without its node JSON payload. */
export interface Neighbor {
  nodeId: string;
  edgeId: string;
}

/** A graph neighbor together with its decoded node JSON payload. */
export interface NeighborNode<T> extends Neighbor {
  value: T;
}

/** Metadata returned by the file Stat RPC. */
export interface FileMetadata {
  sizeBytes: bigint;
  contentType: string;
  checksum: Uint8Array;
  createdAt: string;
  updatedAt: string;
}

/**
 * Controls an in-memory file upload. Chunking always uses the server's fixed 1 MiB
 * chunk size and is not configurable: sending a different chunk size than the server
 * reads back (also 1 MiB) silently truncates downloads, so the option was removed
 * rather than kept as a footgun.
 *
 * If `checksum` is omitted, the SDK computes a SHA-256 digest of `bytes` automatically
 * (the server rejects any checksum whose length is not exactly 32 bytes). If supplied,
 * it must already be a 32-byte SHA-256 digest.
 */
export interface FileUploadOptions {
  contentType?: string;
  checksum?: Uint8Array;
  requestId?: string;
}

/**
 * Metadata required before a streaming upload can begin.
 *
 * `checksum` is required (must be exactly 32 bytes, i.e. a SHA-256 digest): file
 * metadata, including the checksum, travels on the first gRPC message, before the SDK
 * has read any chunk from the source, so it cannot be computed on the fly the way
 * {@link RociaDbClient.uploadFile} computes it for in-memory buffers. Hash the source
 * ahead of time (e.g. with a first pass over the file) if you only have raw bytes.
 */
export interface FileStreamUpload {
  tenantId: string;
  bucket: string;
  fileId: string;
  sizeBytes: bigint;
  contentType?: string;
  checksum: Uint8Array;
  requestId?: string;
}

/** OAuth2 client-credentials configuration used for outgoing gRPC metadata. */
export interface ClientCredentials {
  tokenUrl: string;
  clientId: string;
  clientSecret: string;
  refreshSkewMs?: number;
}

/** Direct connection options for {@link RociaDbClient.connect}. */
export interface RociaDbClientOptions {
  host?: string;
  auth?: ClientCredentials | false;
  connectTimeoutMs?: number;
}

/**
 * Error raised by the SDK.
 *
 * For gRPC failures, {@link code} contains the grpc-js status code and `cause` retains
 * the original service error. {@link reason} carries the server's `reason` trailing
 * metadata (one of `invalid_argument`, `not_found`, `already_exists`, `permission_denied`,
 * `unauthenticated`, or `internal`), which is finer-grained than `code` alone — for
 * example it distinguishes `UNAUTHENTICATED` (the token expired; refresh and retry, see
 * {@link RociaDbClient.invalidateToken}) from `PERMISSION_DENIED` (the token's scope is
 * insufficient; retrying will not help).
 */
export class RociaDbError extends Error {
  readonly code?: status;
  readonly reason?: string;

  constructor(message: string, options?: { cause?: unknown; code?: status; reason?: string }) {
    super(message, { cause: options?.cause });
    this.name = "RociaDbError";
    this.code = options?.code;
    this.reason = options?.reason;
  }
}
