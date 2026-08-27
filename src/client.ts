import * as grpc from "@grpc/grpc-js";
import { createHash, randomUUID } from "node:crypto";
import { once } from "node:events";
import { TokenManager } from "./auth.js";
import {
  createServiceClients,
  type AddEdgeRequest,
  type DeleteDocRequest,
  type DeleteEdgeRequest,
  type DeleteRequest,
  type DocumentListResponse,
  type DownloadRequest,
  type FileServiceClient,
  type FindByFieldRequest,
  type GetDocRequest,
  type GetNodeRequest,
  type GraphServiceClient,
  type ListBucketsRequest,
  type ListCollectionsRequest,
  type ListDocRequest,
  type ListFilesRequest,
  type ListGraphsRequest,
  type ListNodesRequest,
  type ListTenantsRequest,
  type NeighborsInRequest,
  type NeighborsOutRequest,
  type NeighborsResponse,
  type PutDocRequest,
  type PutNodeRequest,
  type QueryDocRequest,
  type StatRequest,
  type TenantServiceClient,
  type UploadRequest,
  type DocumentServiceClient,
} from "./proto.js";
import type {
  ClientCredentials,
  CollectionInfo,
  DocumentPage,
  DocumentQuery,
  EdgeInput,
  FileMetadata,
  FileStreamUpload,
  FileUploadOptions,
  Neighbor,
  NeighborNode,
  NodeInput,
  Page,
  PageOptions,
  RawUploadMessage,
  RociaDbClientOptions,
} from "./types.js";
import { RociaDbError } from "./types.js";
import {
  CONCURRENT_REQUESTS,
  DEFAULT_PAGE_SIZE,
  decodeJson,
  encodeJson,
  mapConcurrent,
  optionalCursor,
  rpcError,
  unary,
} from "./utils.js";

const QUERY_OPERATORS = { eq: 1, in: 2, contains: 3 } as const;
const SORT_DIRECTIONS = { asc: 1, desc: 2 } as const;

/**
 * Size of every upload message this SDK emits, except the last one. This is the
 * server's per-message ceiling (server.rs `CHUNK_SIZE`); a larger message is refused with
 * `INVALID_ARGUMENT`, so sending exactly this much is the fewest messages a file can take.
 * It is also the only chunk size safe against a server older than `1.0.0-rc.16`, which
 * reassembled a download from a guessed chunk count instead of the recorded `size_bytes`.
 */
const UPLOAD_CHUNK_BYTES = 1_048_576; // 1 MiB
/** Server default `limits.max_file_bytes`; checked client-side to fail fast. */
const MAX_FILE_BYTES = 5 * 1024 * 1024 * 1024; // 5 GiB
/** SHA-256 digest length the server requires ("checksum must be 32 bytes (sha256)"). */
const CHECKSUM_BYTES = 32;
/**
 * Default connect deadline applied when {@link RociaDbBuilder.connectTimeout} is never
 * called and {@link RociaDbClientOptions.connectTimeoutMs} is omitted.
 */
export const DEFAULT_CONNECT_TIMEOUT_MS = 10_000;

interface Services {
  documents: DocumentServiceClient;
  graph: GraphServiceClient;
  files: FileServiceClient;
  tenants: TenantServiceClient;
}

type NeighborDirection = "out" | "in";

/** Fluent builder for a connected {@link RociaDbClient}. */
export class RociaDbBuilder {
  #host = "http://127.0.0.1:50051";
  #auth: ClientCredentials | false | undefined;
  #connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;

  /** Set the gRPC endpoint. The URL scheme selects insecure HTTP or TLS. */
  host(host: string): this {
    this.#host = host;
    return this;
  }

  /** Configure OAuth2 client credentials instead of reading environment variables. */
  authClientCredentials(
    tokenUrl: string,
    clientId: string,
    clientSecret: string,
    refreshSkewMs?: number,
  ): this {
    this.#auth = { tokenUrl, clientId, clientSecret, refreshSkewMs };
    return this;
  }

  /** Disable authorization metadata. Intended only for controlled environments. */
  disableAuth(): this {
    this.#auth = false;
    return this;
  }

  /** Set the deadline used while waiting for every gRPC service. */
  connectTimeout(timeoutMs: number): this {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      throw new RociaDbError("Connect timeout must be greater than zero", { kind: "connection" });
    }
    this.#connectTimeoutMs = timeoutMs;
    return this;
  }

  /** Fetch the initial token, connect to the services, and return the client. */
  async build(): Promise<RociaDbClient> {
    return RociaDbClient.connect({
      host: this.#host,
      auth: this.#auth ?? credentialsFromEnvironment(),
      connectTimeoutMs: this.#connectTimeoutMs,
    });
  }
}

/**
 * Connected client covering the RociaDB document, graph, file, and tenant services.
 * Reuse one instance and call {@link close} during graceful shutdown.
 */
export class RociaDbClient {
  readonly #services: Services;
  readonly #tokenManager?: TokenManager;

  private constructor(services: Services, tokenManager?: TokenManager) {
    this.#services = services;
    this.#tokenManager = tokenManager;
  }

  /** Connect without using the fluent builder. Authentication is enabled by default. */
  static async connect(options: RociaDbClientOptions = {}): Promise<RociaDbClient> {
    const endpoint = endpointFromHost(options.host ?? "http://127.0.0.1:50051");
    const auth = options.auth === undefined ? credentialsFromEnvironment() : options.auth;
    const tokenManager = auth === false ? undefined : new TokenManager(auth);
    await tokenManager?.initialize();
    let services: Services | undefined;
    const timeout = options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
    try {
      const connectedServices = createServiceClients(endpoint.address, endpoint.credentials);
      services = connectedServices;
      await Promise.all(
        Object.values(connectedServices).map(
          (client) =>
            new Promise<void>((resolve, reject) => {
              client.waitForReady(Date.now() + timeout, (error) =>
                error ? reject(error) : resolve(),
              );
            }),
        ),
      );
    } catch (cause) {
      if (services) {
        for (const client of Object.values(services)) client.close();
      }
      throw new RociaDbError("Failed to connect to RociaDB", { kind: "connection", cause });
    }
    if (!services) {
      throw new RociaDbError("RociaDB services were not initialized", { kind: "connection" });
    }
    return new RociaDbClient(services, tokenManager);
  }

  /** Close the underlying gRPC channels. The instance must not be reused afterward. */
  close(): void {
    for (const client of Object.values(this.#services)) client.close();
  }

  /**
   * Force the next request to fetch a fresh bearer token, discarding the cached one.
   *
   * Call this after catching a {@link RociaDbError} whose `reason` is `"unauthenticated"`
   * (or `code` is `grpc.status.UNAUTHENTICATED`) before retrying: the server treats that
   * status as a renewal signal, not a permanent failure — unlike `"permission_denied"`,
   * which means the token's scope is insufficient and will not be fixed by retrying. A
   * no-op when the client was built with `disableAuth()`.
   */
  invalidateToken(): void {
    this.#tokenManager?.invalidate();
  }

  /**
   * Force an immediate, blocking refresh of the auth token and wait for the round trip
   * to complete before resolving.
   *
   * Call this after catching a {@link RociaDbError} whose `reason` is `"unauthenticated"`
   * (or `code` is `grpc.status.UNAUTHENTICATED`), before retrying the call that just
   * failed — unlike {@link invalidateToken}, which only marks the cached token stale and
   * lets the next call pay the refresh latency, this eagerly fetches now so the retry
   * itself does not race a still-in-flight refresh. A no-op resolved immediately when the
   * client was built with `disableAuth()`.
   */
  async refreshAuthToken(): Promise<void> {
    await this.#tokenManager?.refreshNow();
  }

  /** Create or replace a JSON document. Supply `requestId` when retrying a mutation. */
  async putDocument(
    tenantId: string,
    collection: string,
    documentId: string,
    value: unknown,
    requestId = `put_document:${collection}:${randomUUID()}`,
  ): Promise<void> {
    const request: PutDocRequest = {
      tenant_id: tenantId,
      collection,
      id: documentId,
      json: encodeJson(value),
      request_id: requestId,
    };
    await unary("PutDoc", this.#services.documents.putDoc.bind(this.#services.documents), request, await this.#metadata());
  }

  /**
   * Create a document and optionally a `label:id` graph node pointing to it.
   * This composite operation is not transactional: the document is written first.
   */
  async createDocument(
    tenantId: string,
    collection: string,
    documentId: string,
    value: unknown,
    options: { nodeLabel?: string; nodeGraph?: string; requestId?: string } = {},
  ): Promise<void> {
    if ((options.nodeLabel === undefined) !== (options.nodeGraph === undefined)) {
      throw new RociaDbError("nodeLabel and nodeGraph must be provided together", {
        kind: "validation",
      });
    }
    await this.putDocument(tenantId, collection, documentId, value, options.requestId);
    if (options.nodeLabel && options.nodeGraph) {
      await this.putNode(
        tenantId,
        options.nodeGraph,
        `${options.nodeLabel}:${documentId}`,
        { collection, id: documentId },
      );
    }
  }

  /** Fetch and JSON-decode one document into the caller-selected type. */
  async getDocument<T>(tenantId: string, collection: string, documentId: string): Promise<T> {
    const request: GetDocRequest = { tenant_id: tenantId, collection, id: documentId };
    const response = await unary("GetDoc", this.#services.documents.getDoc.bind(this.#services.documents), request, await this.#metadata());
    return decodeJson<T>(response.json);
  }

  /** Delete one document. Reuse `requestId` when retrying the same deletion. */
  async deleteDocument(
    tenantId: string,
    collection: string,
    documentId: string,
    requestId = `delete_document:${collection}:${randomUUID()}`,
  ): Promise<void> {
    const request: DeleteDocRequest = {
      tenant_id: tenantId,
      collection,
      id: documentId,
      request_id: requestId,
    };
    await unary("DeleteDoc", this.#services.documents.deleteDoc.bind(this.#services.documents), request, await this.#metadata());
  }

  /** Find documents whose field equals the supplied JSON value. */
  async findDocumentsByField<T>(
    tenantId: string,
    collection: string,
    field: string,
    value: unknown,
    page: PageOptions = {},
  ): Promise<DocumentPage<T>> {
    const request: FindByFieldRequest = {
      tenant_id: tenantId,
      collection,
      field,
      value_json: encodeJson(value),
      page: pageRequest(page),
    };
    const response = await unary("FindByField", this.#services.documents.findByField.bind(this.#services.documents), request, await this.#metadata());
    return documentPage<T>(response);
  }

  /** Return one cursor-based page from a document collection. */
  async listDocuments<T>(
    tenantId: string,
    collection: string,
    page: PageOptions = {},
  ): Promise<DocumentPage<T>> {
    const request: ListDocRequest = {
      tenant_id: tenantId,
      collection,
      page: pageRequest(page),
    };
    const response = await unary("ListDoc", this.#services.documents.listDoc.bind(this.#services.documents), request, await this.#metadata());
    return documentPage<T>(response);
  }

  /** Run a paginated, filtered, and sorted document query. */
  async queryDocuments<T>(
    tenantId: string,
    collection: string,
    query: DocumentQuery = {},
  ): Promise<DocumentPage<T>> {
    const request: QueryDocRequest = {
      tenant_id: tenantId,
      collection,
      filters: (query.filters ?? []).map((filter) => ({
        field: filter.field,
        operator: QUERY_OPERATORS[filter.operator],
        values_json: filter.values.map(encodeJson),
      })),
      sort: (query.sort ?? []).map((sort) => ({
        field: sort.field,
        direction: SORT_DIRECTIONS[sort.direction],
      })),
      page: pageRequest(query),
    };
    const response = await unary("QueryDoc", this.#services.documents.queryDoc.bind(this.#services.documents), request, await this.#metadata());
    return documentPage<T>(response);
  }

  /** List the collections holding at least one document, with their document counts. */
  async listCollections(tenantId: string, page: PageOptions = {}): Promise<Page<CollectionInfo>> {
    const request: ListCollectionsRequest = { tenant_id: tenantId, page: pageRequest(page) };
    const response = await unary("ListCollections", this.#services.documents.listCollections.bind(this.#services.documents), request, await this.#metadata());
    return {
      items: response.collections.map((collection) => ({
        collection: collection.collection,
        count: parseUint64(collection.count, "collection document count"),
      })),
      nextCursor: optionalCursor(response.page?.next_cursor),
    };
  }

  /** List the graph names holding at least one node. */
  async listGraphs(tenantId: string, page: PageOptions = {}): Promise<Page<string>> {
    const request: ListGraphsRequest = { tenant_id: tenantId, page: pageRequest(page) };
    const response = await unary("ListGraphs", this.#services.graph.listGraphs.bind(this.#services.graph), request, await this.#metadata());
    return { items: response.graphs, nextCursor: optionalCursor(response.page?.next_cursor) };
  }

  /** Return one cursor-based page of node IDs stored in one graph. */
  async listNodes(tenantId: string, graph: string, page: PageOptions = {}): Promise<Page<string>> {
    const request: ListNodesRequest = { tenant_id: tenantId, graph, page: pageRequest(page) };
    const response = await unary("ListNodes", this.#services.graph.listNodes.bind(this.#services.graph), request, await this.#metadata());
    return { items: response.node_ids, nextCursor: optionalCursor(response.page?.next_cursor) };
  }

  /**
   * Create or replace one graph node using its complete node ID.
   *
   * `signal`, when supplied, cancels the in-flight RPC if it aborts — used internally by
   * {@link putNodes} to cancel a batch's other already-dispatched calls as soon as one
   * fails; most callers invoking this directly have no need to pass it.
   */
  async putNode(
    tenantId: string,
    graph: string,
    nodeId: string,
    value: unknown,
    requestId = `put_node:${randomUUID()}`,
    signal?: AbortSignal,
  ): Promise<void> {
    const request: PutNodeRequest = {
      tenant_id: tenantId,
      graph,
      node_id: nodeId,
      json: encodeJson(value),
      request_id: requestId,
    };
    await unary("PutNode", this.#services.graph.putNode.bind(this.#services.graph), request, await this.#metadata(), signal);
  }

  /**
   * Put nodes with at most ten in-flight RPCs. The batch is not atomic: as soon as one
   * node fails, every other already-dispatched call in the batch is actively cancelled
   * (not merely left to finish) rather than only stopping new ones from being scheduled.
   */
  async putNodes<T>(tenantId: string, graph: string, nodes: readonly NodeInput<T>[]): Promise<void> {
    await mapConcurrent(nodes, CONCURRENT_REQUESTS, async (node, _index, signal) =>
      this.putNode(tenantId, graph, node.nodeId, node.value, node.requestId, signal),
    );
  }

  /**
   * Fetch and JSON-decode one graph node into the caller-selected type.
   *
   * `signal`, when supplied, cancels the in-flight RPC if it aborts — used internally by
   * the neighbor-node helpers to cancel a batch's other already-dispatched calls as soon
   * as one fails; most callers invoking this directly have no need to pass it.
   */
  async getNode<T>(tenantId: string, graph: string, nodeId: string, signal?: AbortSignal): Promise<T> {
    const request: GetNodeRequest = { tenant_id: tenantId, graph, node_id: nodeId };
    const response = await unary("GetNode", this.#services.graph.getNode.bind(this.#services.graph), request, await this.#metadata(), signal);
    return decodeJson<T>(response.json);
  }

  /**
   * Create or replace one directed edge and its JSON payload.
   *
   * `signal`, when supplied, cancels the in-flight RPC if it aborts — used internally by
   * {@link addEdges} to cancel a batch's other already-dispatched calls as soon as one
   * fails; most callers invoking this directly have no need to pass it.
   */
  async addEdge(
    tenantId: string,
    graph: string,
    edge: EdgeInput,
    signal?: AbortSignal,
  ): Promise<void> {
    const request: AddEdgeRequest = {
      tenant_id: tenantId,
      graph,
      edge_id: edge.edgeId,
      from: edge.from,
      to: edge.to,
      label: edge.label,
      json: encodeJson(edge.value),
      request_id: edge.requestId ?? randomUUID(),
    };
    await unary("AddEdge", this.#services.graph.addEdge.bind(this.#services.graph), request, await this.#metadata(), signal);
  }

  /**
   * Add edges with at most ten in-flight RPCs. The batch is not atomic: as soon as one
   * edge fails, every other already-dispatched call in the batch is actively cancelled
   * (not merely left to finish) rather than only stopping new ones from being scheduled.
   */
  async addEdges<T>(tenantId: string, graph: string, edges: readonly EdgeInput<T>[]): Promise<void> {
    await mapConcurrent(edges, CONCURRENT_REQUESTS, async (edge, _index, signal) =>
      this.addEdge(tenantId, graph, edge, signal),
    );
  }

  /** Delete one edge by its raw edge ID. */
  async deleteEdge(
    tenantId: string,
    graph: string,
    edgeId: string,
    requestId = `delete_edge:${randomUUID()}`,
  ): Promise<void> {
    const request: DeleteEdgeRequest = {
      tenant_id: tenantId,
      graph,
      edge_id: edgeId,
      request_id: requestId,
    };
    await unary("DeleteEdge", this.#services.graph.deleteEdge.bind(this.#services.graph), request, await this.#metadata());
  }

  /** Return one page of nodes reached by outgoing edges from `from`. */
  async neighborsOut(
    tenantId: string,
    graph: string,
    from: string,
    label: string,
    page: PageOptions = {},
  ): Promise<Page<Neighbor>> {
    const request: NeighborsOutRequest = {
      tenant_id: tenantId,
      graph,
      from,
      label,
      page: pageRequest(page),
    };
    const response = await unary("NeighborsOut", this.#services.graph.neighborsOut.bind(this.#services.graph), request, await this.#metadata());
    return neighborPage(response);
  }

  /** Return one page of nodes connected by incoming edges to `to`. */
  async neighborsIn(
    tenantId: string,
    graph: string,
    to: string,
    label: string,
    page: PageOptions = {},
  ): Promise<Page<Neighbor>> {
    const request: NeighborsInRequest = {
      tenant_id: tenantId,
      graph,
      to,
      label,
      page: pageRequest(page),
    };
    const response = await unary("NeighborsIn", this.#services.graph.neighborsIn.bind(this.#services.graph), request, await this.#metadata());
    return neighborPage(response);
  }

  /** Follow all outgoing-neighbor pages and hydrate every node payload. */
  async getOutgoingNeighborNodes<T>(
    tenantId: string,
    graph: string,
    nodeId: string,
    label: string,
  ): Promise<NeighborNode<T>[]> {
    return this.#getNeighborNodes<T>("out", tenantId, graph, nodeId, label);
  }

  /** Follow all incoming-neighbor pages and hydrate every node payload. */
  async getIncomingNeighborNodes<T>(
    tenantId: string,
    graph: string,
    nodeId: string,
    label: string,
  ): Promise<NeighborNode<T>[]> {
    return this.#getNeighborNodes<T>("in", tenantId, graph, nodeId, label);
  }

  /**
   * Upload an in-memory file. Internally re-chunked to the server's fixed 1 MiB chunk
   * size (not configurable, see {@link FileUploadOptions}). If `options.checksum` is
   * omitted, a SHA-256 digest of `bytes` is computed automatically; if supplied, it is
   * validated to be exactly 32 bytes before any network call is made.
   */
  async uploadFile(
    tenantId: string,
    bucket: string,
    fileId: string,
    bytes: Uint8Array,
    options: FileUploadOptions = {},
  ): Promise<void> {
    validateFileSize(BigInt(bytes.byteLength));
    const checksum = computeOrValidateChecksum(bytes, options.checksum);
    await this.uploadFileStream(
      {
        tenantId,
        bucket,
        fileId,
        sizeBytes: BigInt(bytes.byteLength),
        contentType: options.contentType,
        checksum,
        requestId: options.requestId,
      },
      [bytes],
    );
  }

  /**
   * Upload an iterable of chunks without buffering the complete file. `file.sizeBytes`
   * must contain the total size before streaming starts, and `file.checksum` must
   * already be a 32-byte SHA-256 digest (see {@link FileStreamUpload} for why it cannot
   * be computed from the stream itself).
   *
   * Source chunks may be of any size — the README's `createReadStream()` example emits
   * 64 KiB pieces, for instance — because this method re-buffers them internally and
   * only ever writes exactly {@link UPLOAD_CHUNK_BYTES} (1 MiB) per outgoing message
   * (the last message carries the remainder). The server stores one chunk per non-empty
   * message, so this re-buffering sends the fewest possible messages, and the fixed
   * 1 MiB size is the only one safe against a server older than `1.0.0-rc.16`, which
   * guessed the chunk count instead of reading it back from the recorded `size_bytes`.
   */
  async uploadFileStream(
    file: FileStreamUpload,
    chunks: AsyncIterable<Uint8Array> | Iterable<Uint8Array>,
  ): Promise<void> {
    validateFileSize(file.sizeBytes);
    const checksum = requireStreamChecksum(file.checksum);
    const requestId = file.requestId ?? `upload_file:${randomUUID()}`;
    await this.#writeUploadStream(
      await this.#metadata(),
      buildUploadStreamRequests(file, checksum, requestId, chunks),
    );
  }

  /**
   * Raw streaming upload escape hatch: passes every message through to the gRPC call
   * exactly as given, with **no re-chunking, no checksum validation, and no distinction
   * between the first message and the rest**. The caller is fully responsible for the
   * server's wire contract:
   * - the first message must carry `tenantId`, `bucket`, `fileId`, `sizeBytes` (the exact
   *   total byte count) and `checksum` set to the SHA-256 digest of the whole file, as
   *   exactly 32 raw bytes;
   * - every message's `chunk` must be at most 1 MiB (1_048_576 bytes); a larger single
   *   message is rejected with `INVALID_ARGUMENT`;
   * - `sizeBytes` must equal the exact sum of every chunk sent, or the upload fails with
   *   `INVALID_ARGUMENT` once the stream ends;
   * - `tenantId`, `bucket`, `fileId`, `sizeBytes`, `contentType`, and `checksum` on
   *   messages after the first are ignored by the server and may be left empty.
   *
   * Any deviation either fails the upload outright or, worse, silently corrupts a later
   * download. Only use this if you understand and reproduce that contract yourself; for
   * the common cases, use {@link uploadFile} (in-memory buffer) or {@link
   * uploadFileStream} (assisted re-chunking of arbitrarily-sized source chunks) instead.
   */
  async uploadFileRaw(
    requests: AsyncIterable<RawUploadMessage> | Iterable<RawUploadMessage>,
  ): Promise<void> {
    await this.#writeUploadStream(await this.#metadata(), buildRawUploadRequests(requests));
  }

  /**
   * Drive the file-upload client stream: writes every request from `requests` to the
   * call, respecting backpressure, and settles once the call completes or fails. Shared
   * by {@link uploadFileStream} and {@link uploadFileRaw}, which differ only in how they
   * build the `UploadRequest` sequence.
   */
  async #writeUploadStream(
    metadata: grpc.Metadata,
    requests: AsyncIterable<UploadRequest> | Iterable<UploadRequest>,
  ): Promise<void> {
    // `ended` settles exactly once, from whichever of the completion callback or the
    // call's 'error' event fires first (grpc-js can raise both for the same failure).
    // Guarding with `settled` means neither resolve nor reject can ever fire twice.
    let settled = false;
    let resolveEnded!: () => void;
    let rejectEnded!: (error: unknown) => void;
    const ended = new Promise<void>((resolve, reject) => {
      resolveEnded = () => {
        if (!settled) {
          settled = true;
          resolve();
        }
      };
      rejectEnded = (error) => {
        if (!settled) {
          settled = true;
          reject(error instanceof RociaDbError ? error : rpcError("Upload", error));
        }
      };
    });

    const call = this.#services.files.upload(metadata, (error) =>
      error ? rejectEnded(error) : resolveEnded(),
    );
    // A writable stream with no 'error' listener makes Node throw an uncaught exception
    // and can crash the process; the completion callback above already reports failures
    // to the caller, so this listener exists purely to keep the stream from doing that.
    call.on("error", (error) => rejectEnded(error));

    void (async () => {
      try {
        for await (const request of requests) {
          if (settled) return;
          if (!call.write(request)) {
            // Race backpressure against call completion: if the server ends the call
            // early (e.g. an error), 'drain' may never fire and this must not hang.
            await Promise.race([once(call, "drain"), ended]);
          }
        }
        call.end();
      } catch (cause) {
        call.cancel();
        rejectEnded(cause);
      }
    })();

    await ended;
  }

  /**
   * Download file chunks lazily as an async iterable. If the caller stops iterating
   * before the stream ends (a `break`/`return` out of the consuming `for await`, or a
   * thrown error), the underlying gRPC call is cancelled rather than left running.
   */
  async *downloadFileStream(
    tenantId: string,
    bucket: string,
    fileId: string,
  ): AsyncGenerator<Uint8Array> {
    const request: DownloadRequest = { tenant_id: tenantId, bucket, file_id: fileId };
    const call = this.#services.files.download(request, await this.#metadata());
    try {
      for await (const response of call) yield response.chunk;
    } catch (cause) {
      throw rpcError("Download", cause);
    } finally {
      call.cancel();
    }
  }

  /** Download a complete file into memory. Prefer streaming for large objects. */
  async downloadFile(tenantId: string, bucket: string, fileId: string): Promise<Uint8Array> {
    const chunks: Buffer[] = [];
    for await (const chunk of this.downloadFileStream(tenantId, bucket, fileId)) {
      chunks.push(Buffer.from(chunk));
    }
    return Buffer.concat(chunks);
  }

  /** Fetch size, content type, checksum, and timestamps for one file. */
  async statFile(tenantId: string, bucket: string, fileId: string): Promise<FileMetadata> {
    const request: StatRequest = { tenant_id: tenantId, bucket, file_id: fileId };
    const response = await unary("Stat", this.#services.files.stat.bind(this.#services.files), request, await this.#metadata());
    return {
      sizeBytes: parseUint64(response.size_bytes, "file size"),
      contentType: response.content_type,
      checksum: response.checksum,
      createdAt: response.created_at,
      updatedAt: response.updated_at,
    };
  }

  /** Delete one stored file. Reuse `requestId` when retrying the deletion. */
  async deleteFile(
    tenantId: string,
    bucket: string,
    fileId: string,
    requestId = `delete_file:${randomUUID()}`,
  ): Promise<void> {
    const request: DeleteRequest = {
      tenant_id: tenantId,
      bucket,
      file_id: fileId,
      request_id: requestId,
    };
    await unary("Delete", this.#services.files.delete.bind(this.#services.files), request, await this.#metadata());
  }

  /** List the bucket names holding at least one file. */
  async listBuckets(tenantId: string, page: PageOptions = {}): Promise<Page<string>> {
    const request: ListBucketsRequest = { tenant_id: tenantId, page: pageRequest(page) };
    const response = await unary("ListBuckets", this.#services.files.listBuckets.bind(this.#services.files), request, await this.#metadata());
    return { items: response.buckets, nextCursor: optionalCursor(response.page?.next_cursor) };
  }

  /** Return one cursor-based page of file IDs stored in one bucket. */
  async listFiles(tenantId: string, bucket: string, page: PageOptions = {}): Promise<Page<string>> {
    const request: ListFilesRequest = { tenant_id: tenantId, bucket, page: pageRequest(page) };
    const response = await unary("ListFiles", this.#services.files.listFiles.bind(this.#services.files), request, await this.#metadata());
    return { items: response.file_ids, nextCursor: optionalCursor(response.page?.next_cursor) };
  }

  /**
   * List the tenant IDs known to the deployment.
   * This RPC is not scoped to a tenant and may be restricted server-side.
   */
  async listTenants(page: PageOptions = {}): Promise<Page<string>> {
    const request: ListTenantsRequest = { page: pageRequest(page) };
    const response = await unary("ListTenants", this.#services.tenants.listTenants.bind(this.#services.tenants), request, await this.#metadata());
    return { items: response.tenant_ids, nextCursor: optionalCursor(response.page?.next_cursor) };
  }

  async #metadata(): Promise<grpc.Metadata> {
    return this.#tokenManager?.metadata() ?? new grpc.Metadata();
  }

  async #getNeighborNodes<T>(
    direction: NeighborDirection,
    tenantId: string,
    graph: string,
    nodeId: string,
    label: string,
  ): Promise<NeighborNode<T>[]> {
    const neighbors: Neighbor[] = [];
    let cursor: string | undefined;
    do {
      const page = direction === "out"
        ? await this.neighborsOut(tenantId, graph, nodeId, label, { limit: 50, cursor })
        : await this.neighborsIn(tenantId, graph, nodeId, label, { limit: 50, cursor });
      neighbors.push(...page.items);
      if (page.nextCursor === cursor) break;
      cursor = page.nextCursor;
    } while (cursor);
    return mapConcurrent(neighbors, CONCURRENT_REQUESTS, async (neighbor, _index, signal) => ({
      ...neighbor,
      value: await this.getNode<T>(tenantId, graph, neighbor.nodeId, signal),
    }));
  }
}

export function pageRequest(page: PageOptions): { limit: number; cursor: string } {
  const limit = page.limit ?? DEFAULT_PAGE_SIZE;
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new RociaDbError("Page limit must be a positive integer", { kind: "validation" });
  }
  return { limit, cursor: page.cursor ?? "" };
}

function documentPage<T>(response: DocumentListResponse): DocumentPage<T> {
  return {
    items: response.json.map(decodeJson<T>),
    nextCursor: optionalCursor(response.page?.next_cursor),
    totalCount: parseUint64(response.total_count, "document total count"),
  };
}

function neighborPage(response: NeighborsResponse): Page<Neighbor> {
  return {
    items: response.neighbors.map((neighbor) => ({
      nodeId: neighbor.node_id,
      edgeId: neighbor.edge_id,
    })),
    nextCursor: optionalCursor(response.page?.next_cursor),
  };
}

export function endpointFromHost(host: string): {
  address: string;
  credentials: grpc.ChannelCredentials;
} {
  const hasScheme = host.includes("://");
  let url: URL;
  try {
    url = new URL(hasScheme ? host : `http://${host}`);
  } catch (cause) {
    throw new RociaDbError("RociaDB host is not a valid URL", { kind: "connection", cause });
  }
  // WHATWG URL normalizes away a port that matches its scheme's default — e.g.
  // `new URL("https://db.example.com:443").port` is `""`, not `"443"` — even though the
  // caller wrote an explicit port. TLS terminates at a reverse proxy in front of the
  // server, so `https://host:443` is the normal production endpoint; without this
  // fallback it would always be rejected. A port that is empty because none was ever
  // supplied is filled in the same way, from the scheme's own default.
  const port = url.port || (url.protocol === "https:" ? "443" : url.protocol === "http:" ? "80" : "");
  if (!url.hostname || !port || url.pathname !== "/") {
    throw new RociaDbError("RociaDB host must contain only a hostname and port", {
      kind: "connection",
    });
  }
  return {
    address: `${url.hostname}:${port}`,
    credentials: url.protocol === "https:"
      ? grpc.credentials.createSsl()
      : grpc.credentials.createInsecure(),
  };
}

export function validateFileSize(sizeBytes: bigint): void {
  if (sizeBytes < 0n) {
    throw new RociaDbError("File size must not be negative", { kind: "validation" });
  }
  if (sizeBytes > BigInt(MAX_FILE_BYTES)) {
    throw new RociaDbError(
      `File size (${sizeBytes} bytes) exceeds the server's default ${MAX_FILE_BYTES}-byte (5 GiB) limit`,
      { kind: "validation" },
    );
  }
}

export function requireChecksumLength(checksum: Uint8Array): Buffer {
  if (checksum.byteLength !== CHECKSUM_BYTES) {
    throw new RociaDbError(
      `File checksum must be exactly ${CHECKSUM_BYTES} bytes (a SHA-256 digest), got ${checksum.byteLength}`,
      { kind: "validation" },
    );
  }
  return Buffer.from(checksum);
}

/**
 * Compute a SHA-256 digest of `bytes` when the caller supplies no checksum, or validate
 * that a supplied checksum is exactly {@link CHECKSUM_BYTES} bytes (the server rejects
 * any other length). Exported for unit testing; used internally by {@link
 * RociaDbClient.uploadFile}.
 */
export function computeOrValidateChecksum(bytes: Uint8Array, checksum?: Uint8Array): Buffer {
  return checksum === undefined
    ? createHash("sha256").update(bytes).digest()
    : requireChecksumLength(checksum);
}

/**
 * Re-chunk an iterable of arbitrarily-sized byte pieces into messages of exactly
 * {@link UPLOAD_CHUNK_BYTES} (1 MiB) each, except the last message, which carries
 * whatever remains. 1 MiB is the server's per-message ceiling, so this is the fewest
 * messages a file can take, and the only chunking safe against a server older than
 * `1.0.0-rc.16`, which reassembled a download from a guessed chunk count and truncated
 * anything sent in smaller pieces.
 *
 * Validates as it goes: throws before yielding a chunk that would push the running
 * total past `sizeBytes`, and throws once the source is exhausted if the total falls
 * short of `sizeBytes`. An empty source (`sizeBytes` 0n, no bytes at all) still yields
 * exactly one empty chunk, because the server only learns the file's metadata from a
 * message, and an upload that writes nothing would never deliver it.
 *
 * Exported for unit testing; used internally by {@link RociaDbClient.uploadFileStream}.
 */
export async function* rechunkToUploadSize(
  chunks: AsyncIterable<Uint8Array> | Iterable<Uint8Array>,
  sizeBytes: bigint,
): AsyncGenerator<Buffer> {
  let buffer = Buffer.alloc(0);
  let totalWritten = 0n;
  let wroteAny = false;

  const accept = (byteLength: number): void => {
    totalWritten += BigInt(byteLength);
    if (totalWritten > sizeBytes) {
      throw new RociaDbError(
        `uploadFileStream received more data than sizeBytes (${sizeBytes} bytes) declared`,
        { kind: "validation" },
      );
    }
    wroteAny = true;
  };

  for await (const chunk of chunks) {
    buffer = buffer.byteLength === 0
      ? Buffer.from(chunk)
      : Buffer.concat([buffer, Buffer.from(chunk)]);
    while (buffer.byteLength >= UPLOAD_CHUNK_BYTES) {
      const piece = buffer.subarray(0, UPLOAD_CHUNK_BYTES);
      accept(piece.byteLength);
      yield piece;
      buffer = Buffer.from(buffer.subarray(UPLOAD_CHUNK_BYTES));
    }
  }
  // Flush the remainder, and for an empty source (no chunks, sizeBytes 0n) yield one
  // empty chunk anyway: the server only learns the file's metadata from a message, and
  // an upload that writes nothing would never deliver it.
  if (buffer.byteLength > 0 || !wroteAny) {
    accept(buffer.byteLength);
    yield buffer;
  }
  if (totalWritten !== sizeBytes) {
    throw new RociaDbError(
      `uploadFileStream sent ${totalWritten} bytes but sizeBytes declared ${sizeBytes}`,
      { kind: "validation" },
    );
  }
}

export function requireStreamChecksum(checksum: Uint8Array | undefined): Buffer {
  if (checksum === undefined) {
    throw new RociaDbError(
      "uploadFileStream requires a precomputed 32-byte SHA-256 checksum in file.checksum: " +
        "file metadata travels on the first gRPC message, before any chunk has been read " +
        "from the source, so the SDK cannot hash the stream as it goes. Hash the source " +
        "ahead of time, or use uploadFile for data already held in memory.",
      { kind: "validation" },
    );
  }
  return requireChecksumLength(checksum);
}

/**
 * Build the `UploadRequest` message sequence for an assisted, re-chunked upload: only
 * the first message carries the file's metadata (tenant/bucket/file id, size_bytes,
 * content_type, checksum, request_id) — the server only reads those fields off the first
 * message of the stream, so repeating them on every later message would be wasted
 * bandwidth and CPU.
 *
 * Exported for unit testing the exact wire shape produced; used internally by
 * {@link RociaDbClient.uploadFileStream}, which passes in `checksum` and `requestId`
 * already validated/defaulted.
 */
export async function* buildUploadStreamRequests(
  file: FileStreamUpload,
  checksum: Buffer,
  requestId: string,
  chunks: AsyncIterable<Uint8Array> | Iterable<Uint8Array>,
): AsyncGenerator<UploadRequest> {
  let isFirstMessage = true;
  for await (const chunk of rechunkToUploadSize(chunks, file.sizeBytes)) {
    yield isFirstMessage
      ? {
          tenant_id: file.tenantId,
          bucket: file.bucket,
          file_id: file.fileId,
          size_bytes: file.sizeBytes.toString(),
          content_type: file.contentType ?? "application/octet-stream",
          checksum,
          chunk,
          request_id: requestId,
        }
      : {
          tenant_id: "",
          bucket: "",
          file_id: "",
          size_bytes: "0",
          content_type: "",
          checksum: Buffer.alloc(0),
          chunk,
          request_id: "",
        };
    isFirstMessage = false;
  }
}

/**
 * Convert each {@link RawUploadMessage} into the wire `UploadRequest` shape unchanged —
 * no re-chunking, no validation, and no first-message/later-message distinction; every
 * field travels exactly as the caller supplied it, for every message.
 *
 * Exported for unit testing the passthrough behavior; used internally by
 * {@link RociaDbClient.uploadFileRaw}.
 */
export async function* buildRawUploadRequests(
  requests: AsyncIterable<RawUploadMessage> | Iterable<RawUploadMessage>,
): AsyncGenerator<UploadRequest> {
  for await (const message of requests) {
    yield {
      tenant_id: message.tenantId,
      bucket: message.bucket,
      file_id: message.fileId,
      size_bytes: message.sizeBytes.toString(),
      content_type: message.contentType,
      checksum: Buffer.from(message.checksum),
      chunk: Buffer.from(message.chunk),
      request_id: message.requestId,
    };
  }
}

/**
 * Parses a wire-format protobuf `uint64` (transmitted as a decimal string, see
 * `longs: String` in {@link createServiceClients}) into a `bigint`, preserving the full
 * uint64 range. `kind: "decode"` because this is decoding a value out of a response
 * received from upstream, the same bucket as {@link decodeJson}.
 *
 * Exported for unit testing (no gRPC response is needed to exercise the decode-failure
 * branch); used internally by every response reader that carries a wire uint64.
 */
export function parseUint64(value: string, field: string): bigint {
  try {
    return BigInt(value);
  } catch (cause) {
    throw new RociaDbError(`Invalid protobuf uint64 value for ${field}`, { kind: "decode", cause });
  }
}

function credentialsFromEnvironment(): ClientCredentials {
  const tokenUrl = process.env.AUTH_TOKEN_URL;
  const clientId = process.env.AUTH_CLIENT_ID;
  const clientSecret = process.env.AUTH_CLIENT_SECRET;
  if (!tokenUrl || !clientId || !clientSecret) {
    throw new RociaDbError(
      "Missing auth configuration (set AUTH_TOKEN_URL, AUTH_CLIENT_ID, and AUTH_CLIENT_SECRET)",
      { kind: "connection" },
    );
  }
  return { tokenUrl, clientId, clientSecret };
}
