/**
 * Generated protobuf request/response shapes and gRPC service client interfaces.
 *
 * This module is **not** part of the package's semver contract: it is a thin reflection
 * of `proto/upstream/v1/upstream.proto` and can be reshaped by a routine
 * `@grpc/proto-loader` upgrade without the rest of the SDK's own API changing. It is
 * exported under the `rocia-db-sdk/proto` subpath so a caller who needs to build a custom
 * gRPC client (bypassing {@link RociaDbClient}) can do so without duplicating the
 * `.proto` loading logic — depend on it only if you accept that its shapes can change
 * between any two versions of this package.
 */
import * as grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

export interface Empty {}
export interface PageRequest { limit: number; cursor: string }
export interface PageResponse { next_cursor: string }
export interface PutDocRequest { tenant_id: string; collection: string; id: string; json: Buffer; request_id: string }
export interface GetDocRequest { tenant_id: string; collection: string; id: string }
export interface GetDocResponse { json: Buffer }
export interface DeleteDocRequest extends GetDocRequest { request_id: string }
export interface FindByFieldRequest { tenant_id: string; collection: string; field: string; value_json: Buffer; page: PageRequest }
export interface ListDocRequest { tenant_id: string; collection: string; page: PageRequest }
export interface QueryFilterRequest { field: string; operator: number; values_json: Buffer[] }
export interface QuerySortRequest { field: string; direction: number }
export interface QueryDocRequest { tenant_id: string; collection: string; filters: QueryFilterRequest[]; sort: QuerySortRequest[]; page: PageRequest }
export interface DocumentListResponse { json: Buffer[]; page?: PageResponse; total_count: string }
export interface TenantPageRequest { tenant_id: string; page: PageRequest }
export interface ListCollectionsRequest extends TenantPageRequest {}
export interface CollectionInfoResponse { collection: string; count: string }
export interface ListCollectionsResponse { collections: CollectionInfoResponse[]; page?: PageResponse }
export interface ListGraphsRequest extends TenantPageRequest {}
export interface ListGraphsResponse { graphs: string[]; page?: PageResponse }
export interface ListNodesRequest { tenant_id: string; graph: string; page: PageRequest }
export interface ListNodesResponse { node_ids: string[]; page?: PageResponse }
export interface ListBucketsRequest extends TenantPageRequest {}
export interface ListBucketsResponse { buckets: string[]; page?: PageResponse }
export interface ListFilesRequest { tenant_id: string; bucket: string; page: PageRequest }
export interface ListFilesResponse { file_ids: string[]; page?: PageResponse }
export interface ListTenantsRequest { page: PageRequest }
export interface ListTenantsResponse { tenant_ids: string[]; page?: PageResponse }
export interface PutNodeRequest { tenant_id: string; graph: string; node_id: string; json: Buffer; request_id: string }
export interface GetNodeRequest { tenant_id: string; graph: string; node_id: string }
export interface GetNodeResponse { json: Buffer }
export interface AddEdgeRequest { tenant_id: string; graph: string; edge_id: string; from: string; to: string; label: string; json: Buffer; request_id: string }
export interface DeleteEdgeRequest { tenant_id: string; graph: string; edge_id: string; request_id: string }
export interface NeighborsOutRequest { tenant_id: string; graph: string; from: string; label: string; page: PageRequest }
export interface NeighborsInRequest { tenant_id: string; graph: string; to: string; label: string; page: PageRequest }
export interface NeighborResponse { node_id: string; edge_id: string }
export interface NeighborsResponse { neighbors: NeighborResponse[]; page?: PageResponse }
export interface UploadRequest { tenant_id: string; bucket: string; file_id: string; size_bytes: string; content_type: string; checksum: Buffer; chunk: Buffer; request_id: string }
export interface DownloadRequest { tenant_id: string; bucket: string; file_id: string }
export interface DownloadResponse { chunk: Buffer }
export interface StatRequest extends DownloadRequest {}
export interface StatResponse { size_bytes: string; content_type: string; checksum: Buffer; created_at: string; updated_at: string }
export interface DeleteRequest extends DownloadRequest { request_id: string }

type UnaryMethod<Request, Response> = (
  request: Request,
  metadata: grpc.Metadata,
  callback: (error: grpc.ServiceError | null, response?: Response) => void,
) => grpc.ClientUnaryCall;

export interface DocumentServiceClient extends grpc.Client {
  putDoc: UnaryMethod<PutDocRequest, Empty>;
  getDoc: UnaryMethod<GetDocRequest, GetDocResponse>;
  deleteDoc: UnaryMethod<DeleteDocRequest, Empty>;
  findByField: UnaryMethod<FindByFieldRequest, DocumentListResponse>;
  listDoc: UnaryMethod<ListDocRequest, DocumentListResponse>;
  queryDoc: UnaryMethod<QueryDocRequest, DocumentListResponse>;
  listCollections: UnaryMethod<ListCollectionsRequest, ListCollectionsResponse>;
}

export interface GraphServiceClient extends grpc.Client {
  putNode: UnaryMethod<PutNodeRequest, Empty>;
  getNode: UnaryMethod<GetNodeRequest, GetNodeResponse>;
  addEdge: UnaryMethod<AddEdgeRequest, Empty>;
  deleteEdge: UnaryMethod<DeleteEdgeRequest, Empty>;
  neighborsOut: UnaryMethod<NeighborsOutRequest, NeighborsResponse>;
  neighborsIn: UnaryMethod<NeighborsInRequest, NeighborsResponse>;
  listGraphs: UnaryMethod<ListGraphsRequest, ListGraphsResponse>;
  listNodes: UnaryMethod<ListNodesRequest, ListNodesResponse>;
}

export interface FileServiceClient extends grpc.Client {
  upload(
    metadata: grpc.Metadata,
    callback: (error: grpc.ServiceError | null, response?: Empty) => void,
  ): grpc.ClientWritableStream<UploadRequest>;
  download(request: DownloadRequest, metadata: grpc.Metadata): grpc.ClientReadableStream<DownloadResponse>;
  stat: UnaryMethod<StatRequest, StatResponse>;
  delete: UnaryMethod<DeleteRequest, Empty>;
  listBuckets: UnaryMethod<ListBucketsRequest, ListBucketsResponse>;
  listFiles: UnaryMethod<ListFilesRequest, ListFilesResponse>;
}

export interface TenantServiceClient extends grpc.Client {
  listTenants: UnaryMethod<ListTenantsRequest, ListTenantsResponse>;
}

interface ServiceConstructor<T extends grpc.Client> {
  new(address: string, credentials: grpc.ChannelCredentials): T;
}

interface RociaPackage {
  rocia: {
    v1: {
      DocumentService: ServiceConstructor<DocumentServiceClient>;
      GraphService: ServiceConstructor<GraphServiceClient>;
      FileService: ServiceConstructor<FileServiceClient>;
      TenantService: ServiceConstructor<TenantServiceClient>;
    };
  };
}

export function createServiceClients(
  address: string,
  credentials: grpc.ChannelCredentials,
): {
  documents: DocumentServiceClient;
  graph: GraphServiceClient;
  files: FileServiceClient;
  tenants: TenantServiceClient;
} {
  const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
  const protoRoot = join(packageRoot, "proto");
  const definition = loadSync(join(protoRoot, "upstream", "v1", "upstream.proto"), {
    keepCase: true,
    longs: String,
    enums: Number,
    defaults: true,
    oneofs: true,
    includeDirs: [protoRoot],
  });
  const services = grpc.loadPackageDefinition(definition) as unknown as RociaPackage;
  return {
    documents: new services.rocia.v1.DocumentService(address, credentials),
    graph: new services.rocia.v1.GraphService(address, credentials),
    files: new services.rocia.v1.FileService(address, credentials),
    tenants: new services.rocia.v1.TenantService(address, credentials),
  };
}
