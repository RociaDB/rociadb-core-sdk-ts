import type { ClientUnaryCall, Metadata, ServiceError, status } from "@grpc/grpc-js";
import { RociaDbError } from "./types.js";

export const DEFAULT_PAGE_SIZE = 20;
export const CONCURRENT_REQUESTS = 10;

export function encodeJson(value: unknown): Buffer {
  try {
    return Buffer.from(JSON.stringify(value));
  } catch (cause) {
    throw new RociaDbError("Failed to encode JSON", { kind: "encode", cause });
  }
}

export function decodeJson<T>(value: Uint8Array): T {
  try {
    return JSON.parse(Buffer.from(value).toString("utf8")) as T;
  } catch (cause) {
    throw new RociaDbError("Failed to decode JSON", { kind: "decode", cause });
  }
}

export function optionalCursor(cursor: string | undefined): string | undefined {
  return cursor ? cursor : undefined;
}

export function rpcError(operation: string, cause: unknown): RociaDbError {
  // `cause` is whatever the failed call handed back; grpc-js normally supplies a
  // ServiceError, but callers (and tests) may pass null/undefined/anything else, so this
  // must not assume an object is present before reading its fields.
  const serviceError = (cause ?? undefined) as Partial<ServiceError> | undefined;
  return new RociaDbError(`${operation} failed`, {
    kind: "status",
    cause,
    code: typeof serviceError?.code === "number" ? (serviceError.code as status) : undefined,
    reason: extractReason(serviceError?.metadata),
  });
}

/** Read the server's `reason` trailing metadata (see RociaDbError.reason). */
function extractReason(metadata: Metadata | null | undefined): string | undefined {
  const value = metadata?.get("reason")?.[0];
  return typeof value === "string" ? value : undefined;
}

/**
 * Call a unary gRPC method and resolve/reject with its result.
 *
 * `signal`, when supplied, cancels the in-flight gRPC call as soon as it aborts (see
 * {@link mapConcurrent}, which drives this from a batch's shared `AbortController` so a
 * failure in one item of a batch actively cancels the others' already-dispatched calls
 * rather than merely stopping new ones from being scheduled).
 */
export function unary<Request, Response>(
  operation: string,
  method: (
    request: Request,
    metadata: Metadata,
    callback: (error: ServiceError | null, response?: Response) => void,
  ) => ClientUnaryCall,
  request: Request,
  metadata: Metadata,
  signal?: AbortSignal,
): Promise<Response> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new RociaDbError(`${operation} aborted`, { kind: "status" }));
      return;
    }
    let call: ClientUnaryCall | undefined;
    const onAbort = (): void => call?.cancel();
    signal?.addEventListener("abort", onAbort, { once: true });
    call = method(request, metadata, (error, response) => {
      signal?.removeEventListener("abort", onAbort);
      if (error) {
        reject(rpcError(operation, error));
      } else if (response === undefined) {
        reject(new RociaDbError(`${operation} returned no response`, { kind: "status" }));
      } else {
        resolve(response);
      }
    });
  });
}

/**
 * Run `mapper` over `values` with at most `concurrency` calls in flight at once.
 *
 * As soon as one call rejects, every other in-flight call is actively cancelled — not
 * merely prevented from being followed by new ones — via the `AbortSignal` passed to
 * `mapper`, which callers (see `RociaDbClient.putNodes`/`addEdges`/the neighbor-node
 * helpers) must thread down to {@link unary}. This mirrors the Rust SDK's
 * `try_for_each_concurrent`, which drops in-flight futures on the first error: retrying
 * is always safe because every write here carries an idempotency key, so cancelling
 * already-dispatched requests is a pure reduction in wasted server-side work, never a
 * correctness risk.
 */
export async function mapConcurrent<Input, Output>(
  values: readonly Input[],
  concurrency: number,
  mapper: (value: Input, index: number, signal: AbortSignal) => Promise<Output>,
): Promise<Output[]> {
  const output = new Array<Output>(values.length);
  let next = 0;
  let failed = false;
  let firstError: unknown;
  const controller = new AbortController();
  async function worker(): Promise<void> {
    while (!failed && next < values.length) {
      const index = next++;
      try {
        output[index] = await mapper(values[index]!, index, controller.signal);
      } catch (error) {
        // Stop this worker from claiming further indices, let every other worker's
        // `while` check see `failed` before it starts another operation, and cancel
        // every call already in flight so the server stops doing work for a batch that
        // is going to be reported as failed anyway.
        if (!failed) {
          failed = true;
          firstError = error;
          controller.abort();
        }
        return;
      }
    }
  }
  await Promise.all(
    Array.from({ length: Math.min(concurrency, values.length) }, () => worker()),
  );
  if (failed) throw firstError;
  return output;
}
