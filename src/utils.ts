import type { Metadata, ServiceError, status } from "@grpc/grpc-js";
import { RociaDbError } from "./types.js";

export const DEFAULT_PAGE_SIZE = 20;
export const CONCURRENT_REQUESTS = 10;

export function encodeJson(value: unknown): Buffer {
  try {
    return Buffer.from(JSON.stringify(value));
  } catch (cause) {
    throw new RociaDbError("Failed to encode JSON", { cause });
  }
}

export function decodeJson<T>(value: Uint8Array): T {
  try {
    return JSON.parse(Buffer.from(value).toString("utf8")) as T;
  } catch (cause) {
    throw new RociaDbError("Failed to decode JSON", { cause });
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

export function unary<Request, Response>(
  operation: string,
  method: (
    request: Request,
    metadata: Metadata,
    callback: (error: ServiceError | null, response?: Response) => void,
  ) => unknown,
  request: Request,
  metadata: Metadata,
): Promise<Response> {
  return new Promise((resolve, reject) => {
    method(request, metadata, (error, response) => {
      if (error) {
        reject(rpcError(operation, error));
      } else if (response === undefined) {
        reject(new RociaDbError(`${operation} returned no response`));
      } else {
        resolve(response);
      }
    });
  });
}

export async function mapConcurrent<Input, Output>(
  values: readonly Input[],
  concurrency: number,
  mapper: (value: Input, index: number) => Promise<Output>,
): Promise<Output[]> {
  const output = new Array<Output>(values.length);
  let next = 0;
  let failed = false;
  let firstError: unknown;
  async function worker(): Promise<void> {
    while (!failed && next < values.length) {
      const index = next++;
      try {
        output[index] = await mapper(values[index]!, index);
      } catch (error) {
        // Stop this worker from claiming further indices, and let every other worker's
        // `while` check see `failed` before it starts another operation. In-flight calls
        // already claimed before this point are allowed to finish (they're caught the
        // same way), so nothing here is left unhandled.
        if (!failed) {
          failed = true;
          firstError = error;
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
