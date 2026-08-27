import { Metadata } from "@grpc/grpc-js";
import type { ClientCredentials } from "./types.js";
import { RociaDbError } from "./types.js";

interface TokenResponse {
  access_token: string;
  expires_in: number;
  token_type: string;
}

/** Fields extracted from a successful OAuth2 client-credentials token response. */
export interface OAuthToken {
  accessToken: string;
  tokenType: string;
  expiresIn: number;
}

type FetchLike = typeof fetch;

/**
 * Fetch one OAuth2 client-credentials token. Exported standalone for callers who want the
 * raw token exchange without the caching and background-refresh behavior of
 * {@link TokenManager} — `new TokenManager(config).initialize()` already covers that in
 * practice, so reach for this only when you need the token exchange in isolation.
 */
export async function fetchOAuthToken(
  config: ClientCredentials,
  fetchImplementation: FetchLike = fetch,
): Promise<OAuthToken> {
  const body = new URLSearchParams({
    grant_type: "client_credentials",
    client_id: config.clientId,
    client_secret: config.clientSecret,
  });
  let response: Response;
  try {
    response = await fetchImplementation(config.tokenUrl, {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body,
    });
  } catch (cause) {
    throw new RociaDbError("OAuth token request failed", { kind: "auth", cause });
  }
  if (!response.ok) {
    throw new RociaDbError(`OAuth token endpoint returned ${response.status}`, { kind: "auth" });
  }
  let token: Partial<TokenResponse>;
  try {
    token = (await response.json()) as Partial<TokenResponse>;
  } catch (cause) {
    throw new RociaDbError("OAuth token response is not valid JSON", { kind: "auth", cause });
  }
  if (!token.access_token || !token.token_type || typeof token.expires_in !== "number") {
    throw new RociaDbError("OAuth token response is invalid", { kind: "auth" });
  }
  return {
    accessToken: token.access_token,
    tokenType: token.token_type,
    expiresIn: token.expires_in,
  };
}

/** Fetches, caches, and refreshes OAuth2 client-credentials bearer tokens. */
export class TokenManager {
  readonly #config: ClientCredentials;
  readonly #fetch: FetchLike;
  #authorization?: string;
  #expiresAt = 0;
  #refresh?: Promise<void>;

  constructor(config: ClientCredentials, fetchImplementation: FetchLike = fetch) {
    this.#config = config;
    this.#fetch = fetchImplementation;
  }

  /** Fetch the first token, failing early before the gRPC client is returned. */
  async initialize(): Promise<void> {
    await this.#refreshToken();
  }

  /** Return authorization metadata, refreshing the token near expiration. */
  async metadata(): Promise<Metadata> {
    const skew = this.#config.refreshSkewMs ?? 30_000;
    if (!this.#authorization || Date.now() + skew >= this.#expiresAt) {
      try {
        await this.#refreshToken();
      } catch (error) {
        // Only within the skew margin, not actually expired yet: a refresh hiccup here
        // must not fail the in-flight RPC when a still-valid cached token could serve it
        // instead, so the cached header is only ever overwritten on success. A token that
        // never fetched, or one that is genuinely past its expiry, has nothing usable to
        // fall back to, so the error still propagates in that case.
        if (this.#authorization !== undefined && Date.now() < this.#expiresAt) {
          console.warn(
            `RociaDB auth token refresh failed; reusing the still-valid cached token: ${String(error)}`,
          );
        } else {
          throw error;
        }
      }
    }
    const metadata = new Metadata();
    metadata.set("authorization", this.#authorization!);
    return metadata;
  }

  /**
   * Force an immediate, unconditional token refresh, ignoring both the cached token and
   * the refresh skew margin.
   *
   * Call this after catching a {@link RociaDbError} whose `reason` is `"unauthenticated"`
   * (or `code` is `grpc.status.UNAUTHENTICATED`), before retrying: the server treats that
   * status as a renewal signal. The cached token is replaced only on success — a failed
   * refresh here throws without discarding whatever token is currently cached.
   */
  async refreshNow(): Promise<void> {
    const token = await fetchOAuthToken(this.#config, this.#fetch);
    this.#authorization = `${token.tokenType} ${token.accessToken}`;
    this.#expiresAt = Date.now() + token.expiresIn * 1_000;
  }

  /**
   * Drop the cached token so the next {@link metadata} call fetches a fresh one.
   *
   * The server treats `UNAUTHENTICATED` as a renewal signal (unlike `PERMISSION_DENIED`,
   * which is final and calling this will not fix). Tokens are only valid for 600 seconds
   * server-side, so a clock skew, an early server-side revocation, or any other cause of
   * an `UNAUTHENTICATED` response before this SDK's own expiry timer would fire should
   * call this before retrying, rather than reusing the still-cached (but now-rejected)
   * token.
   */
  invalidate(): void {
    this.#authorization = undefined;
    this.#expiresAt = 0;
  }

  async #refreshToken(): Promise<void> {
    if (this.#refresh) {
      return this.#refresh;
    }
    this.#refresh = this.#fetchToken();
    try {
      await this.#refresh;
    } finally {
      this.#refresh = undefined;
    }
  }

  async #fetchToken(): Promise<void> {
    await this.refreshNow();
  }
}
