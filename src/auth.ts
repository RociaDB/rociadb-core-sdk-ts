import { Metadata } from "@grpc/grpc-js";
import type { ClientCredentials } from "./types.js";
import { RociaDbError } from "./types.js";

interface TokenResponse {
  access_token: string;
  expires_in: number;
  token_type: string;
}

type FetchLike = typeof fetch;

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
      await this.#refreshToken();
    }
    const metadata = new Metadata();
    metadata.set("authorization", this.#authorization!);
    return metadata;
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
    const body = new URLSearchParams({
      grant_type: "client_credentials",
      client_id: this.#config.clientId,
      client_secret: this.#config.clientSecret,
    });
    let response: Response;
    try {
      response = await this.#fetch(this.#config.tokenUrl, {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body,
      });
    } catch (cause) {
      throw new RociaDbError("OAuth token request failed", { cause });
    }
    if (!response.ok) {
      throw new RociaDbError(`OAuth token endpoint returned ${response.status}`);
    }
    let token: Partial<TokenResponse>;
    try {
      token = (await response.json()) as Partial<TokenResponse>;
    } catch (cause) {
      throw new RociaDbError("OAuth token response is not valid JSON", { cause });
    }
    if (!token.access_token || !token.token_type || typeof token.expires_in !== "number") {
      throw new RociaDbError("OAuth token response is invalid");
    }
    this.#authorization = `${token.token_type} ${token.access_token}`;
    this.#expiresAt = Date.now() + token.expires_in * 1_000;
  }
}
