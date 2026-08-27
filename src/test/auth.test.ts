import assert from "node:assert/strict";
import test from "node:test";
import { fetchOAuthToken, TokenManager } from "../auth.js";
import { RociaDbError } from "../types.js";

const CONFIG = { tokenUrl: "https://auth.example/token", clientId: "id", clientSecret: "secret" };

/** Matches a rejected {@link RociaDbError} whose wrapped `cause` carries `message`. */
function rejectedWithCause(message: string): (error: unknown) => boolean {
  return (error: unknown) => {
    assert.ok(error instanceof RociaDbError);
    assert.equal(error.kind, "auth");
    assert.ok(error.cause instanceof Error, "expected the network error to be preserved as .cause");
    assert.equal(error.cause.message, message);
    return true;
  };
}

function tokenResponse(overrides: Partial<Record<string, unknown>> = {}): Response {
  const body = { access_token: "token", expires_in: 3600, token_type: "Bearer", ...overrides };
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

test("TokenManager fetches and caches an OAuth token", async () => {
  let calls = 0;
  const mockFetch: typeof fetch = async () => {
    calls += 1;
    return tokenResponse();
  };
  const manager = new TokenManager(CONFIG, mockFetch);

  await manager.initialize();
  const first = await manager.metadata();
  const second = await manager.metadata();

  assert.deepEqual(first.get("authorization"), ["Bearer token"]);
  assert.deepEqual(second.get("authorization"), ["Bearer token"]);
  assert.equal(calls, 1);
});

test("TokenManager.invalidate(): the next metadata() call fetches a fresh token instead of reusing the cache", async () => {
  let calls = 0;
  const mockFetch: typeof fetch = async () => {
    calls += 1;
    return tokenResponse({ access_token: `token-${calls}` });
  };
  const manager = new TokenManager(CONFIG, mockFetch);

  await manager.initialize();
  assert.deepEqual((await manager.metadata()).get("authorization"), ["Bearer token-1"]);
  assert.equal(calls, 1);

  manager.invalidate();
  assert.deepEqual((await manager.metadata()).get("authorization"), ["Bearer token-2"]);
  assert.equal(calls, 2);
});

test("fetchOAuthToken: exchanges client credentials for an access token (success case)", async () => {
  let requestBody: string | undefined;
  let requestHeaders: Headers | undefined;
  const mockFetch: typeof fetch = async (_url, init) => {
    requestBody = String(init?.body);
    requestHeaders = new Headers(init?.headers);
    return tokenResponse({ access_token: "abc123", expires_in: 120, token_type: "Bearer" });
  };

  const token = await fetchOAuthToken(CONFIG, mockFetch);

  assert.deepEqual(token, { accessToken: "abc123", tokenType: "Bearer", expiresIn: 120 });
  assert.equal(requestHeaders?.get("content-type"), "application/x-www-form-urlencoded");
  const params = new URLSearchParams(requestBody);
  assert.equal(params.get("grant_type"), "client_credentials");
  assert.equal(params.get("client_id"), "id");
  assert.equal(params.get("client_secret"), "secret");
});

test("fetchOAuthToken: a network failure is wrapped as kind \"auth\", preserving the cause", async () => {
  const networkError = new Error("getaddrinfo ENOTFOUND");
  const mockFetch: typeof fetch = async () => {
    throw networkError;
  };

  await assert.rejects(fetchOAuthToken(CONFIG, mockFetch), (error: unknown) => {
    assert.ok(error instanceof RociaDbError);
    assert.equal(error.kind, "auth");
    assert.equal(error.cause, networkError);
    return true;
  });
});

test("fetchOAuthToken: a non-2xx response is wrapped as kind \"auth\" and names the status", async () => {
  const mockFetch: typeof fetch = async () =>
    new Response("unauthorized", { status: 401 });

  await assert.rejects(fetchOAuthToken(CONFIG, mockFetch), (error: unknown) => {
    assert.ok(error instanceof RociaDbError);
    assert.equal(error.kind, "auth");
    assert.match(error.message, /401/);
    return true;
  });
});

test("fetchOAuthToken: a non-JSON response body is wrapped as kind \"auth\"", async () => {
  const mockFetch: typeof fetch = async () =>
    new Response("not json", { status: 200, headers: { "content-type": "text/plain" } });

  await assert.rejects(fetchOAuthToken(CONFIG, mockFetch), (error: unknown) => {
    assert.ok(error instanceof RociaDbError);
    assert.equal(error.kind, "auth");
    assert.match(error.message, /not valid JSON/);
    return true;
  });
});

test("fetchOAuthToken: a response missing a required field is rejected as kind \"auth\"", async () => {
  for (const overrides of [
    { access_token: undefined },
    { token_type: undefined },
    { expires_in: "soon" },
  ]) {
    const mockFetch: typeof fetch = async () => tokenResponse(overrides);
    await assert.rejects(fetchOAuthToken(CONFIG, mockFetch), (error: unknown) => {
      assert.ok(error instanceof RociaDbError);
      assert.equal(error.kind, "auth");
      assert.match(error.message, /invalid/);
      return true;
    });
  }
});

test("TokenManager.refreshNow(): unconditionally refetches, ignoring the cached token and the skew margin", async () => {
  let calls = 0;
  const mockFetch: typeof fetch = async () => {
    calls += 1;
    // A long expiry and the default 30s skew mean metadata() alone would never refresh
    // again here — refreshNow() must still hit the network unconditionally.
    return tokenResponse({ access_token: `token-${calls}`, expires_in: 3600 });
  };
  const manager = new TokenManager(CONFIG, mockFetch);

  await manager.initialize();
  assert.equal(calls, 1);
  assert.deepEqual((await manager.metadata()).get("authorization"), ["Bearer token-1"]);

  await manager.refreshNow();
  assert.equal(calls, 2, "refreshNow() must hit the network even though the cached token is nowhere near expiry");
  assert.deepEqual((await manager.metadata()).get("authorization"), ["Bearer token-2"]);
});

test("TokenManager.refreshNow(): a failing refresh rejects without discarding the still-cached token", async () => {
  let calls = 0;
  const mockFetch: typeof fetch = async () => {
    calls += 1;
    if (calls === 1) return tokenResponse({ access_token: "good-token", expires_in: 3600 });
    throw new Error("token endpoint is down");
  };
  const manager = new TokenManager(CONFIG, mockFetch);
  await manager.initialize();

  await assert.rejects(manager.refreshNow(), rejectedWithCause("token endpoint is down"));

  // The failed refreshNow() must not have overwritten the cached header: metadata()
  // still serves the original token without triggering another network call (the cached
  // token is nowhere near its 3600s expiry).
  assert.deepEqual((await manager.metadata()).get("authorization"), ["Bearer good-token"]);
  assert.equal(calls, 2);
});

test(
  "TokenManager.metadata(): a refresh failure within the skew margin falls back to the " +
    "still-valid cached token instead of failing the in-flight RPC (regression test for the " +
    "old behavior, which propagated the error and discarded a token that was not actually expired)",
  async () => {
    let calls = 0;
    const mockFetch: typeof fetch = async () => {
      calls += 1;
      if (calls === 1) {
        // Expires in 1 real second; the default 30s refresh skew means the very next
        // metadata() call already falls inside the "should refresh" window even though
        // the token has not actually expired yet.
        return tokenResponse({ access_token: "still-valid", expires_in: 1 });
      }
      throw new Error("token endpoint hiccup");
    };
    const manager = new TokenManager(CONFIG, mockFetch);
    await manager.initialize();
    assert.equal(calls, 1);

    const warnings: unknown[][] = [];
    const originalWarn = console.warn;
    console.warn = (...args: unknown[]) => warnings.push(args);
    try {
      const metadata = await manager.metadata();
      assert.deepEqual(metadata.get("authorization"), ["Bearer still-valid"]);
    } finally {
      console.warn = originalWarn;
    }

    assert.equal(calls, 2, "metadata() must have attempted (and failed) a refresh");
    assert.equal(warnings.length, 1, "the swallowed refresh failure must be surfaced as a warning");
    assert.match(String(warnings[0]?.[0]), /reusing the still-valid cached token/);
  },
);

test(
  "TokenManager.metadata(): once the cached token is genuinely expired, a failing refresh " +
    "still throws instead of serving the stale token",
  async () => {
    let calls = 0;
    const mockFetch: typeof fetch = async () => {
      calls += 1;
      if (calls === 1) return tokenResponse({ access_token: "will-expire", expires_in: 0 });
      throw new Error("token endpoint hiccup");
    };
    const manager = new TokenManager(CONFIG, mockFetch);
    await manager.initialize();
    assert.equal(calls, 1);

    // expires_in: 0 means #expiresAt was set to (roughly) Date.now() at fetch time; a
    // short real sleep guarantees Date.now() has moved strictly past it.
    await new Promise((resolve) => setTimeout(resolve, 20));

    await assert.rejects(manager.metadata(), rejectedWithCause("token endpoint hiccup"));
    assert.equal(calls, 2);
  },
);

test("TokenManager.metadata(): the very first refresh has no cached token to fall back to and propagates its failure", async () => {
  const mockFetch: typeof fetch = async () => {
    throw new Error("token endpoint unreachable");
  };
  const manager = new TokenManager(CONFIG, mockFetch);

  await assert.rejects(manager.initialize(), rejectedWithCause("token endpoint unreachable"));
  await assert.rejects(manager.metadata(), rejectedWithCause("token endpoint unreachable"));
});
