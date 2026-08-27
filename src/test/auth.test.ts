import assert from "node:assert/strict";
import test from "node:test";
import { TokenManager } from "../auth.js";

test("TokenManager fetches and caches an OAuth token", async () => {
  let calls = 0;
  const mockFetch: typeof fetch = async () => {
    calls += 1;
    return new Response(
      JSON.stringify({ access_token: "token", expires_in: 3600, token_type: "Bearer" }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  };
  const manager = new TokenManager(
    { tokenUrl: "https://auth.example/token", clientId: "id", clientSecret: "secret" },
    mockFetch,
  );

  await manager.initialize();
  const first = await manager.metadata();
  const second = await manager.metadata();

  assert.deepEqual(first.get("authorization"), ["Bearer token"]);
  assert.deepEqual(second.get("authorization"), ["Bearer token"]);
  assert.equal(calls, 1);
});
