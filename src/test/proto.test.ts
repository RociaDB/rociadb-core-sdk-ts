import assert from "node:assert/strict";
import test from "node:test";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");

async function packagedProto(): Promise<string> {
  return readFile(join(packageRoot, "proto", "upstream", "v1", "upstream.proto"), "utf8");
}

test("the packaged protobuf exposes all twenty-two RPCs", async () => {
  const proto = await packagedProto();
  assert.equal(proto.match(/^\s*rpc\s/gm)?.length, 22);
});

test("the packaged protobuf declares the four services", async () => {
  const proto = await packagedProto();
  const services = proto.match(/^service\s+(\w+)/gm)?.map((line) => line.split(/\s+/)[1]);
  assert.deepEqual(services, ["DocumentService", "GraphService", "FileService", "TenantService"]);
});

test("the packaged protobuf exposes every listing RPC", async () => {
  const proto = await packagedProto();
  for (const rpc of [
    "ListCollections",
    "ListGraphs",
    "ListNodes",
    "ListBuckets",
    "ListFiles",
    "ListTenants",
  ]) {
    assert.match(proto, new RegExp(`^\\s*rpc ${rpc}\\(`, "m"), `missing rpc ${rpc}`);
  }
});
