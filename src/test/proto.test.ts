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

test("package.json exposes the raw generated protobuf types under a \"./proto\" subpath export", async () => {
  const pkg = JSON.parse(await readFile(join(packageRoot, "package.json"), "utf8")) as {
    exports: Record<string, { types?: string; import?: string } | string>;
  };
  const protoExport = pkg.exports["./proto"];
  assert.ok(protoExport && typeof protoExport === "object", "\"./proto\" must be an exports subpath entry");
  assert.equal(protoExport.types, "./dist/proto.d.ts");
  assert.equal(protoExport.import, "./dist/proto.js");
});

test("the \"./proto\" subpath export's build outputs actually exist after `npm run build`", async () => {
  // A dangling exports entry that points at files the build never produces would break
  // for every consumer of `@rocia/rociadb-sdk/proto`; this fails loudly instead.
  await assert.doesNotReject(readFile(join(packageRoot, "dist", "proto.js"), "utf8"));
  await assert.doesNotReject(readFile(join(packageRoot, "dist", "proto.d.ts"), "utf8"));
});
