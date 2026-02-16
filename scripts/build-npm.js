#!/usr/bin/env node

"use strict";

const fs = require("fs");
const path = require("path");

const version = process.argv[2];
if (!version) {
  console.error("Usage: node scripts/build-npm.js <version>");
  process.exit(1);
}

const PLATFORMS = [
  { dir: "pgplan-linux-x64", name: "linux-x64", os: ["linux"], cpu: ["x64"], bin: "pgplan" },
  { dir: "pgplan-linux-arm64", name: "linux-arm64", os: ["linux"], cpu: ["arm64"], bin: "pgplan" },
  { dir: "pgplan-darwin-x64", name: "darwin-x64", os: ["darwin"], cpu: ["x64"], bin: "pgplan" },
  { dir: "pgplan-darwin-arm64", name: "darwin-arm64", os: ["darwin"], cpu: ["arm64"], bin: "pgplan" },
  { dir: "pgplan-win32-x64", name: "win32-x64", os: ["win32"], cpu: ["x64"], bin: "pgplan.exe" },
];

const npmDir = path.join(__dirname, "..", "npm");

for (const p of PLATFORMS) {
  const pkgDir = path.join(npmDir, p.dir);
  fs.mkdirSync(pkgDir, { recursive: true });

  const pkg = {
    name: `@pgplan/${p.name}`,
    version,
    description: `pgplan binary for ${p.name}`,
    repository: {
      type: "git",
      url: "git+https://github.com/JacobArthurs/pgplan.git",
    },
    author: "Jacob Arthurs",
    license: "MIT",
    preferUnplugged: true,
    os: p.os,
    cpu: p.cpu,
    files: [p.bin],
  };

  fs.writeFileSync(
    path.join(pkgDir, "package.json"),
    JSON.stringify(pkg, null, 2) + "\n"
  );
  console.log(`Generated ${p.dir}/package.json`);
}

const rootPkgPath = path.join(npmDir, "pgplan", "package.json");
const rootPkg = JSON.parse(fs.readFileSync(rootPkgPath, "utf8"));
rootPkg.version = version;

for (const key of Object.keys(rootPkg.optionalDependencies)) {
  rootPkg.optionalDependencies[key] = version;
}

fs.writeFileSync(rootPkgPath, JSON.stringify(rootPkg, null, 2) + "\n");
console.log(`Updated pgplan/package.json to ${version}`);