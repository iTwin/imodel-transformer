/*---------------------------------------------------------------------------------------------
 * Copyright (c) Bentley Systems, Incorporated. All rights reserved.
 * See LICENSE.md in the project root for license terms and full copyright notice.
 *--------------------------------------------------------------------------------------------*/

"use strict";

const crypto = require("node:crypto");
const fs = require("node:fs");
const path = require("node:path");

const outputGroups = [
  {
    name: "transformer",
    relativePath: "packages/transformer/lib/cjs",
    kind: "package",
  },
  {
    name: "test-utils",
    relativePath: "packages/test-utils/lib",
    kind: "package",
  },
  {
    name: "performance-scripts",
    relativePath: "packages/performance-scripts/lib",
    kind: "package",
  },
  { name: "test-app", relativePath: "packages/test-app/lib", kind: "package" },
  { name: "api", relativePath: "common/api", kind: "api" },
];

function usage() {
  console.error(
    [
      "Usage: node scripts/compare-build-output.js --baseline <root> --candidate <root> [--report <file>] [--fail-on-javascript-diff]",
      "",
      "The comparison requires identical emitted file inventories and unchanged API reports and public transformer declarations. JavaScript and source-map differences are reported for review because compiler upgrades can change helper and mapping output without changing the public API.",
    ].join("\n")
  );
}

function parseArguments(argv) {
  const options = { failOnJavaScriptDiff: false };

  const readValue = (index, argument) => {
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--"))
      throw new Error(`${argument} requires a value`);
    return value;
  };

  for (let index = 0; index < argv.length; ++index) {
    const argument = argv[index];
    switch (argument) {
      case "--":
        break;
      case "--baseline":
        options.baseline = readValue(index, argument);
        ++index;
        break;
      case "--candidate":
        options.candidate = readValue(index, argument);
        ++index;
        break;
      case "--report":
        options.report = readValue(index, argument);
        ++index;
        break;
      case "--fail-on-javascript-diff":
        options.failOnJavaScriptDiff = true;
        break;
      case "--help":
      case "-h":
        usage();
        process.exit(0);
        break;
      default:
        throw new Error(`Unknown argument: ${argument}`);
    }
  }

  if (options.baseline === undefined || options.candidate === undefined) {
    usage();
    throw new Error("Both --baseline and --candidate are required");
  }

  return options;
}

function normalizeRelativePath(relativePath) {
  return relativePath.split(path.sep).join("/");
}

function walkFiles(directory, relative = "") {
  const currentDirectory = path.join(directory, relative);
  const entries = fs.readdirSync(currentDirectory, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const entryRelativePath = path.join(relative, entry.name);
    if (entry.isDirectory())
      files.push(...walkFiles(directory, entryRelativePath));
    else files.push(entryRelativePath);
  }

  return files.sort();
}

function classifyOutputFile(group, relativePath) {
  const normalizedPath = relativePath.toLowerCase();

  if (normalizedPath.endsWith(".tsbuildinfo")) return "ignored";

  if (group.kind === "api") {
    if (normalizedPath.startsWith("temp/")) return "ignored";
    if (/^[^/]+\.api\.md$/.test(normalizedPath)) return "api";
    if (/^summary\/[^/]+\.csv$/.test(normalizedPath)) return "api";
    return "unexpected";
  }

  if (normalizedPath.endsWith(".d.ts"))
    return group.name === "transformer"
      ? "publicTransformerDeclaration"
      : "declaration";
  if (/\.(?:js|cjs|mjs)$/.test(normalizedPath)) return "javascript";
  if (normalizedPath.endsWith(".map")) return "sourceMap";
  return "unexpected";
}

function sha256(contents) {
  return crypto.createHash("sha256").update(contents).digest("hex");
}

function buildManifest(rootDirectory) {
  const root = path.resolve(rootDirectory);
  const files = [];
  const errors = [];

  if (!fs.existsSync(root)) {
    errors.push({ type: "missing-root", path: root });
    return { root, files, errors };
  }

  for (const group of outputGroups) {
    const directory = path.join(root, group.relativePath);
    if (!fs.existsSync(directory)) {
      errors.push({
        type: "missing-output-directory",
        group: group.name,
        path: group.relativePath,
      });
      continue;
    }

    let relativePaths;
    try {
      relativePaths = walkFiles(directory);
    } catch (error) {
      errors.push({
        type: "cannot-read-output-directory",
        group: group.name,
        path: group.relativePath,
        message: error instanceof Error ? error.message : String(error),
      });
      continue;
    }

    for (const relativePath of relativePaths) {
      const normalizedPath = normalizeRelativePath(relativePath);
      const filePath = path.join(directory, relativePath);
      try {
        const contents = fs.readFileSync(filePath);
        const category = classifyOutputFile(group, normalizedPath);
        if (category === "ignored") continue;
        files.push({
          group: group.name,
          category,
          path: `${group.relativePath}/${normalizedPath}`,
          bytes: contents.byteLength,
          sha256: sha256(contents),
        });
      } catch (error) {
        errors.push({
          type: "cannot-read-output-file",
          group: group.name,
          path: `${group.relativePath}/${normalizedPath}`,
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }

  files.sort((left, right) => left.path.localeCompare(right.path));
  return { root, files, errors };
}

function indexManifest(files) {
  return new Map(files.map((file) => [file.path, file]));
}

function compareManifests(baselineFiles, candidateFiles) {
  const baseline = indexManifest(baselineFiles);
  const candidate = indexManifest(candidateFiles);
  const missing = [];
  const extra = [];
  const changed = [];
  const allPaths = new Set([...baseline.keys(), ...candidate.keys()]);

  for (const filePath of [...allPaths].sort()) {
    const baselineFile = baseline.get(filePath);
    const candidateFile = candidate.get(filePath);
    if (baselineFile === undefined) {
      extra.push(filePath);
      continue;
    }
    if (candidateFile === undefined) {
      missing.push(filePath);
      continue;
    }
    if (
      baselineFile.bytes !== candidateFile.bytes ||
      baselineFile.sha256 !== candidateFile.sha256
    ) {
      changed.push({
        path: filePath,
        group: baselineFile.group,
        category: baselineFile.category,
        baseline: {
          bytes: baselineFile.bytes,
          sha256: baselineFile.sha256,
        },
        candidate: {
          bytes: candidateFile.bytes,
          sha256: candidateFile.sha256,
        },
      });
    }
  }

  return { changed, extra, missing };
}

function classifyChanges(changed) {
  const categories = {
    api: [],
    declarations: [],
    javascript: [],
    sourceMaps: [],
    other: [],
  };

  for (const difference of changed) {
    switch (difference.category) {
      case "api":
        categories.api.push(difference);
        break;
      case "publicTransformerDeclaration":
      case "declaration":
        categories.declarations.push(difference);
        break;
      case "javascript":
        categories.javascript.push(difference);
        break;
      case "sourceMap":
        categories.sourceMaps.push(difference);
        break;
      default:
        categories.other.push(difference);
        break;
    }
  }

  return categories;
}

function displayPaths(paths) {
  return paths.length === 0 ? "none" : paths.join(", ");
}

function writeReport(reportPath, report) {
  if (reportPath === undefined) return;
  const absolutePath = path.resolve(reportPath);
  fs.mkdirSync(path.dirname(absolutePath), { recursive: true });
  fs.writeFileSync(absolutePath, `${JSON.stringify(report, null, 2)}\n`);
}

function createReport(options, baselineManifest, candidateManifest) {
  const comparison = compareManifests(
    baselineManifest.files,
    candidateManifest.files
  );
  const categories = classifyChanges(comparison.changed);
  const publicDeclarationDifferences = categories.declarations.filter(
    (difference) => difference.category === "publicTransformerDeclaration"
  );
  const unexpectedFiles = [baselineManifest, candidateManifest]
    .flatMap((manifest) =>
      manifest.files
        .filter((file) => file.category === "unexpected")
        .map((file) => file.path)
    )
    .filter((filePath, index, paths) => paths.indexOf(filePath) === index)
    .sort();
  const errors = [
    ...baselineManifest.errors.map((error) => ({ side: "baseline", ...error })),
    ...candidateManifest.errors.map((error) => ({
      side: "candidate",
      ...error,
    })),
  ];
  const javascriptOrSourceMapDifferences = [
    ...categories.javascript,
    ...categories.sourceMaps,
  ];
  const failures = [];

  if (errors.length !== 0) failures.push("output-read-errors");
  if (comparison.missing.length !== 0 || comparison.extra.length !== 0)
    failures.push("file-inventory");
  if (categories.api.length !== 0) failures.push("api-changes");
  if (publicDeclarationDifferences.length !== 0)
    failures.push("public-transformer-declarations");
  if (categories.other.length !== 0 || unexpectedFiles.length !== 0)
    failures.push("unexpected-output-categories");
  if (
    options.failOnJavaScriptDiff &&
    javascriptOrSourceMapDifferences.length !== 0
  )
    failures.push("javascript-or-source-map-differences");

  return {
    schemaVersion: 1,
    options: {
      failOnJavaScriptDiff: options.failOnJavaScriptDiff,
    },
    baselineFileCount: baselineManifest.files.length,
    candidateFileCount: candidateManifest.files.length,
    inventories: {
      baseline: {
        root: baselineManifest.root,
        files: baselineManifest.files,
        errors: baselineManifest.errors,
      },
      candidate: {
        root: candidateManifest.root,
        files: candidateManifest.files,
        errors: candidateManifest.errors,
      },
    },
    errors,
    missing: comparison.missing,
    extra: comparison.extra,
    changed: comparison.changed,
    categories: {
      api: categories.api.map((difference) => difference.path),
      declarations: categories.declarations.map(
        (difference) => difference.path
      ),
      publicTransformerDeclarations: publicDeclarationDifferences.map(
        (difference) => difference.path
      ),
      javascript: categories.javascript.map((difference) => difference.path),
      sourceMaps: categories.sourceMaps.map((difference) => difference.path),
      other: categories.other.map((difference) => difference.path),
      unexpected: unexpectedFiles,
    },
    gate: {
      fileInventory:
        errors.length === 0 &&
        comparison.missing.length === 0 &&
        comparison.extra.length === 0,
      apiReports: categories.api.length === 0,
      publicDeclarations: publicDeclarationDifferences.length === 0,
      unexpectedOutputCategories:
        unexpectedFiles.length === 0 && categories.other.length === 0,
      javascriptDifferencesReviewed:
        javascriptOrSourceMapDifferences.length === 0,
      javascriptOrSourceMapDifferences:
        !options.failOnJavaScriptDiff ||
        javascriptOrSourceMapDifferences.length === 0,
      passed: failures.length === 0,
      failures,
    },
  };
}

function printComparison(report) {
  console.log(
    `Compared ${report.baselineFileCount} baseline files with ${report.candidateFileCount} candidate files.`
  );
  console.log(`Missing files: ${displayPaths(report.missing)}`);
  console.log(`Extra files: ${displayPaths(report.extra)}`);
  console.log(
    `Unexpected output files: ${displayPaths(report.categories.unexpected)}`
  );
  console.log(`Changed API files: ${displayPaths(report.categories.api)}`);
  console.log(
    `Changed declaration files: ${displayPaths(report.categories.declarations)}`
  );
  console.log(
    `Changed public transformer declarations: ${displayPaths(report.categories.publicTransformerDeclarations)}`
  );
  console.log(
    `Changed JavaScript files: ${displayPaths(report.categories.javascript)}`
  );
  console.log(
    `Changed source maps: ${displayPaths(report.categories.sourceMaps)}`
  );
  console.log(`Changed other files: ${displayPaths(report.categories.other)}`);
  if (report.errors.length !== 0)
    console.log(`Output read errors: ${JSON.stringify(report.errors)}`);
  console.log(`Build output gate: ${report.gate.passed ? "PASS" : "FAIL"}`);
}

function main() {
  const options = parseArguments(process.argv.slice(2));
  const baselineManifest = buildManifest(options.baseline);
  const candidateManifest = buildManifest(options.candidate);
  const report = createReport(options, baselineManifest, candidateManifest);
  writeReport(options.report, report);
  printComparison(report);

  if (!report.gate.passed)
    throw new Error(
      `Build output comparison failed: ${report.gate.failures.join(", ")}`
    );
}

try {
  main();
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
}
