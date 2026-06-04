#!/usr/bin/env node

import { readFileSync, writeFileSync } from 'node:fs';

const dryRun =
  process.argv.includes('--dry-run') ||
  process.env.NX_DRY_RUN === 'true' ||
  process.env.npm_config_dry_run === 'true';

const packageJson = JSON.parse(readFileSync('package.json', 'utf8'));
const projectJson = JSON.parse(readFileSync('project.json', 'utf8'));
const nextVersion = packageJson.version;

const updates = (projectJson.release?.syncFiles ?? []).map(({ path, match, replace }) => ({
  filePath: path,
  match: new RegExp(match, 'm'),
  replace: replace.replaceAll('{version}', nextVersion)
}));

const pendingWrites = updates.flatMap(({ filePath, match, replace }) => {
  const current = readFileSync(filePath, 'utf8');

  if (!match.test(current)) {
    throw new Error(`Could not find version pattern in ${filePath}`);
  }

  const next = current.replace(match, replace);
  return current === next ? [] : [{ filePath, contents: next }];
});

if (pendingWrites.length === 0) {
  console.log('Release manifests are already in sync.');
  process.exit(0);
}

if (dryRun) {
  console.log('Dry run: the following release manifests would be updated:');
  for (const { filePath } of pendingWrites) {
    console.log(`- ${filePath}`);
  }
  process.exit(0);
}

for (const { filePath, contents } of pendingWrites) {
  writeFileSync(filePath, contents);
  console.log(`Updated ${filePath}`);
}
