// Independent streaming reference. No Spark or AWS dependencies.
import { createReadStream } from 'node:fs';
import { readdir, readFile } from 'node:fs/promises';
import { createInterface } from 'node:readline';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

export async function verifyCounts(input, output) {
  const edges = new Set(), expected = new Map();
  let valid = 0, blank = 0, total = 0, selfEdges = 0;
  for await (const line of createInterface({ input: createReadStream(input), crlfDelay: Infinity })) {
    total++;
    const parts = line.trim().split(/\s+/);
    if (!line.trim()) { blank++; continue; }
    if (parts.length !== 2 || !parts.every(p => /^[0-9]+$/.test(p))) throw new Error(`Malformed input at line ${total}`);
    valid++;
    const [source, target] = parts, edge = `${source}\t${target}`;
    if (edges.has(edge)) continue;
    edges.add(edge); if (source === target) selfEdges++;
    const a = expected.get(source) ?? { userId: source, followers: 0, followees: 0 };
    a.followees++; expected.set(source, a);
    const b = expected.get(target) ?? { userId: target, followers: 0, followees: 0 };
    b.followers++; expected.set(target, b);
  }
  await Promise.all(['users', 'metadata'].map(dir => readFile(join(output, dir, '_SUCCESS'))));
  const metadataFiles = (await readdir(join(output, 'metadata'))).filter(n => n.startsWith('part-'));
  if (metadataFiles.length !== 1) throw new Error('Expected exactly one metadata part');
  const metadata = JSON.parse(await readFile(join(output, 'metadata', metadataFiles[0]), 'utf8'));
  const seen = new Set();
  for (const part of (await readdir(join(output, 'users'))).filter(n => n.startsWith('part-'))) {
    for await (const line of createInterface({ input: createReadStream(join(output, 'users', part)), crlfDelay: Infinity })) {
      if (!line.trim()) continue;
      const row = JSON.parse(line), reference = expected.get(row.userId);
      if (!reference || seen.has(row.userId) || row.followers !== reference.followers || row.followees !== reference.followees) throw new Error(`Count mismatch or duplicate output: ${row.userId}`);
      seen.add(row.userId);
    }
  }
  if (seen.size !== expected.size) throw new Error('Output is missing users');
  const summary = { totalLines: total, validRows: valid, blankRows: blank, invalidRows: 0, duplicateRows: valid - edges.size, uniqueEdges: edges.size, selfEdges, users: expected.size, totalFollowers: edges.size, totalFollowees: edges.size };
  for (const [key, value] of Object.entries(summary)) if (metadata[key] !== value) throw new Error(`Metadata mismatch: ${key}`);
  return { ...summary, requiredUsers: Object.fromEntries(['214328887', '107830991'].filter(id => expected.has(id)).map(id => [id, expected.get(id)])), allUsersVerified: true };
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  if (!process.argv[2] || !process.argv[3]) throw new Error('Usage: node scripts/verify-cloud.mjs <input.txt> <spark-output-directory>');
  console.log(JSON.stringify(await verifyCounts(process.argv[2], process.argv[3]), null, 2));
}
