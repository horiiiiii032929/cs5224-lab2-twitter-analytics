import { mkdir, readFile, readdir, writeFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import { S3Client, GetObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';

export class JobStore {
  constructor(directory) { this.directory = directory; }
  async init() { await mkdir(this.directory, { recursive: true }); }
  async list() {
    const names = (await readdir(this.directory)).filter(n => n.endsWith('.json'));
    const rows = await Promise.all(names.map(n => readFile(join(this.directory, n), 'utf8').then(JSON.parse)));
    return rows.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
  }
  async get(id) {
    if (!/^[a-f0-9-]{36}$/.test(id)) return undefined;
    try { return JSON.parse(await readFile(join(this.directory, `${id}.json`), 'utf8')); }
    catch (error) { if (error.code === 'ENOENT') return undefined; throw error; }
  }
  async save(job) {
    const dest = join(this.directory, `${job.id}.json`);
    await writeFile(`${dest}.tmp`, JSON.stringify(job, null, 2));
    await rename(`${dest}.tmp`, dest);
  }
}

export class ResultsStore {
  constructor({ mode, bucket, directory, region }) {
    Object.assign(this, { mode, bucket, directory });
    this.s3 = mode === 'aws' ? new S3Client({ region }) : undefined;
    this.cache = new Map();
  }
  async read(key) {
    if (this.mode === 'local') return readFile(join(this.directory, key), 'utf8');
    const result = await this.s3.send(new GetObjectCommand({ Bucket: this.bucket, Key: key }));
    return result.Body.transformToString();
  }
  async parts(prefix) {
    if (this.mode === 'local') return (await readdir(join(this.directory, prefix))).filter(n => n.startsWith('part-')).map(n => `${prefix}/${n}`).sort();
    const keys = []; let token;
    do {
      const page = await this.s3.send(new ListObjectsV2Command({ Bucket: this.bucket, Prefix: `${prefix}/`, ContinuationToken: token }));
      keys.push(...(page.Contents ?? []).map(o => o.Key).filter(k => k.split('/').at(-1).startsWith('part-')));
      token = page.NextContinuationToken;
    } while (token);
    return keys.sort();
  }
  async load(prefix) {
    if (!this.cache.has(prefix)) {
      const loading = this.loadUncached(prefix).catch(error => { this.cache.delete(prefix); throw error; });
      this.cache.set(prefix, loading);
      // Small lab dataset: cache one completed run, not every historical run.
      for (const key of this.cache.keys()) if (key !== prefix) this.cache.delete(key);
    }
    return this.cache.get(prefix);
  }
  async loadUncached(prefix) {
    await Promise.all([this.read(`${prefix}/users/_SUCCESS`), this.read(`${prefix}/metadata/_SUCCESS`)]);
    const metadataParts = await this.parts(`${prefix}/metadata`);
    if (metadataParts.length !== 1) throw new Error('Expected one metadata part');
    const metadata = JSON.parse((await this.read(metadataParts[0])).trim());
    const users = new Map(); let followers = 0; let followees = 0;
    // Read each distributed part in turn; do not expose S3 or credentials to browsers.
    for (const key of await this.parts(`${prefix}/users`)) {
      for (const line of (await this.read(key)).split('\n').filter(Boolean)) {
        const row = JSON.parse(line);
        if (!/^[0-9]+$/.test(row.userId) || !Number.isSafeInteger(row.followers) || !Number.isSafeInteger(row.followees) || row.followers < 0 || row.followees < 0 || users.has(row.userId)) throw new Error('Invalid result row');
        users.set(row.userId, row); followers += row.followers; followees += row.followees;
      }
    }
    if (users.size !== metadata.users || followers !== metadata.uniqueEdges || followees !== metadata.uniqueEdges) throw new Error('Result integrity check failed');
    return { metadata, users, ids: [...users.keys()].sort() };
  }
}
