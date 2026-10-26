import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createApp } from '../backend/app.mjs';
import { ResultsStore } from '../backend/storage.mjs';

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'lab2-api-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  for (const folder of ['users', 'metadata']) { await mkdir(join(directory, 'output', folder), { recursive: true }); await writeFile(join(directory, 'output', folder, '_SUCCESS'), ''); }
  const rows = [{ userId: '1', followers: 0, followees: 2 }, { userId: '2', followers: 1, followees: 0 }, { userId: '3', followers: 1, followees: 0 }];
  await writeFile(join(directory, 'output/users/part-00000'), rows.map(JSON.stringify).join('\n') + '\n');
  await writeFile(join(directory, 'output/metadata/part-00000'), JSON.stringify({ users: 3, uniqueEdges: 2 }));
  const config = { mode: 'local', adminToken: 'test-token', publicDirectory: directory, manualResults: 'output' };
  let starts = 0;
  const jobs = { list: async () => [], get: async () => undefined, start: async () => { starts++; return { id: 'new-job' }; }, cancel: async () => undefined };
  const results = new ResultsStore({ mode: 'local', directory });
  const server = createApp({ config, jobs, results }).listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  t.after(() => new Promise(resolve => { server.close(resolve); server.closeAllConnections(); }));
  return { url: `http://127.0.0.1:${server.address().port}`, starts: () => starts, directory };
}

test('API counts, zero degrees, pagination, missing users and validation', async t => {
  const { url } = await fixture(t);
  const get = path => fetch(url + path);
  assert.deepEqual(await (await get('/api/users/1')).json(), { jobId: 'manual', userId: '1', followers: 0, followees: 2 });
  const first = await (await get('/api/users?limit=2')).json();
  assert.equal(first.nextCursor, '2'); assert.equal(first.total, 3);
  const second = await (await get('/api/users?limit=2&cursor=2')).json();
  assert.deepEqual(second.users.map(u => u.userId), ['3']); assert.equal(second.nextCursor, null);
  assert.equal((await get('/api/users/999')).status, 404);
  for (const path of ['/api/users?limit=201', '/api/users?limit=1e2', '/api/users?cursor=bad', '/api/users/abc', '/api/summary?jobId=../../etc']) assert.equal((await get(path)).status, 400, path);
});

test('job creation requires operator token and blocks cross-site requests', async t => {
  const { url, starts } = await fixture(t);
  assert.equal((await fetch(url + '/api/jobs', { method: 'POST' })).status, 401);
  assert.equal((await fetch(url + '/api/jobs', { method: 'POST', headers: { Authorization: 'Bearer test-token', 'Sec-Fetch-Site': 'cross-site' } })).status, 403);
  assert.equal((await fetch(url + '/api/jobs', { method: 'POST', headers: { Authorization: 'Bearer test-token' } })).status, 202);
  assert.equal(starts(), 1);
});

test('partial Spark output is not served as successful results', async t => {
  const { url, directory } = await fixture(t);
  await rm(join(directory, 'output/users/_SUCCESS'));
  assert.equal((await fetch(url + '/api/summary')).status, 500);
});
