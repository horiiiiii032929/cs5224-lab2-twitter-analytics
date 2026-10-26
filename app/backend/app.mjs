import express from 'express';
import { timingSafeEqual } from 'node:crypto';
import { active } from './jobs.mjs';

export function createApp({ config, jobs, results }) {
  const app = express();
  app.disable('x-powered-by');
  app.use((req, res, next) => {
    res.set({ 'Content-Security-Policy': "default-src 'self'; script-src 'self'; style-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'", 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer' }); next();
  });
  app.use(express.json({ limit: '4kb' }));
  app.use('/api', (req, res, next) => { res.set('Cache-Control', 'no-store'); next(); });
  const admin = (req, res, next) => {
    const actual = Buffer.from(req.get('authorization') ?? '');
    const expected = Buffer.from(`Bearer ${config.adminToken}`);
    if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) return res.status(401).json({ error: 'A valid operator token is required' });
    if (req.get('sec-fetch-site') === 'cross-site') return res.status(403).json({ error: 'Cross-site job requests are not allowed' });
    next();
  };
  app.get('/api/health', (req, res) => res.json({ status: 'ok', mode: config.mode }));
  app.get('/api/jobs', async (req, res) => res.json({ jobs: await jobs.list() }));
  app.post('/api/jobs', admin, async (req, res) => res.status(202).json(await jobs.start()));
  app.get('/api/jobs/:id', async (req, res) => {
    const job = await jobs.get(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    res.json(job);
  });
  app.delete('/api/jobs/:id', admin, async (req, res) => {
    const job = await jobs.cancel(req.params.id);
    if (!job) return res.status(404).json({ error: 'Job not found' });
    res.json(job);
  });
  async function current(req) {
    if (req.query.jobId && (typeof req.query.jobId !== 'string' || !/^[a-f0-9-]{36}$/.test(req.query.jobId))) throw Object.assign(new Error('Invalid jobId'), { status: 400 });
    const job = req.query.jobId ? await jobs.get(req.query.jobId) : (await jobs.list()).find(j => j.state === 'SUCCEEDED');
    if (job && job.state === 'SUCCEEDED') return { jobId: job.id, ...await results.load(job.outputPrefix) };
    if (!req.query.jobId && config.manualResults) return { jobId: 'manual', ...await results.load(config.manualResults) };
    throw Object.assign(new Error(job && active(job) ? 'Analysis is still running' : 'No completed analysis. Run a job first.'), { status: 409 });
  }
  app.get('/api/summary', async (req, res) => {
    const data = await current(req); res.json({ jobId: data.jobId, ...data.metadata });
  });
  app.get('/api/users/:id', async (req, res) => {
    if (!/^[0-9]{1,30}$/.test(req.params.id)) return res.status(400).json({ error: 'User ID must contain digits only' });
    const data = await current(req); const row = data.users.get(req.params.id);
    if (!row) return res.status(404).json({ error: 'User is not present in this dataset' });
    res.json({ jobId: data.jobId, ...row });
  });
  app.get('/api/users', async (req, res) => {
    const limitText = req.query.limit ?? '50';
    if (typeof limitText !== 'string' || !/^[0-9]+$/.test(limitText) || +limitText < 1 || +limitText > 200 || (req.query.cursor !== undefined && (typeof req.query.cursor !== 'string' || !/^[0-9]{1,30}$/.test(req.query.cursor)))) return res.status(400).json({ error: 'limit must be 1–200; cursor must be a user ID' });
    const data = await current(req);
    const start = req.query.cursor ? data.ids.findIndex(id => id > req.query.cursor) : 0;
    const ids = start < 0 ? [] : data.ids.slice(start, start + +limitText);
    res.json({ jobId: data.jobId, total: data.ids.length, users: ids.map(id => data.users.get(id)), nextCursor: start >= 0 && start + ids.length < data.ids.length ? ids.at(-1) : null });
  });
  app.use(express.static(config.publicDirectory));
  app.use((error, req, res, next) => {
    console.error(error.message);
    res.status(error.status ?? 500).json({ error: error.status ? error.message : 'Operation failed. Check the server log and AWS job status.' });
  });
  return app;
}
