import { fileURLToPath } from 'node:url';
import { resolve, join } from 'node:path';
import { access } from 'node:fs/promises';
import { createApp } from './app.mjs';
import { JobStore, ResultsStore } from './storage.mjs';
import { JobManager } from './jobs.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const mode = process.env.MODE ?? 'local';
if (!['local', 'aws'].includes(mode)) throw new Error('MODE must be local or aws');
const config = {
  root, mode, publicDirectory: join(root, 'public'), region: process.env.AWS_REGION ?? 'ap-southeast-1',
  adminToken: process.env.ADMIN_TOKEN ?? (mode === 'local' ? 'local-lab2' : ''),
  resultsDirectory: resolve(process.env.VAR_DIR ?? join(root, 'var')),
  input: resolve(process.env.INPUT_FILE ?? join(root, '../twitter_combined.txt')),
  python: resolve(process.env.PYTHON_BIN ?? join(root, '.venv/bin/python')),
  script: join(root, 'analysis/twitter_counts.py'),
  bucket: process.env.DATA_BUCKET, release: process.env.EMR_RELEASE ?? 'emr-7.10.0',
  serviceRole: process.env.EMR_SERVICE_ROLE, instanceProfile: process.env.EMR_INSTANCE_PROFILE,
  subnetId: process.env.EMR_SUBNET_ID, primarySg: process.env.EMR_PRIMARY_SG, coreSg: process.env.EMR_CORE_SG,
  instanceType: process.env.EMR_INSTANCE_TYPE ?? 'm5.xlarge',
};
if (!config.adminToken) throw new Error('ADMIN_TOKEN is required in AWS mode');
if (mode === 'aws') for (const key of ['bucket', 'serviceRole', 'instanceProfile', 'subnetId', 'primarySg', 'coreSg']) if (!config[key]) throw new Error(`Missing AWS configuration: ${key}`);
if (mode === 'local') {
  try { await access(join(config.resultsDirectory, 'results/manual/metadata/_SUCCESS')); config.manualResults = 'results/manual'; } catch {}
}
const store = new JobStore(join(config.resultsDirectory, 'jobs'));
const results = new ResultsStore({ ...config, directory: config.resultsDirectory });
const jobs = new JobManager(config, store); await jobs.init();
const app = createApp({ config, jobs, results });
const host = process.env.HOST ?? (mode === 'local' ? '127.0.0.1' : '0.0.0.0');
const server = app.listen(Number(process.env.PORT ?? 3000), host, () => console.log(`Lab 2 (${mode}): http://${host}:${server.address().port}`));
