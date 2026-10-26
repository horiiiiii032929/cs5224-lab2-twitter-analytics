import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { JobManager, clusterRequest } from '../backend/jobs.mjs';
import { JobStore } from '../backend/storage.mjs';

const config = { mode: 'aws', region: 'ap-southeast-1', bucket: 'lab-data', release: 'emr-7.10.0', instanceType: 'm5.xlarge', serviceRole: 'service-role', instanceProfile: 'profile', subnetId: 'subnet', primarySg: 'sg-primary', coreSg: 'sg-core' };
test('EMR request processes S3 data with YARN and shuts down after steps', () => {
  const request = clusterRequest(config, { id: 'test', outputPrefix: 'results/test' });
  assert.equal(request.Instances.KeepJobFlowAliveWhenNoSteps, false);
  assert.equal(request.Instances.InstanceGroups[1].InstanceCount, 2);
  const args = request.Steps[0].HadoopJarStep.Args;
  assert.deepEqual(args.slice(0, 5), ['spark-submit', '--master', 'yarn', '--deploy-mode', 'cluster']);
  assert.ok(args.includes('s3://lab-data/input/twitter_combined.txt'));
  assert.ok(args.includes('s3://lab-data/results/test'));
  assert.equal(request.Steps[0].ActionOnFailure, 'TERMINATE_CLUSTER');
});

test('EMR lifecycle is persisted and concurrent submissions are rejected', async t => {
  const directory = await mkdtemp(join(tmpdir(), 'lab2-jobs-')); t.after(() => rm(directory, { recursive: true, force: true }));
  const store = new JobStore(directory); const manager = new JobManager(config, store); await manager.init();
  let launches = 0; let completed = false;
  manager.emr = { send: async command => {
    switch (command.constructor.name) {
      case 'RunJobFlowCommand': launches++; return { JobFlowId: 'j-test' };
      case 'ListStepsCommand': return { Steps: [{ Id: 's-test', Name: 'Deduplicate directed edges and count degrees', Status: { State: completed ? 'COMPLETED' : 'RUNNING' } }] };
      case 'DescribeClusterCommand': return { Cluster: { Status: { State: 'RUNNING' } } };
      default: throw new Error('Unexpected command');
    }
  } };
  const launched = await manager.start(); assert.equal(launched.clusterId, 'j-test');
  await assert.rejects(manager.start(), error => error.status === 409); assert.equal(launches, 1);
  assert.equal((await manager.get(launched.id)).state, 'RUNNING');
  completed = true;
  assert.equal((await manager.get(launched.id)).state, 'SUCCEEDED');
  assert.equal((await store.get(launched.id)).stepId, 's-test');
});
