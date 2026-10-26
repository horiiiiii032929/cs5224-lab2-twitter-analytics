import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { createWriteStream } from 'node:fs';
import { join } from 'node:path';
import { EMRClient, RunJobFlowCommand, ListStepsCommand, DescribeClusterCommand, TerminateJobFlowsCommand } from '@aws-sdk/client-emr';

export const active = job => ['PENDING', 'RUNNING'].includes(job.state);

export function clusterRequest(config, job) {
  return {
    Name: `CS5224-Lab2-${job.id}`,
    ReleaseLabel: config.release,
    Applications: [{ Name: 'Spark' }],
    ServiceRole: config.serviceRole,
    JobFlowRole: config.instanceProfile,
    LogUri: `s3://${config.bucket}/logs/`,
    VisibleToAllUsers: true,
    Tags: [{ Key: 'Project', Value: 'CS5224-Lab2' }, { Key: 'for-use-with-amazon-emr-managed-policies', Value: 'true' }],
    Instances: {
      Ec2SubnetId: config.subnetId,
      EmrManagedMasterSecurityGroup: config.primarySg,
      EmrManagedSlaveSecurityGroup: config.coreSg,
      KeepJobFlowAliveWhenNoSteps: false,
      TerminationProtected: false,
      InstanceGroups: [
        { Name: 'Primary', InstanceRole: 'MASTER', InstanceType: config.instanceType, InstanceCount: 1, Market: 'ON_DEMAND' },
        { Name: 'Workers', InstanceRole: 'CORE', InstanceType: config.instanceType, InstanceCount: 2, Market: 'ON_DEMAND' },
      ],
    },
    Configurations: [{ Classification: 'spark-defaults', Properties: { 'spark.eventLog.enabled': 'true', 'spark.eventLog.dir': `s3://${config.bucket}/spark-events/`, 'spark.dynamicAllocation.enabled': 'false', 'spark.executor.instances': '2', 'spark.executor.cores': '2', 'spark.executor.memory': '6g' } }],
    Steps: [{
      Name: 'Deduplicate directed edges and count degrees',
      ActionOnFailure: 'TERMINATE_CLUSTER',
      HadoopJarStep: { Jar: 'command-runner.jar', Args: [
        'spark-submit', '--master', 'yarn', '--deploy-mode', 'cluster',
        's3://' + config.bucket + '/scripts/twitter_counts.py',
        '--input', 's3://' + config.bucket + '/input/twitter_combined.txt',
        '--output', 's3://' + config.bucket + '/' + job.outputPrefix,
        '--partitions', '8',
      ] },
    }],
  };
}

export class JobManager {
  constructor(config, store) {
    this.config = config; this.store = store; this.starting = false;
    this.emr = config.mode === 'aws' ? new EMRClient({ region: config.region }) : undefined;
    this.children = new Map();
    this.locks = new Map();
  }
  async init() {
    await this.store.init();
    for (const job of await this.store.list()) {
      if (active(job) && (this.config.mode === 'local' || !job.clusterId)) {
        job.state = 'FAILED'; job.error = 'Server restarted before execution could be recovered. Check EMR for any cluster whose launch response was lost.';
        await this.store.save(job);
      }
    }
  }
  async start() {
    if (this.starting) throw Object.assign(new Error('A job is already being started'), { status: 409 });
    this.starting = true;
    try {
      if ((await this.list()).some(active)) throw Object.assign(new Error('An analysis job is already active'), { status: 409 });
      const job = { id: randomUUID(), mode: this.config.mode, state: 'PENDING', createdAt: new Date().toISOString() };
      job.outputPrefix = `results/${job.id}`;
      await this.store.save(job);
      try {
        if (this.emr) {
          const response = await this.emr.send(new RunJobFlowCommand(clusterRequest(this.config, job)));
          job.clusterId = response.JobFlowId;
          await this.store.save(job);
        } else {
          await this.launchLocal(job);
        }
      } catch (error) {
        job.state = 'FAILED'; job.error = error.message; await this.store.save(job); throw error;
      }
      return job;
    } finally { this.starting = false; }
  }
  async launchLocal(job) {
    job.state = 'RUNNING'; await this.store.save(job);
    const log = createWriteStream(join(this.store.directory, `${job.id}.log`));
    const child = spawn(this.config.python, [this.config.script, '--input', this.config.input, '--output', join(this.config.resultsDirectory, job.outputPrefix), '--master', 'local[2]', '--partitions', '4'], {
      cwd: this.config.root,
      env: { ...process.env, SPARK_LOCAL_IP: '127.0.0.1', PYSPARK_PYTHON: this.config.python },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout.pipe(log, { end: false }); child.stderr.pipe(log, { end: false });
    this.children.set(job.id, child);
    let done = false;
    const finish = async (state, error) => {
      if (done) return; done = true; clearTimeout(timer); this.children.delete(job.id); log.end();
      // Preserve explicit cancellation.
      const latest = await this.store.get(job.id);
      if (latest?.state === 'CANCELLED') return;
      job.state = state; job.finishedAt = new Date().toISOString();
      if (error) job.error = error;
      await this.store.save(job);
    };
    const timer = setTimeout(() => child.kill('SIGTERM'), 30 * 60 * 1000).unref();
    child.once('error', error => { finish('FAILED', error.message).catch(console.error); });
    child.once('close', (code, signal) => { finish(code === 0 ? 'SUCCEEDED' : 'FAILED', code === 0 ? undefined : `Spark exited (${signal ?? code}); see var/jobs/${job.id}.log`).catch(console.error); });
  }
  async get(id) {
    // Serialise per-job refreshes/cancellation to keep atomic file writes ordered.
    const previous = this.locks.get(id) ?? Promise.resolve();
    const next = previous.catch(() => {}).then(() => this.refresh(id));
    this.locks.set(id, next);
    try { return await next; } finally { if (this.locks.get(id) === next) this.locks.delete(id); }
  }
  async refresh(id) {
    const job = await this.store.get(id);
    if (!job || !active(job) || !this.emr || !job.clusterId) return job;
    const [steps, cluster] = await Promise.all([
      this.emr.send(new ListStepsCommand({ ClusterId: job.clusterId })),
      this.emr.send(new DescribeClusterCommand({ ClusterId: job.clusterId })),
    ]);
    const step = steps.Steps?.find(s => s.Name === 'Deduplicate directed edges and count degrees');
    job.clusterState = cluster.Cluster?.Status?.State;
    job.stepId = step?.Id; job.stepState = step?.Status?.State;
    if (job.stepState === 'COMPLETED') job.state = 'SUCCEEDED';
    else if (['FAILED', 'CANCELLED', 'INTERRUPTED'].includes(job.stepState) || ['TERMINATED', 'TERMINATED_WITH_ERRORS'].includes(job.clusterState)) {
      job.state = 'FAILED'; job.error = step?.Status?.FailureDetails?.Message ?? cluster.Cluster?.Status?.StateChangeReason?.Message ?? 'EMR execution failed';
    } else if (job.stepState === 'RUNNING') job.state = 'RUNNING';
    if (!active(job)) job.finishedAt = new Date().toISOString();
    await this.store.save(job); return job;
  }
  async list() { return Promise.all((await this.store.list()).map(job => this.get(job.id))); }
  async cancel(id) {
    await this.get(id);
    const previous = this.locks.get(id) ?? Promise.resolve();
    const operation = previous.catch(() => {}).then(async () => {
      const job = await this.store.get(id);
      if (!job || !active(job)) return job;
      if (this.emr && job.clusterId) await this.emr.send(new TerminateJobFlowsCommand({ JobFlowIds: [job.clusterId] }));
      job.state = 'CANCELLED'; job.finishedAt = new Date().toISOString();
      await this.store.save(job);
      this.children.get(id)?.kill('SIGTERM');
      return job;
    });
    this.locks.set(id, operation);
    try { return await operation; } finally { if (this.locks.get(id) === operation) this.locks.delete(id); }
  }
}
