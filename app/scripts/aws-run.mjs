// One-command deployment and EMR verification. Loads local .env; never uploads it.
// Node >=22 and AWS CLI are the only local prerequisites. No SSM plugin required.
import { spawn } from 'node:child_process';
import { access, mkdir, readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { join, resolve } from 'node:path';
import { isIPv4 } from 'node:net';
import { verifyCounts } from './verify-cloud.mjs';

export const ROOT = fileURLToPath(new URL('../', import.meta.url));
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

export function execute(file, args, { cwd, env, stream = false } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(file, args, { cwd, env, stdio: stream ? ['ignore', 'inherit', 'pipe'] : ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    child.stdout?.on('data', chunk => { stdout += chunk; });
    child.stderr?.on('data', chunk => { stderr = (stderr + chunk).slice(-32000); if (stream) process.stderr.write(chunk); });
    child.once('error', error => reject(new Error(`Cannot run ${file}: ${error.message}`)));
    child.once('close', code => code === 0 ? resolve(stdout) : reject(new Error(`${file} failed (${code}): ${stderr.trim().slice(-5000)}`)));
  });
}

export function validateCidr(cidr) {
  const [ip, mask] = cidr.split('/');
  if (!isIPv4(ip) || mask !== '32' || cidr.split('/').length !== 2) throw new Error('LAB2_VIEWER_CIDR must be one IPv4 address followed by /32');
  return cidr;
}

export function emrGroupReferenceRevocations(groups) {
  // EMR adds TCP/UDP/ICMP cross-references that CloudFormation does not own.
  // Remove only references between this stack's tagged EMR groups, after the
  // clusters have terminated. Preserve self-rules, CIDRs and unrelated groups.
  const owned = groups.filter(g =>
    ['EMR primary; no public ingress', 'EMR workers; no public ingress'].includes(g.Description) &&
    g.Tags?.some(t => t.Key === 'Project' && t.Value === 'CS5224-Lab2') &&
    g.Tags?.some(t => t.Key === 'aws:cloudformation:stack-name' && t.Value === 'CS5224Lab2'));
  const ids = new Set(owned.map(g => g.GroupId));
  return owned.flatMap(group => {
    const permissions = (group.IpPermissions ?? []).flatMap(p => {
      const pairs = (p.UserIdGroupPairs ?? []).filter(pair => ids.has(pair.GroupId) && pair.GroupId !== group.GroupId);
      if (!pairs.length) return [];
      return [{ IpProtocol: p.IpProtocol, ...(p.FromPort === undefined ? {} : { FromPort: p.FromPort }), ...(p.ToPort === undefined ? {} : { ToPort: p.ToPort }), UserIdGroupPairs: pairs.map(pair => ({ GroupId: pair.GroupId })) }];
    });
    return permissions.length ? [{ groupId: group.GroupId, permissions }] : [];
  });
}

export async function clearEmrGroupReferences(aws, log = console.log) {
  const { SecurityGroups = [] } = await aws(['ec2', 'describe-security-groups', '--filters', 'Name=tag:Project,Values=CS5224-Lab2', 'Name=tag:aws:cloudformation:stack-name,Values=CS5224Lab2']);
  const removals = emrGroupReferenceRevocations(SecurityGroups);
  for (const { groupId, permissions } of removals) {
    try {
      await aws(['ec2', 'revoke-security-group-ingress', '--group-id', groupId, '--ip-permissions', JSON.stringify(permissions)]);
    } catch (error) {
      if (!/InvalidPermission.NotFound|InvalidGroup.NotFound/.test(error.message)) throw error;
    }
  }
  if (removals.length) log(`Removed EMR-added cross-references from ${removals.length} lab security groups.`);
  return removals;
}

export function remoteStartScript() {
  // The token remains inside EC2. SSM command output only contains a job record.
  const js = `
    const request = async (path, options) => {
      const response = await fetch('http://127.0.0.1:3000' + path, {...options, signal: AbortSignal.timeout(180000)});
      const body = await response.json();
      if (!response.ok) throw new Error(body.error ?? ('HTTP ' + response.status));
      return body;
    };
    const {jobs} = await request('/api/jobs');
    const previous = jobs.find(j => ['PENDING','RUNNING'].includes(j.state)) ?? jobs.find(j => j.state === 'SUCCEEDED');
    const job = previous ?? await request('/api/jobs', {method:'POST', headers:{Authorization:'Bearer ' + process.env.ADMIN_TOKEN}});
    console.log('LAB2_JOB=' + JSON.stringify(job));
  `;
  const quote = s => "'" + s.replaceAll("'", "'\\''") + "'";
  return [
    'set -eu',
    'ready=0',
    'for attempt in $(seq 1 120); do',
    '  if curl -fsS http://127.0.0.1:3000/api/health >/dev/null 2>&1; then ready=1; break; fi',
    '  sleep 5',
    'done',
    'if [ "$ready" != 1 ]; then echo "Web app did not start. Inspect /var/log/cloud-init-output.log and docker logs lab2." >&2; exit 1; fi',
    `sudo docker exec lab2 node --input-type=module -e ${quote(js)}`,
  ].join('\n');
}

async function browserOpen(url, env) {
  if (env.LAB2_AUTO_OPEN === 'false') return;
  const file = process.platform === 'darwin' ? 'open' : 'xdg-open';
  try { await execute(file, [url], { env }); } catch { /* URL is always printed too. */ }
}

export function credentialEnvironment(env) {
  const result = { ...env };
  // An explicitly selected profile wins over stale pasted credentials in .env.
  if (result.AWS_PROFILE) for (const key of ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN', 'AWS_SECURITY_TOKEN']) delete result[key];
  return result;
}

export async function authenticateAws({ aws, run, env, log }) {
  try { return await aws(['sts', 'get-caller-identity']); }
  catch (error) {
    if (!/ExpiredToken|InvalidClientTokenId|SSO|Token has expired|Unable to locate credentials/i.test(error.message)) throw error;
    // Never choose a different account/profile automatically. Refresh only the
    // selected profile, and only if it is actually configured for SSO.
    const profile = env.AWS_PROFILE || env.AWS_DEFAULT_PROFILE || 'default';
    let sso = false;
    for (const key of ['sso_session', 'sso_start_url']) {
      try { if ((await run('aws', ['configure', 'get', key, '--profile', profile])).trim()) { sso = true; break; } }
      catch { /* Missing profile field is normal for static credentials. */ }
    }
    if (!sso || env.AWS_ACCESS_KEY_ID) throw new Error(`AWS authentication expired or is unavailable. Refresh the credentials in .env, or configure an IAM Identity Center profile once with aws configure sso and set AWS_PROFILE. No resources were changed. (${error.message})`);
    log(`Sign in through the browser for AWS profile ${profile}; no access-key copying is needed.`);
    await run('aws', ['sso', 'login', '--profile', profile], true);
    return aws(['sts', 'get-caller-identity']);
  }
}

export async function runOneShot({ workflow = runWorkflow, ...options } = {}) {
  let authenticated = false, result, failure;
  try {
    result = await workflow({ ...options, command: 'up', onAuthenticated: () => { authenticated = true; } });
  } catch (error) { failure = error; }
  if (authenticated) {
    try { await workflow({ ...options, command: 'down' }); }
    catch (cleanupError) {
      throw new AggregateError([failure, cleanupError].filter(Boolean), `Automatic cleanup failed: ${cleanupError.message}${failure ? `; run failed: ${failure.message}` : ''}. Run npm run aws:down after resolving the error.`);
    }
  }
  if (failure) throw failure;
  (options.log ?? console.log)('One-shot run complete. Results remain in S3 and var/aws-run/; the web stack has been removed.');
  return result;
}

export async function runWorkflow({ root = ROOT, env = process.env, command = 'up', exec = execute, fetcher = fetch, pause = sleep, log = console.log, openBrowser = browserOpen, verify = verifyCounts, onAuthenticated = () => {} } = {}) {
  const region = env.AWS_REGION || env.AWS_DEFAULT_REGION || 'ap-southeast-1';
  if (!/^[a-z]{2}(?:-[a-z]+)+-\d+$/.test(region)) throw new Error('Invalid AWS_REGION');
  const childEnv = { ...credentialEnvironment(env), AWS_REGION: region, AWS_DEFAULT_REGION: region, CDK_DEFAULT_REGION: region, AWS_PAGER: '', AWS_CLI_AUTO_PROMPT: 'off' };
  const run = (file, args, stream = false) => exec(file, args, { cwd: root, env: childEnv, stream });
  const aws = async args => {
    const text = await run('aws', [...args, '--region', region, '--output', 'json']);
    return text.trim() ? JSON.parse(text) : {};
  };
  const npm = args => run('npm', args, true);
  const evidence = join(root, 'var/aws-run');
  await mkdir(evidence, { recursive: true });
  await run('aws', ['--version']);
  const identity = await authenticateAws({ aws, run, env: childEnv, log });
  if (!/^\d{12}$/.test(identity.Account ?? '')) throw new Error('AWS did not return a valid account');
  if (env.LAB2_EXPECTED_ACCOUNT && identity.Account !== env.LAB2_EXPECTED_ACCOUNT) throw new Error(`AWS account ${identity.Account} does not match LAB2_EXPECTED_ACCOUNT; no resources were changed.`);
  onAuthenticated(identity);
  childEnv.CDK_DEFAULT_ACCOUNT = identity.Account;
  log(`AWS account ${identity.Account} · ${region}`);

  if (command === 'down') {
    log('Stopping this lab’s EMR clusters before removing the web stack…');
    let marker;
    const owned = [];
    do {
      const page = await aws(['emr', 'list-clusters', '--cluster-states', 'STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING', 'TERMINATING', ...(marker ? ['--marker', marker] : [])]);
      for (const item of page.Clusters ?? []) {
        const { Cluster: cluster } = await aws(['emr', 'describe-cluster', '--cluster-id', item.Id]);
        if ((cluster.Tags ?? []).some(t => t.Key === 'Project' && t.Value === 'CS5224-Lab2') && cluster.Name?.startsWith('CS5224-Lab2-')) owned.push(cluster.Id);
      }
      marker = page.Marker;
    } while (marker);
    if (owned.length) {
      await aws(['emr', 'terminate-clusters', '--cluster-ids', ...owned]);
      for (const id of owned) await run('aws', ['emr', 'wait', 'cluster-terminated', '--cluster-id', id, '--region', region]);
    }
    await clearEmrGroupReferences(aws, log);
    try { await access(join(root, 'node_modules/.bin/cdk')); } catch { await npm(['ci']); }
    await npm(['run', 'cdk', '--', 'destroy', 'CS5224Lab2', '--force']);
    log('Web stack removed. S3 data and CDK bootstrap resources are retained; delete them separately when no longer needed.');
    return;
  }
  if (command !== 'up') throw new Error('Use up or down');

  const ip = env.LAB2_VIEWER_CIDR ? undefined : (await (await fetcher('https://checkip.amazonaws.com', { signal: AbortSignal.timeout(15000) })).text()).trim();
  const cidr = validateCidr(env.LAB2_VIEWER_CIDR || `${ip}/32`);
  log(`Deploying the web app for viewer IP ${cidr}. Analysis uses a paid 3-node EMR cluster.`);
  await npm(['ci']);
  await npm(['run', 'cdk', '--', 'bootstrap', `aws://${identity.Account}/${region}`]);
  await npm(['run', 'cdk', '--', 'deploy', 'CS5224Lab2', '--require-approval', 'never', '--outputs-file', 'var/cdk-outputs.json', '-c', `viewerCidr=${cidr}`]);

  // Read authoritative outputs from CloudFormation even when CDK reports no changes.
  const deployed = await aws(['cloudformation', 'describe-stacks', '--stack-name', 'CS5224Lab2']);
  const output = Object.fromEntries((deployed.Stacks?.[0]?.Outputs ?? []).map(o => [o.OutputKey, o.OutputValue]));
  const instance = output.WebInstanceId, url = output.WebUrl;
  if (!/^i-[a-f0-9]+$/.test(instance ?? '') || !/^http:\/\/[a-zA-Z0-9.-]+:3000$/.test(url ?? '') || !output.DataBucket) throw new Error('Expected web instance, URL and bucket outputs are missing');
  await writeFile(join(root, 'var/cdk-outputs.json'), JSON.stringify({ CS5224Lab2: output }, null, 2));
  // Spark's event logger requires its base directory to exist before startup.
  await aws(['s3api', 'put-object', '--bucket', output.DataBucket, '--key', 'spark-events/']);

  log('Waiting for EC2 to register with Systems Manager…');
  let online = false;
  for (let attempt = 0; attempt < 120; attempt++) {
    const state = await aws(['ssm', 'describe-instance-information', '--filters', JSON.stringify([{ Key: 'InstanceIds', Values: [instance] }])]);
    if (state.InstanceInformationList?.some(i => i.PingStatus === 'Online')) { online = true; break; }
    await pause(5000);
  }
  if (!online) throw new Error(`EC2 ${instance} is not online in SSM yet. Rerun this command after inspecting its bootstrap log.`);
  log('Starting or resuming analysis on EC2. No token copying is needed…');
  const sent = await aws(['ssm', 'send-command', '--instance-ids', instance, '--document-name', 'AWS-RunShellScript', '--timeout-seconds', '600', '--parameters', JSON.stringify({ commands: [remoteStartScript()], executionTimeout: ['900'] })]);
  const commandId = sent.Command?.CommandId;
  if (!commandId) throw new Error('SSM did not return a command ID');
  let job;
  for (let attempt = 0; attempt < 210; attempt++) {
    let status;
    try { status = await aws(['ssm', 'get-command-invocation', '--command-id', commandId, '--instance-id', instance]); }
    catch (error) { if (!error.message.includes('InvocationDoesNotExist')) throw error; await pause(5000); continue; }
    if (status.Status === 'Success') {
      const record = status.StandardOutputContent?.split('\n').find(line => line.startsWith('LAB2_JOB='));
      if (!record) throw new Error('SSM completed but did not return a job record');
      job = JSON.parse(record.slice('LAB2_JOB='.length)); break;
    }
    if (!['Pending', 'InProgress', 'Delayed'].includes(status.Status)) throw new Error(`EC2 start failed: ${status.StandardErrorContent || status.Status}`);
    await pause(5000);
  }
  if (!job?.id || !/^j-[A-Za-z0-9]+$/.test(job.clusterId ?? '')) throw new Error('No cloud job returned. Check the SSM command and rerun; existing jobs are reused.');
  await writeFile(join(evidence, 'job.json'), JSON.stringify(job, null, 2));
  log(`Open ${url}\nEMR cluster: ${job.clusterId}`);
  await openBrowser(url, childEnv);

  let previous = '', finished = false, failures = 0;
  for (let attempt = 0; attempt < 360; attempt++) {
    try {
      const response = await fetcher(`${url}/api/jobs/${job.id}`, { signal: AbortSignal.timeout(15000) });
      const status = await response.json();
      if (!response.ok) throw new Error(status.error ?? `HTTP ${response.status}`);
      failures = 0; job = status;
    } catch (error) {
      if (++failures >= 6) throw new Error(`Cannot reach the web app from this IP: ${error.message}. Check LAB2_VIEWER_CIDR and rerun; the existing job is reused.`);
      await pause(10000); continue;
    }
    const progress = `${job.clusterState ?? 'STARTING'} / ${job.stepState ?? job.state}`;
    if (progress !== previous) { log(progress); previous = progress; }
    if (job.state === 'SUCCEEDED') { finished = true; break; }
    if (['FAILED', 'CANCELLED'].includes(job.state)) throw new Error(`Analysis ${job.state}: ${job.error ?? 'see EMR step logs'}`);
    await pause(10000);
  }
  if (!finished) throw new Error('Analysis is still running. Rerun to resume monitoring, or run npm run aws:down to stop and remove it.');

  log('Downloading Spark output and independently checking every user…');
  const directory = join(evidence, job.id, 'spark-output');
  await run('aws', ['s3', 'cp', `s3://${output.DataBucket}/${job.outputPrefix}/`, directory, '--recursive', '--only-show-errors', '--region', region], true);
  const verification = await verify(join(root, '../twitter_combined.txt'), directory);
  for (const id of ['214328887', '107830991']) {
    const user = verification.requiredUsers[id];
    if (!user) throw new Error(`Required user ${id} is missing`);
    log(`${id}: ${user.followers} followers, ${user.followees} followees`);
  }
  await writeFile(join(evidence, 'job.json'), JSON.stringify(job, null, 2));
  await writeFile(join(evidence, 'verification.json'), JSON.stringify({ source: 'AWS EMR', verifiedAt: new Date().toISOString(), clusterId: job.clusterId, ...verification }, null, 2));
  const cluster = await aws(['emr', 'describe-cluster', '--cluster-id', job.clusterId]);
  await writeFile(join(evidence, 'cluster.json'), JSON.stringify(cluster, null, 2));
  log('Waiting for EMR to finish automatic termination…');
  await run('aws', ['emr', 'wait', 'cluster-terminated', '--cluster-id', job.clusterId, '--region', region]);
  await writeFile(join(evidence, 'cluster.json'), JSON.stringify(await aws(['emr', 'describe-cluster', '--cluster-id', job.clusterId]), null, 2));
  log(`PASS: every user verified; EMR terminated. Evidence: var/aws-run/\nThe web EC2 instance remains available at ${url}\nWhen finished, run: npm run aws:down`);
  return verification;
}

export async function main() {
  if (Number(process.versions.node.split('.')[0]) < 22) throw new Error('Node.js 22 or newer is required');
  try { process.loadEnvFile(join(ROOT, '.env')); }
  catch (error) {
    if (error.code !== 'ENOENT') throw error;
    // Existing AWS CLI profiles work without a project-specific credential file.
  }
  const command = process.argv[2] ?? 'run';
  if (command === 'run') await runOneShot();
  else await runWorkflow({ command });
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main().catch(error => {
    const secrets = [process.env.AWS_SECRET_ACCESS_KEY, process.env.AWS_SESSION_TOKEN, process.env.AWS_ACCESS_KEY_ID].filter(Boolean);
    let message = error.message;
    for (const secret of secrets) message = message.replaceAll(secret, '[redacted]');
    console.error(`\n${message}\nIf resources were already created, npm run aws:up resumes the job; npm run aws:down stops this lab's EMR clusters and removes the web stack.`);
    process.exitCode = 1;
  });
}
