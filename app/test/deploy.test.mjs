import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { runWorkflow, remoteStartScript, validateCidr, emrGroupReferenceRevocations, clearEmrGroupReferences, runOneShot, credentialEnvironment, authenticateAws } from '../scripts/aws-run.mjs';
import { verifyCounts } from '../scripts/verify-cloud.mjs';

const id = 'e8012031-2d91-469f-8fe2-abf7325d9575';
const url = 'http://ec2-example.compute.amazonaws.com:3000';
const initial = { id, clusterId: 'j-MOCK1', outputPrefix: `results/${id}`, state: 'PENDING' };

test('one-shot returns verified results after running cleanup', async () => {
  const commands=[];const result={allUsersVerified:true};
  const actual=await runOneShot({log:()=>{},workflow:async options=>{
    commands.push(options.command);
    if(options.command==='up'){options.onAuthenticated();return result;}
  }});
  assert.equal(actual,result);assert.deepEqual(commands,['up','down']);
});

test('one-shot cleans up a failed deployment or analysis and preserves the original failure', async () => {
  const commands=[];const failure=Error('Spark failed');
  await assert.rejects(runOneShot({log:()=>{},workflow:async options=>{
    commands.push(options.command);
    if(options.command==='up'){options.onAuthenticated();throw failure;}
  }}),error=>error===failure);
  assert.deepEqual(commands,['up','down']);
});

test('one-shot does not repeat login or invoke cleanup when initial authentication fails', async () => {
  const commands=[];
  await assert.rejects(runOneShot({workflow:async options=>{commands.push(options.command);throw Error('ExpiredToken');}}),/ExpiredToken/);
  assert.deepEqual(commands,['up']);
});

test('one-shot reports cleanup failure as well as the processing failure', async () => {
  await assert.rejects(runOneShot({workflow:async options=>{
    if(options.command==='up'){options.onAuthenticated();throw Error('processing failed');}
    throw Error('cleanup denied');
  }}),error=>error instanceof AggregateError&&error.errors.length===2&&/processing failed/.test(error.message)&&/cleanup denied/.test(error.message));
});

test('explicit profile overrides pasted credentials without mutating the caller environment', () => {
  const env={AWS_PROFILE:'school-sso',AWS_ACCESS_KEY_ID:'old-key',AWS_SECRET_ACCESS_KEY:'old-secret',AWS_SESSION_TOKEN:'expired',AWS_REGION:'ap-southeast-1'};
  assert.deepEqual(credentialEnvironment(env),{AWS_PROFILE:'school-sso',AWS_REGION:'ap-southeast-1'});
  assert.equal(env.AWS_SESSION_TOKEN,'expired');
  assert.equal(credentialEnvironment({AWS_ACCESS_KEY_ID:'static'}).AWS_ACCESS_KEY_ID,'static');
});

test('expired SSO credentials trigger browser login only for the selected profile', async () => {
  let attempts=0;const calls=[];
  const identity=await authenticateAws({env:{AWS_PROFILE:'school-sso'},log:()=>{},aws:async()=>{
    if(++attempts===1)throw Error('SSO Token has expired');return {Account:'123456789012'};
  },run:async(file,args,stream)=>{calls.push({file,args,stream});return args[0]==='configure'?'school-session':'';}});
  assert.equal(identity.Account,'123456789012');assert.equal(attempts,2);
  assert.deepEqual(calls[1],{file:'aws',args:['sso','login','--profile','school-sso'],stream:true});
});

test('static credential failures explain refresh requirements without inventing a login profile', async () => {
  const calls=[];
  await assert.rejects(authenticateAws({env:{AWS_ACCESS_KEY_ID:'expired'},log:()=>{},aws:async()=>{throw Error('ExpiredToken');},run:async(file,args)=>{calls.push(args);throw Error('not configured');}}),/aws configure sso/);
  assert.ok(!calls.some(args=>args.includes('login')));
});

test('account guard stops before deployment and does not authorise automatic cleanup', async t => {
  const root=await mkdtemp(join(tmpdir(),'lab2-account-'));t.after(()=>rm(root,{recursive:true,force:true}));let authenticated=false;
  await assert.rejects(runWorkflow({root,env:{LAB2_EXPECTED_ACCOUNT:'999999999999'},log:()=>{},onAuthenticated:()=>{authenticated=true;},exec:async(file,args)=>{
    assert.equal(file,'aws');if(args[0]==='--version')return 'aws-cli/2';assert.equal(args[0],'sts');return JSON.stringify({Account:'123456789012'});
  }}),/does not match/);
  assert.equal(authenticated,false);
});

test('one-command deployment bootstraps, submits remotely, verifies and waits for termination', async t => {
  const root = await mkdtemp(join(tmpdir(), 'lab2-workflow-')); t.after(() => rm(root, { recursive: true, force: true }));
  const calls = [], logs = [], opened = [];
  let commandPolls = 0;
  const exec = async (file, args, options) => {
    calls.push({ file, args });
    assert.equal(options.env.AWS_DEFAULT_REGION, 'ap-southeast-1');
    if (file === 'npm') return '';
    const key = args.slice(0, 2).join(' ');
    switch (key) {
      case '--version': return 'aws-cli/2';
      case 'sts get-caller-identity': return JSON.stringify({ Account: '123456789012' });
      case 'cloudformation describe-stacks': return JSON.stringify({ Stacks: [{ Outputs: [ ['WebInstanceId', 'i-0123456789abcdef0'], ['WebUrl', url], ['DataBucket', 'lab-data'] ].map(([OutputKey, OutputValue]) => ({OutputKey, OutputValue})) }] });
      case 'ssm describe-instance-information': return JSON.stringify({ InstanceInformationList: [{ PingStatus: 'Online' }] });
      case 'ssm send-command': {
        const parameters = JSON.parse(args[args.indexOf('--parameters') + 1]);
        assert.ok(parameters.commands[0].includes('process.env.ADMIN_TOKEN'));
        assert.ok(!parameters.commands[0].includes('private-credential'));
        return JSON.stringify({ Command: { CommandId: 'command-1' } });
      }
      case 'ssm get-command-invocation': return JSON.stringify(++commandPolls === 1 ? { Status: 'InProgress' } : { Status: 'Success', StandardOutputContent: `LAB2_JOB=${JSON.stringify(initial)}\n` });
      case 's3 cp': return '';
      case 's3api put-object': return JSON.stringify({ ETag: 'example' });
      case 'emr describe-cluster': return JSON.stringify({ Cluster: { Status: { State: 'TERMINATED' } } });
      case 'emr wait': return '';
      default: throw new Error(`Unexpected ${file} ${args.join(' ')}`);
    }
  };
  const fetcher = async address => address.startsWith('https://checkip') ? { text: async () => '203.0.113.9\n' } : { ok: true, json: async () => ({ ...initial, state: 'SUCCEEDED', stepState: 'COMPLETED' }) };
  const verified = { users: 81306, allUsersVerified: true, requiredUsers: { '214328887': { followers:180, followees:214 }, '107830991': { followers:180, followees:157 } } };
  await runWorkflow({ root, env: { AWS_REGION:'ap-southeast-1', AWS_SECRET_ACCESS_KEY:'private-credential' }, exec, fetcher, pause: async () => {}, log: m => logs.push(m), openBrowser: async u => opened.push(u), verify: async (input, output) => { assert.ok(output.includes(id)); return verified; } });
  assert.deepEqual(opened, [url]);
  assert.ok(calls.some(c => c.file === 'npm' && c.args.includes('bootstrap')));
  assert.ok(calls.some(c => c.args.includes('viewerCidr=203.0.113.9/32') && c.args.includes('--require-approval')));
  assert.ok(calls.some(c => c.args.includes('cluster-terminated')));
  assert.ok(!JSON.stringify(calls).includes('private-credential'));
  assert.ok(!logs.join('\n').includes('private-credential'));
  const report = JSON.parse(await readFile(join(root,'var/aws-run/verification.json'),'utf8'));
  assert.equal(report.source,'AWS EMR'); assert.equal(report.allUsersVerified,true);
});

test('cleanup terminates only tagged lab clusters before destroying the stack', async t => {
  const root = await mkdtemp(join(tmpdir(), 'lab2-cleanup-')); t.after(() => rm(root, { recursive: true, force: true }));
  const calls=[];
  const exec=async (file,args) => {
    calls.push({file,args});
    if(file==='npm'||args[0]==='--version') return '';
    const key=args.slice(0,2).join(' ');
    if(key==='sts get-caller-identity') return JSON.stringify({Account:'123456789012'});
    if(key==='emr list-clusters') return JSON.stringify({Clusters:[{Id:'j-OURS'},{Id:'j-OTHER'}]});
    if(key==='emr describe-cluster') {
      const ours=args.includes('j-OURS');
      return JSON.stringify({Cluster:{Id:ours?'j-OURS':'j-OTHER',Name:ours?'CS5224-Lab2-test':'Other workload',Tags:[{Key:'Project',Value:ours?'CS5224-Lab2':'Other'}]}});
    }
    if(key==='emr terminate-clusters'||key==='emr wait') return '';
    if(key==='ec2 describe-security-groups') return JSON.stringify({SecurityGroups:[]});
    throw new Error(`Unexpected ${key}`);
  };
  await runWorkflow({root,command:'down',env:{AWS_REGION:'ap-southeast-1'},exec,log:()=>{}});
  const terminate=calls.find(c=>c.args.includes('terminate-clusters'));
  assert.ok(terminate.args.includes('j-OURS')); assert.ok(!terminate.args.includes('j-OTHER'));
  assert.ok(calls.findIndex(c=>c.args.includes('cluster-terminated'))<calls.findIndex(c=>c.args.includes('destroy')));
  assert.ok(calls.findIndex(c=>c.args.includes('cluster-terminated'))<calls.findIndex(c=>c.args.includes('describe-security-groups')));
});

test('cleanup removes EMR-added mutual references without touching other traffic or groups', () => {
  const tags=[{Key:'Project',Value:'CS5224-Lab2'},{Key:'aws:cloudformation:stack-name',Value:'CS5224Lab2'}];
  const groups=[
    {GroupId:'sg-primary',Description:'EMR primary; no public ingress',Tags:tags,IpPermissions:[{IpProtocol:'tcp',FromPort:0,ToPort:65535,UserIdGroupPairs:[{GroupId:'sg-primary'},{GroupId:'sg-core'},{GroupId:'sg-other'}],IpRanges:[{CidrIp:'192.0.2.0/24'}]},{IpProtocol:'tcp',FromPort:8443,ToPort:8443,PrefixListIds:[{PrefixListId:'pl-managed'}]}]},
    {GroupId:'sg-core',Description:'EMR workers; no public ingress',Tags:tags,IpPermissions:[{IpProtocol:'icmp',FromPort:-1,ToPort:-1,UserIdGroupPairs:[{GroupId:'sg-primary'},{GroupId:'sg-core'}]}]},
    {GroupId:'sg-other',Description:'EMR workers; no public ingress',Tags:[{Key:'Project',Value:'Other'}],IpPermissions:[{IpProtocol:'-1',UserIdGroupPairs:[{GroupId:'sg-primary'}]}]},
  ];
  assert.deepEqual(emrGroupReferenceRevocations(groups),[
    {groupId:'sg-primary',permissions:[{IpProtocol:'tcp',FromPort:0,ToPort:65535,UserIdGroupPairs:[{GroupId:'sg-core'}]}]},
    {groupId:'sg-core',permissions:[{IpProtocol:'icmp',FromPort:-1,ToPort:-1,UserIdGroupPairs:[{GroupId:'sg-primary'}]}]},
  ]);
});

test('cleanup tolerates concurrent deletion but propagates permission errors', async () => {
  const group=id=>({GroupId:id,Description:'EMR workers; no public ingress',Tags:[{Key:'Project',Value:'CS5224-Lab2'},{Key:'aws:cloudformation:stack-name',Value:'CS5224Lab2'}],IpPermissions:[{IpProtocol:'-1',UserIdGroupPairs:[{GroupId:id==='sg-a'?'sg-b':'sg-a'}]}]});
  const aws=async args=>{if(args[1]==='describe-security-groups')return {SecurityGroups:[group('sg-a'),group('sg-b')]};throw Error('InvalidPermission.NotFound');};
  assert.equal((await clearEmrGroupReferences(aws,()=>{})).length,2);
  await assert.rejects(clearEmrGroupReferences(async args=>{if(args[1]==='describe-security-groups')return {SecurityGroups:[group('sg-a'),group('sg-b')]};throw Error('AccessDenied');},()=>{}),/AccessDenied/);
});

test('remote job submission is valid shell and IP access is restricted to /32',()=>{
  assert.equal(spawnSync('bash',['-n'],{input:remoteStartScript()}).status,0);
  assert.equal(validateCidr('203.0.113.5/32'),'203.0.113.5/32');
  for(const cidr of ['0.0.0.0/0','not-an-ip/32','203.0.113.5/24']) assert.throws(()=>validateCidr(cidr));
});

test('independent cloud verifier handles duplicates, reverse edges, self-follows and zeros',async t=>{
  const root=await mkdtemp(join(tmpdir(),'lab2-reference-')); t.after(()=>rm(root,{recursive:true,force:true}));
  const {mkdir,writeFile}=await import('node:fs/promises');
  await writeFile(join(root,'input.txt'),'1 2\n1 2\n2 1\n3 2\n4 4\n\n');
  for(const folder of ['users','metadata']) {await mkdir(join(root,folder));await writeFile(join(root,folder,'_SUCCESS'),'');}
  const rows=[{userId:'1',followers:1,followees:1},{userId:'2',followers:2,followees:1},{userId:'3',followers:0,followees:1},{userId:'4',followers:1,followees:1}];
  await writeFile(join(root,'users/part-00000'),rows.map(JSON.stringify).join('\n'));
  await writeFile(join(root,'metadata/part-00000'),JSON.stringify({totalLines:6,validRows:5,blankRows:1,invalidRows:0,duplicateRows:1,uniqueEdges:4,selfEdges:1,users:4,totalFollowers:4,totalFollowees:4}));
  assert.equal((await verifyCounts(join(root,'input.txt'),root)).allUsersVerified,true);
  rows[0].followers=2; await writeFile(join(root,'users/part-00000'),rows.map(JSON.stringify).join('\n'));
  await assert.rejects(verifyCounts(join(root,'input.txt'),root),/Count mismatch/);
});
