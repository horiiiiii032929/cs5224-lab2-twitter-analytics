// Convert CDK --outputs-file JSON into a private env file. No AWS API requests.
import { readFile, writeFile } from 'node:fs/promises';
const [input = 'var/cdk-outputs.json', output = '.env.runtime'] = process.argv.slice(2);
const stacks = JSON.parse(await readFile(input, 'utf8'));
const stack = stacks.CS5224Lab2;
if (!stack?.RuntimeEnvironment) throw new Error('Missing CS5224Lab2.RuntimeEnvironment. Deploy the updated CDK stack with --outputs-file first.');
const runtime = JSON.parse(stack.RuntimeEnvironment);
const keys = ['AWS_REGION', 'DATA_BUCKET', 'EMR_SERVICE_ROLE', 'EMR_INSTANCE_PROFILE', 'EMR_SUBNET_ID', 'EMR_PRIMARY_SG', 'EMR_CORE_SG', 'EMR_RELEASE', 'EMR_INSTANCE_TYPE'];
for (const key of keys) {
  if (typeof runtime[key] !== 'string' || !/^[A-Za-z0-9_:./-]+$/.test(runtime[key])) throw new Error(`Missing or invalid runtime value: ${key}`);
}
const lines = [
  '# Generated from CDK outputs. Local AWS-backed API; EC2 is configured automatically.',
  'MODE=aws', 'HOST=127.0.0.1', 'PORT=3001',
  ...keys.map(key => `${key}=${runtime[key]}`),
  '# Fill ADMIN_TOKEN from the OperatorSecretArn secret before starting.',
  'ADMIN_TOKEN=', '',
];
await writeFile(output, lines.join('\n'), { flag: 'wx', mode: 0o600 });
console.log(`Created ${output}. Fill ADMIN_TOKEN, then run npm run start:env. Existing files are never overwritten.`);
