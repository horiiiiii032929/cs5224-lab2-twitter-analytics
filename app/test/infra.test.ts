import test from 'node:test';
import { App } from 'aws-cdk-lib';
import { Template, Match } from 'aws-cdk-lib/assertions';
import { Lab2Stack } from '../infra/lib/lab2-stack.ts';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';

test('CDK creates private storage, restricted web ingress and scoped EMR roles', () => {
  const app = new App();
  const template = Template.fromStack(new Lab2Stack(app, 'TestLab2', { env: { account: '123456789012', region: 'ap-southeast-1' } }));
  template.resourceCountIs('AWS::EC2::NatGateway', 0);
  template.resourceCountIs('AWS::EMR::Cluster', 0); // Created only by explicit API submission.
  template.hasResourceProperties('AWS::S3::Bucket', { PublicAccessBlockConfiguration: { BlockPublicAcls: true, BlockPublicPolicy: true, IgnorePublicAcls: true, RestrictPublicBuckets: true } });
  template.hasResourceProperties('AWS::EC2::LaunchTemplate', { LaunchTemplateData: { MetadataOptions: { HttpTokens: 'required' } } });
  template.hasResourceProperties('AWS::EC2::SecurityGroup', { GroupDescription: 'SSM access by default; optional viewer CIDR', SecurityGroupIngress: Match.absent() });
  template.hasResourceProperties('AWS::IAM::Policy', { PolicyDocument: { Statement: Match.arrayWith([Match.objectLike({ Action: 'iam:PassRole', Condition: { StringEquals: { 'iam:PassedToService': 'ec2.amazonaws.com' } }, Resource: Match.objectLike({ 'Fn::GetAtt': Match.anyValue() }) })]) } });
  const instance = Object.values(template.findResources('AWS::EC2::Instance'))[0];
  const fragments = instance.Properties.UserData['Fn::Base64']['Fn::Join'][1];
  const script = fragments.map((part: unknown) => typeof part === 'string' ? part : 'TOKEN').join('');
  assert.equal(spawnSync('bash', ['-n'], { input: script }).status, 0, 'Bootstrap must be valid shell');
  const format = script.match(/printf '([^']+)'/)[1];
  assert.equal(spawnSync('printf', [format, 'example-token'], { encoding: 'utf8' }).stdout, 'ADMIN_TOKEN=example-token\n', 'Secret must be a valid env-file line');
});
