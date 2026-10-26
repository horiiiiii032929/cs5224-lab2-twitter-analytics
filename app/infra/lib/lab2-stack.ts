import { Stack, StackProps, CfnOutput, Duration, RemovalPolicy, Tags } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as assets from 'aws-cdk-lib/aws-s3-assets';
import * as secrets from 'aws-cdk-lib/aws-secretsmanager';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import { mkdtempSync, copyFileSync } from 'node:fs';
import { tmpdir } from 'node:os';

export class Lab2Stack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);
    const root = fileURLToPath(new URL('../../', import.meta.url));
    const emrTag = 'for-use-with-amazon-emr-managed-policies';
    const vpc = new ec2.Vpc(this, 'Vpc', { maxAzs: 1, natGateways: 0, subnetConfiguration: [{ name: 'public', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 }] });
    Tags.of(vpc).add(emrTag, 'true');
    vpc.addGatewayEndpoint('S3Endpoint', { service: ec2.GatewayVpcEndpointAwsService.S3 });
    const bucket = new s3.Bucket(this, 'Data', {
      encryption: s3.BucketEncryption.S3_MANAGED, blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true, removalPolicy: RemovalPolicy.RETAIN,
      lifecycleRules: [{ prefix: 'logs/', expiration: Duration.days(14) }, { prefix: 'spark-events/', expiration: Duration.days(14) }, { abortIncompleteMultipartUploadAfter: Duration.days(1) }],
    });
    // Stage only the supplied input; never package the entire academic vault.
    const staging = mkdtempSync(join(tmpdir(), 'cs5224-lab2-input-'));
    copyFileSync(join(root, '../twitter_combined.txt'), join(staging, 'twitter_combined.txt'));
    const inputUpload = new s3deploy.BucketDeployment(this, 'UploadInput', { sources: [s3deploy.Source.asset(staging)], destinationBucket: bucket, destinationKeyPrefix: 'input', prune: false, memoryLimit: 512 });
    const scriptUpload = new s3deploy.BucketDeployment(this, 'UploadAnalysis', { sources: [s3deploy.Source.asset(join(root, 'analysis'), { exclude: ['__pycache__', '*.pyc'] })], destinationBucket: bucket, destinationKeyPrefix: 'scripts', prune: false });

    const primarySg = new ec2.SecurityGroup(this, 'EmrPrimarySg', { vpc, description: 'EMR primary; no public ingress' });
    const coreSg = new ec2.SecurityGroup(this, 'EmrCoreSg', { vpc, description: 'EMR workers; no public ingress' });
    for (const group of [primarySg, coreSg]) {
      Tags.of(group).add(emrTag, 'true');
      group.addIngressRule(primarySg, ec2.Port.allTraffic(), 'Cluster internal traffic');
      group.addIngressRule(coreSg, ec2.Port.allTraffic(), 'Cluster internal traffic');
    }
    const emrEc2Role = new iam.Role(this, 'EmrEc2Role', { assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com') });
    bucket.grantRead(emrEc2Role, 'input/*'); bucket.grantRead(emrEc2Role, 'scripts/*');
    bucket.grantReadWrite(emrEc2Role, 'results/*'); bucket.grantReadWrite(emrEc2Role, 'logs/*'); bucket.grantReadWrite(emrEc2Role, 'spark-events/*');
    const profile = new iam.CfnInstanceProfile(this, 'EmrProfile', { roles: [emrEc2Role.roleName] });
    const emrRole = new iam.Role(this, 'EmrServiceRole', {
      assumedBy: new iam.ServicePrincipal('elasticmapreduce.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AmazonEMRServicePolicy_v2')],
    });
    // The AWS managed policy only passes the default EC2 role; permit this stack's exact role too.
    emrRole.addToPolicy(new iam.PolicyStatement({ actions: ['iam:PassRole'], resources: [emrEc2Role.roleArn], conditions: { StringEquals: { 'iam:PassedToService': 'ec2.amazonaws.com' } } }));
    emrRole.addToPolicy(new iam.PolicyStatement({ actions: ['iam:GetInstanceProfile'], resources: [profile.attrArn] }));

    const webRole = new iam.Role(this, 'WebRole', { assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'), managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore')] });
    bucket.grantRead(webRole, 'results/*');
    webRole.addToPolicy(new iam.PolicyStatement({ actions: ['elasticmapreduce:RunJobFlow'], resources: ['*'], conditions: { StringEquals: { 'aws:RequestTag/Project': 'CS5224-Lab2' } } }));
    webRole.addToPolicy(new iam.PolicyStatement({ actions: ['elasticmapreduce:AddTags'], resources: [this.formatArn({ service: 'elasticmapreduce', resource: 'cluster', resourceName: '*' })], conditions: { StringEquals: { 'aws:RequestTag/Project': 'CS5224-Lab2' } } }));
    webRole.addToPolicy(new iam.PolicyStatement({ actions: ['elasticmapreduce:ListSteps', 'elasticmapreduce:DescribeCluster', 'elasticmapreduce:TerminateJobFlows'], resources: [this.formatArn({ service: 'elasticmapreduce', resource: 'cluster', resourceName: '*' })], conditions: { StringEquals: { 'aws:ResourceTag/Project': 'CS5224-Lab2' } } }));
    webRole.addToPolicy(new iam.PolicyStatement({ actions: ['iam:PassRole'], resources: [emrRole.roleArn], conditions: { StringEquals: { 'iam:PassedToService': 'elasticmapreduce.amazonaws.com' } } }));
    webRole.addToPolicy(new iam.PolicyStatement({ actions: ['iam:PassRole'], resources: [emrEc2Role.roleArn], conditions: { StringEquals: { 'iam:PassedToService': 'ec2.amazonaws.com' } } }));
    webRole.addToPolicy(new iam.PolicyStatement({ actions: ['iam:GetInstanceProfile'], resources: [profile.attrArn] }));
    webRole.addToPolicy(new iam.PolicyStatement({ actions: ['iam:CreateServiceLinkedRole'], resources: [this.formatArn({ service: 'iam', region: '', resource: 'role', resourceName: 'aws-service-role/elasticmapreduce.amazonaws.com/AWSServiceRoleForEMRCleanup' })], conditions: { StringEquals: { 'iam:AWSServiceName': 'elasticmapreduce.amazonaws.com' } } }));
    const token = new secrets.Secret(this, 'OperatorToken', { generateSecretString: { passwordLength: 40, excludePunctuation: true } });
    token.grantRead(webRole);
    const appAsset = new assets.Asset(this, 'AppCode', { path: root, exclude: ['node_modules', '.venv', 'var', 'cdk.out', 'cdk.context.json', '.git', '.env', '.env.*', '.playwright-cli', '__pycache__', '**/__pycache__'] });
    appAsset.grantRead(webRole);
    const sg = new ec2.SecurityGroup(this, 'WebSg', { vpc, description: 'SSM access by default; optional viewer CIDR' });
    const viewerCidr = this.node.tryGetContext('viewerCidr') as string | undefined;
    if (viewerCidr) {
      if (viewerCidr === '0.0.0.0/0') throw new Error('Use your own IP /32, or omit viewerCidr and use SSM forwarding');
      sg.addIngressRule(ec2.Peer.ipv4(viewerCidr), ec2.Port.tcp(3000), 'Optional lab viewer access');
    }
    const web = new ec2.Instance(this, 'Web', {
      vpc, vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC }, securityGroup: sg, role: webRole,
      instanceType: new ec2.InstanceType('t3.small'),
      machineImage: ec2.MachineImage.latestAmazonLinux2023({ cachedInContext: false }),
      requireImdsv2: true, userDataCausesReplacement: true,
      blockDevices: [{ deviceName: '/dev/xvda', volume: ec2.BlockDeviceVolume.ebs(16, { encrypted: true, deleteOnTermination: true }) }],
    });
    const vars: Record<string, string> = {
      MODE: 'aws', HOST: '0.0.0.0', PORT: '3000', AWS_REGION: this.region,
      DATA_BUCKET: bucket.bucketName, EMR_SERVICE_ROLE: emrRole.roleArn,
      EMR_INSTANCE_PROFILE: profile.ref, EMR_SUBNET_ID: vpc.publicSubnets[0].subnetId,
      EMR_PRIMARY_SG: primarySg.securityGroupId, EMR_CORE_SG: coreSg.securityGroupId,
      EMR_RELEASE: this.node.tryGetContext('emrRelease') ?? 'emr-7.10.0',
      EMR_INSTANCE_TYPE: this.node.tryGetContext('emrInstanceType') ?? 'm5.xlarge',
    };
    web.userData.addCommands(
      'set -euo pipefail',
      'dnf install -y docker unzip awscli-2',
      'systemctl enable --now docker',
      'install -d -m 755 /opt/lab2/code /opt/lab2/var',
      `aws s3 cp '${appAsset.s3ObjectUrl}' /opt/lab2/app.zip --region '${this.region}'`,
      'unzip -o /opt/lab2/app.zip -d /opt/lab2/code',
      "cat > /opt/lab2/app.env <<'LAB2ENV'",
      ...Object.entries(vars).map(([key, value]) => `${key}=${value}`),
      'LAB2ENV',
      `printf 'ADMIN_TOKEN=%s\\n' "$(aws secretsmanager get-secret-value --secret-id '${token.secretArn}' --query SecretString --output text --region '${this.region}')" >> /opt/lab2/app.env`,
      'chmod 600 /opt/lab2/app.env',
      'chown 1000:1000 /opt/lab2/var',
      'docker build -t cs5224-lab2 /opt/lab2/code',
      'docker run -d --name lab2 --network host --restart unless-stopped --env-file /opt/lab2/app.env -v /opt/lab2/var:/app/var cs5224-lab2',
    );
    web.node.addDependency(inputUpload, scriptUpload, profile, emrRole);
    new CfnOutput(this, 'WebInstanceId', { value: web.instanceId });
    new CfnOutput(this, 'DataBucket', { value: bucket.bucketName });
    new CfnOutput(this, 'RuntimeEnvironment', { value: this.toJsonString(vars), description: 'Non-secret application configuration; use scripts/create-aws-env.mjs' });
    new CfnOutput(this, 'OperatorSecretArn', { value: token.secretArn });
    new CfnOutput(this, 'WebUrl', { value: viewerCidr ? `http://${web.instancePublicDnsName}:3000` : 'Use SSM port forwarding; see README.md' });
    Tags.of(this).add('Project', 'CS5224-Lab2');
  }
}
