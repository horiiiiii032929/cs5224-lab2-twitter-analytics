#!/usr/bin/env node
import { App } from 'aws-cdk-lib';
import { Lab2Stack } from '../lib/lab2-stack.ts';
const app = new App();
new Lab2Stack(app, 'CS5224Lab2', { env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION ?? 'ap-southeast-1' } });
