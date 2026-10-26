# CS5224 Lab 2 — Twitter Analytics

A complete local/cloud implementation of Part 2. The same PySpark script runs locally with `local[2]` and on Amazon EMR with YARN. The browser calls Express APIs; Express submits analysis jobs and reads their results. Every displayed user, including the paginated list, uses the required two-line follower/followee sentence format. AWS EMR execution was verified on 26 October 2026 (Asia/Singapore): all 81,306 users matched the independent reference, two executors processed tasks on both core workers, and the EMR cluster terminated successfully. The submission package contains evidence under `evidence/aws/`.

## One-shot AWS run

From this directory, run:

```bash
npm run aws:run
```

This authenticates, deploys the web app, submits the EMR job through SSM, verifies every user's counts, downloads results, waits for EMR termination, and removes the web stack. It also attempts cleanup if deployment, monitoring or verification fails after authentication. S3 results and downloaded evidence remain. The browser is for viewing progress and results; **no operator-token copying or typing is needed**. Optional manual controls are collapsed under Advanced job controls.

Requires Node.js 22+ and AWS CLI. No local Python, Java, Docker or Session Manager plugin is needed for the cloud workflow. The command uses the existing AWS CLI configuration, with an optional local `.env` for profile/credentials and region. It never uploads that file. The authenticated account is printed before deployment; set `LAB2_EXPECTED_ACCOUNT` to stop if a different account is selected.

### Avoid repeatedly pasting AWS session tokens

If your school provides IAM Identity Center access, configure its profile once:

```bash
aws configure sso --profile school-sso
```

Use the school's SSO start URL, region, account and role. Set `AWS_PROFILE=school-sso` in `.env` (or the shell). The runner uses that profile instead of any stale access keys left in `.env`. It reuses valid sessions and opens `aws sso login` automatically when an SSO-configured profile needs renewal. AWS still requires the browser sign-in when the session expires; the runner does not create permanent credentials or change profiles on your behalf.

A profile containing manually pasted temporary credentials cannot refresh itself. For that option, copy `.env.example` to `.env` and fill `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`; AWS will require fresh values after they expire. No project-specific `.env` is required if your AWS CLI profile already works.

For a web instance that stays available after verification, use `npm run aws:up`, then `npm run aws:down` when finished. `aws:run` removes it automatically. The commands use paid cloud resources and non-interactive CDK deployment. Force-quitting the process or revoking its credentials can prevent cleanup; `aws:down` remains available to retry it.

Evidence is saved in `var/aws-run/`, with distributed results in `var/aws-run/<job-id>/spark-output/`. Valid existing jobs on the same web instance are reused. The data bucket and CDK bootstrap resources are retained. Cleanup waits for tagged EMR clusters to terminate, removes mutual references between this stack's tagged EMR security groups, then destroys the web stack; EC2 security-group describe/revoke permissions are needed.

If public IP discovery needs an override, set `LAB2_VIEWER_CIDR` to your current IPv4 `/32`. Tokens stay inside EC2 for automated submission. Direct viewing uses the IP-restricted HTTP endpoint; use SSM port forwarding for manual token-bearing administration.

AWS references: [IAM Identity Center setup](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html) and [SSO browser login](https://docs.aws.amazon.com/cli/latest/reference/sso/login.html).

## Architecture and lecture basis

```text
Browser ──HTTP API──> Express on EC2 ──RunJobFlow──> EMR / Spark (YARN)
                          │                          │
                          │                   reads input + code from S3
                          │                          │
                          └──reads completed results─S3 results/<job-id>/
```

- Lecture 06, pp.26–29: build a custom analytics pipeline using cloud infrastructure/platforms. Express is one of the technologies in the lecture example.
- Lecture 07, pp.3–7, 11–12: split inputs, map contributions, shuffle by user ID, reduce counts, write distributed output. Page 18 introduces Spark and its batch-processing support.
- Lecture 08, pp.15–16: put input/code on S3, execute with EMR, write results to S3. The lecture's K-means stage is not needed here.
- Cloud default: EMR `emr-7.10.0` (Spark 3.5.5), one primary plus two core workers, `m5.xlarge`. Two Spark executors, 2 cores and 6 GiB heap each, with eight output partitions. This is a lab configuration, not an autoscaling production architecture. Check executor hosts in the actual EMR run.
- Web server: `t3.small`, Amazon Linux 2023, Docker running Node 22. No NAT gateway or load balancer. Frontend and API share one origin. EC2 is deployed by CDK; EMR clusters are created only when an authorised job request is submitted.

## Files

```text
analysis/twitter_counts.py     Distributed processing; also the submitted analysis code
backend/                      Express API, local/EMR job orchestration, output reader
public/                       Plain HTML/CSS/JavaScript frontend
infra/                        TypeScript AWS CDK application
scripts/audit_dataset.py       Independent set/Counter reference and input audit
scripts/verify_results.py      Compare every output user against the reference
reports/dataset-audit.json     Audit of the supplied file (including SHA-256)
test/                         API, orchestration, infrastructure and parsing checks
```

The original `../twitter_combined.txt` stays in place. CDK uploads only this file to `input/` and `analysis/` to `scripts/`; it does not upload the surrounding vault.

## Data semantics

`A B` means A follows B: add one **followee** to A and one **follower** to B.

1. Parse two ASCII decimal user IDs, kept as strings to avoid integer precision loss.
2. Count blank lines; reject malformed nonblank rows instead of silently dropping them.
3. Remove duplicate **directed pairs** with distributed `distinct`. `A B` and `B A` are different edges.
4. Retain self-follows: no exclusion is specified. A unique `A A` adds one follower and one followee to A.
5. Map each edge to `(A, (0, 1))`, `(B, (1, 0))`, then `reduceByKey` to sum `(followers, followees)`.
6. Include every ID present on either side, with zero for a missing direction.
7. Write distributed JSONL part files under `users/`; write metadata last under `metadata/`. Serve results only when both directories have `_SUCCESS` markers and the backend integrity checks pass.

The backend caches one complete result set in memory (81,306 users for this lab). Processing stays distributed; the small final result is loaded by the API for lookup and paging. IDs are paginated in lexicographic string order.

### Audited results for the supplied dataset

| Metric | Value |
|---|---:|
| Input lines | 2,426,143 |
| Invalid / blank lines | 0 / 0 |
| Duplicate rows removed | 657,994 |
| Unique directed relationships | 1,768,149 |
| Unique self-follows retained | 14 |
| Users | 81,306 |

| User ID | Followers | Followees |
|---|---:|---:|
| 214328887 | 180 | 214 |
| 107830991 | 180 | 157 |

Both degree sums must equal 1,768,149. `verify_results.py` independently checks every user's counts, not just those sums or the two required users.

## Run locally

Requirements: Node.js 22+, Python 3.11, Java 17, and `uv` (or Python venv/pip). Run these commands **from this directory**:

```bash
npm ci
uv venv --python python3.11 .venv
uv pip install --python .venv/bin/python -r requirements.txt
npm run audit
npm run test:python
npm test
npm run typecheck
npm run synth -- --quiet
```

Run Spark and compare all results:

```bash
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
npm run analyze
npm run verify
npm start
```

Open **http://127.0.0.1:3000**. The completed manual result is shown automatically. Look up IDs or use Previous/Next to browse all users.

For a new end-to-end job from the browser, enter the local operator token **`local-lab2`** and click **Run analysis**. It starts the same PySpark script, polls status, and replaces the displayed results when successful. The default local server binds to loopback. Set `ADMIN_TOKEN` to override the token.

`npm run analyze` writes `var/results/manual` and deliberately refuses to overwrite existing output. Use the browser to create a fresh job-specific directory for subsequent runs, or specify another output directory:

```bash
npm run analyze -- --output var/results/another-run
.venv/bin/python scripts/verify_results.py ../twitter_combined.txt var/results/another-run
```

The local job log is `var/jobs/<job-id>.log`. Job records are persisted under `var/jobs/`. On a local server restart, unfinished jobs are marked failed; inspect for surviving Spark processes before rerunning. The cloud Docker image runs the API only: analysis there is delegated to EMR, so it does not contain Python/Spark.

Configuration: `MODE`, `HOST`, `PORT`, `ADMIN_TOKEN`, `VAR_DIR`, `INPUT_FILE`, and `PYTHON_BIN`. `MODE=aws` additionally requires the bucket, EMR roles/profile, subnet and security group environment variables defined by CDK. No AWS credentials are needed in local mode.

## Manual deployment (optional)

This creates paid AWS resources. EMR, its EC2 nodes, the web EC2 instance/public IPv4, Secrets Manager and storage may incur charges; do not assume free-tier coverage. Confirm account credits, EMR availability and EC2 quotas in the target region. The initial setup needs permission to create networking, IAM, EC2, S3, deployment Lambda resources and Secrets Manager resources.

```bash
export AWS_DEFAULT_REGION=ap-southeast-1
aws sts get-caller-identity
npm run cdk -- bootstrap
npm run synth -- --quiet
npm run cdk -- diff
mkdir -p var
npm run cdk -- deploy --outputs-file var/cdk-outputs.json
```

Create `var/` first if it is absent. Deployment uploads the supplied data/code to S3, provisions the web server and builds the Docker image there. CDK deployment can complete before the application has finished bootstrapping.

### Access with SSM (default; no inbound security-group ports)

Install AWS CLI's Session Manager plugin on your computer if needed. Substitute the `WebInstanceId` stack output:

```bash
aws ssm start-session \
  --target <WebInstanceId> \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["3000"],"localPortNumber":["3001"]}'
```

Then open **http://127.0.0.1:3001**. Retrieve the generated operator token using the `OperatorSecretArn` output:

```bash
aws secretsmanager get-secret-value \
  --secret-id <OperatorSecretArn> \
  --query SecretString --output text
```

Paste that token into the page. It is never bundled in JavaScript or stored in browser storage. Do not include the token or credentials in screenshots.

Optional direct viewing from your current IP is available with `npm run cdk -- deploy -c viewerCidr=<YOUR-IP>/32`; this opens port 3000 only to that CIDR. Use SSM for operator-token requests because the optional direct endpoint is plain HTTP.

### Execute on EMR

1. Click **Run analysis** with the operator token. The backend launches one EMR cluster and includes a Spark step that reads S3 input and writes a unique `results/<job-id>/` prefix.
2. The UI polls cluster/step state and shows the cluster ID. Use the EMR console to inspect progress, executors and step logs.
3. Successful results are shown through Express APIs. `KeepJobFlowAliveWhenNoSteps=false` and `ActionOnFailure=TERMINATE_CLUSTER` request cluster termination after completion or failure. Check that termination actually finishes.
4. **Cancel active job** requests EMR termination. The last successful result remains available while a new job runs.

Only one job can be active per web server. Web process restarts recover known cloud cluster IDs from the persisted job records. If a launch response is lost, or the EC2 instance is replaced, manually inspect EMR for clusters tagged `Project=CS5224-Lab2`; these runtime clusters are not CloudFormation resources.

Default release and worker type can be changed through `-c emrRelease=...` and `-c emrInstanceType=...`. The default executor memory assumes `m5.xlarge` workers; review Spark resource settings if changing worker size.

### Cloud troubleshooting

- Connect to the web instance with `aws ssm start-session --target <WebInstanceId>`.
- Inspect `/var/log/cloud-init-output.log`, `sudo docker logs lab2`, and `sudo docker ps -a` for bootstrap/application failures.
- EMR logs are stored at `s3://<DataBucket>/logs/`; Spark event logs at `spark-events/`.
- EMR service role uses `AmazonEMRServicePolicy_v2`, tagged VPC/subnet/security groups and an explicit pass-role grant for this stack's instance role. Cluster EC2 permissions are scoped to this lab's S3 prefixes; no deprecated blanket EC2 role policy is used.
- A running Spark step can finish before cluster termination completes. Check the EMR console before removing the stack.

### Cleanup

Cancel/wait for all jobs, and confirm no tagged EMR clusters remain running. Then:

```bash
npm run cdk -- destroy
```

The data bucket is **retained** intentionally to preserve results/evidence; delete its contents and bucket manually when no longer needed. CDK does not own runtime-created EMR clusters. Saved job records reside on the web instance's disk, which is removed when the instance is deleted/replaced. Retain any logs you need before teardown.

## Environment files

- `.env.example` → `.env`: credentials/profile and region for **`npm run aws:up` / `npm run aws:down`**. No resource IDs or admin tokens need to be entered.
- `.env.local.example` → `.env.runtime`: optional local Spark settings for `npm run start:env`.
- `.env.runtime.example` → `.env.runtime`: optional advanced setup for an AWS-backed API running on your computer. Normally unnecessary: the one-command workflow uses the EC2 backend.

`npm start` uses the shell environment. `npm run start:env` explicitly loads `.env.runtime`. The deployment scripts load `.env` automatically. Actual env files are ignored by Git, Docker and CDK assets and must not be put into submission archives.

For advanced local API operation after deploying:

```bash
npm run env:aws
# Fill ADMIN_TOKEN in the generated .env.runtime using the OperatorSecretArn secret.
npm run start:env
```

The helper reads `var/cdk-outputs.json` and creates `.env.runtime` with mode 0600, refusing to overwrite existing files. The EC2 application is configured automatically and needs none of these manual runtime steps.

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/health` | Mode / health |
| GET | `/api/jobs` | Jobs and current state |
| POST | `/api/jobs` | Start job; operator Bearer token required |
| GET | `/api/jobs/:id` | State / cluster and step IDs |
| DELETE | `/api/jobs/:id` | Cancel; operator Bearer token required |
| GET | `/api/summary` | Latest successful result metadata |
| GET | `/api/users/:id` | One user's counts; 404 if absent |
| GET | `/api/users?limit=25&cursor=<last-id>` | Page through users; max limit 200 |

Result APIs accept `jobId=<UUID>` to pin a completed run. The frontend pins results so paging cannot mix two different analysis runs. A missing completed run returns 409. AWS read access is provided by the instance role rather than browser credentials.

## Submission checklist

- Frontend setup description: max 50 words. Backend setup description: max 100 words.
- Show both required users and counts in a browser screenshot.
- Include the analysis source and explain directed-pair deduplication, retained self-follows and zero counts.
- Capture EMR cluster/node configuration, successful Spark step, distributed executors/tasks, S3 input/output and relevant logs. Local screenshots cannot demonstrate cloud execution.
- Include the degree-sum check and independent verification result.
- Combine with Part 1 in `<StudentID>.pdf`.

## Official implementation references

- [EMR Spark job submission](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-submit-step.html)
- [EMR 7.10.0 / Spark 3.5.5](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-7100-release.html)
- [EMR node roles](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-master-core-task-nodes.html)
- [EMR service role and tags](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role.html)
- [EMR IAM actions and tag conditions](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazonelasticmapreduce.html)
- [Amazon Linux 2023 package names](https://docs.aws.amazon.com/linux/al2023/ug/amzn2-al2023-ami.html)
