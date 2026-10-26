# CS5224 Lab 2: Twitter Analytics

Source code for the Lab 2 submission: a web frontend and Express backend coordinating a distributed PySpark analysis on Amazon EMR, with input and output on Amazon S3.

## Code

- [Analysis functions](app/analysis/twitter_counts.py): directed-edge deduplication and follower/followee counts.
- [Backend](app/backend): APIs, job orchestration and result loading.
- [Frontend](app/public): results, lookup, pagination and job status.
- [AWS CDK stack](app/infra/lib/lab2-stack.ts): storage, networking, roles and EC2.
- [Deployment runner](app/scripts/aws-run.mjs): deployment, EMR execution, verification and cleanup.
- [Tests](app/test): API, workflow, infrastructure and real Spark pipeline checks.
- [Editable diagrams](diagrams): draw.io files.

## Run

Place the course-supplied `twitter_combined.txt` beside the `app/` directory. The supplied dataset is separate from this source repository. Use Node.js 22+ and AWS CLI with valid credentials or a configured school SSO profile.

```bash
cd app
npm run aws:run
```

This deploys the application, runs EMR, checks every result and cleans up the web stack. Results remain in S3 and local `var/aws-run/`. No operator token needs to be copied into the browser. For a web deployment that stays available, use `npm run aws:up` and then `npm run aws:down` when finished.

See [app/README.md](app/README.md) for local Spark setup, credentials, permissions and full instructions. Cloud execution screenshots and validation evidence are included in the submitted PDF.

## Counting rules

A pair `A B` means A follows B: one followee for A, one follower for B. Identical directed pairs count once; reversed pairs are distinct. IDs remain strings, zero counts are retained, malformed rows fail the job, and unique self-follows are retained.

## AI assistance

OpenAI Codex assisted with implementation, debugging, tests, diagrams and documentation. See [AI-DECLARATION.md](AI-DECLARATION.md). Verification scripts were also AI-assisted; cloud execution and independent result comparisons were performed.
