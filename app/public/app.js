const $ = id => document.getElementById(id);
const number = value => new Intl.NumberFormat('en-US').format(value);
let selectedJob; let activeJob; let cursors = ['']; let page = 0; let nextCursor; let poll;
async function api(path, options) {
  const response = await fetch(path, options); const body = await response.json();
  if (!response.ok) throw new Error(body.error ?? `HTTP ${response.status}`);
  return body;
}
function notice(message, error = false) { $('notice').textContent = message; $('notice').classList.toggle('error', error); }
function withJob(path) { return selectedJob && selectedJob !== 'manual' ? `${path}${path.includes('?') ? '&' : '?'}jobId=${selectedJob}` : path; }
function card(row) {
  const element = document.createElement('article'); element.className = 'user-card';
  const title = document.createElement('h3'); title.textContent = `User ${row.userId}`; element.append(title);
  for (const kind of ['followers', 'followees']) {
    const line = document.createElement('p'); line.textContent = `${row.userId} has ${number(row[kind])} ${kind}`; element.append(line);
  }
  return element;
}
async function loadPage() {
  const data = await api(withJob(`/api/users?limit=25${cursors[page] ? `&cursor=${cursors[page]}` : ''}`));
  const rows = data.users.map(user => {
    const tr = document.createElement('tr');
    for (const value of [user.userId, number(user.followers), number(user.followees)]) { const td = document.createElement('td'); td.textContent = value; tr.append(td); }
    return tr;
  });
  $('users').replaceChildren(...rows); nextCursor = data.nextCursor;
  $('previous').disabled = page === 0; $('next').disabled = !nextCursor;
  $('page-number').textContent = `Page ${page + 1}`; $('page-count').textContent = `${number(data.total)} users · ordered by ID (text)`;
}
async function loadResults() {
  const metadata = await api(withJob('/api/summary')); selectedJob = metadata.jobId;
  const dl = $('summary'); dl.replaceChildren();
  for (const [label, value] of [['Users', metadata.users], ['Unique relationships', metadata.uniqueEdges], ['Duplicate rows removed', metadata.duplicateRows]]) {
    const div = document.createElement('div'), dt = document.createElement('dt'), dd = document.createElement('dd'); dt.textContent = label; dd.textContent = number(value); div.append(dt, dd); dl.append(div);
  }
  const required = await Promise.all(['214328887', '107830991'].map(id => api(withJob(`/api/users/${id}`))));
  $('required').replaceChildren(...required.map(card)); page = 0; cursors = ['']; await loadPage();
  notice(`Analysis complete · Spark ${metadata.sparkVersion} · ${metadata.partitions} output partitions · ${number(metadata.selfEdges)} self-follows retained.`);
}
function showJob(job) {
  activeJob = ['PENDING', 'RUNNING'].includes(job.state) ? job : undefined;
  $('run').disabled = Boolean(activeJob); $('cancel').disabled = !activeJob;
  $('job-status').textContent = `Job ${job.state.toLowerCase()}${job.stepState ? ` · Spark step ${job.stepState.toLowerCase()}` : ''}`;
  $('job-detail').textContent = [job.id, job.clusterId, job.error].filter(Boolean).join(' · ');
}
async function watchJob(id) {
  clearTimeout(poll);
  try {
    const job = await api(`/api/jobs/${id}`); showJob(job);
    if (activeJob) poll = setTimeout(() => watchJob(id), 4000);
    else if (job.state === 'SUCCEEDED') { selectedJob = id; await loadResults(); }
    else if (job.state === 'FAILED') notice(job.error ?? 'Analysis failed.', true);
  } catch (error) { notice(error.message, true); poll = setTimeout(() => watchJob(id), 8000); }
}
$('lookup').addEventListener('submit', async event => {
  event.preventDefault(); const button = event.submitter; button.disabled = true;
  try { const row = await api(withJob(`/api/users/${encodeURIComponent($('user-id').value.trim())}`)); $('lookup-result').replaceChildren(card(row)); }
  catch (error) { $('lookup-result').textContent = error.message; }
  finally { button.disabled = false; }
});
for (const [id, direction] of [['previous', -1], ['next', 1]]) $(id).addEventListener('click', async () => {
  const oldPage = page; $('previous').disabled = true; $('next').disabled = true;
  if (direction === 1) cursors[page + 1] = nextCursor;
  page += direction;
  try { await loadPage(); } catch (error) { page = oldPage; notice(error.message, true); $('previous').disabled = page === 0; $('next').disabled = !nextCursor; }
});
$('run').addEventListener('click', async () => {
  $('run').disabled = true;
  try {
    const job = await api('/api/jobs', { method: 'POST', headers: { Authorization: `Bearer ${$('operator-token').value}` } });
    showJob(job); notice('Analysis started. Saved results remain available while processing.'); await watchJob(job.id);
  } catch (error) { notice(error.message, true); $('run').disabled = Boolean(activeJob); }
});
$('cancel').addEventListener('click', async () => {
  $('cancel').disabled = true;
  try { const job = await api(`/api/jobs/${activeJob.id}`, { method: 'DELETE', headers: { Authorization: `Bearer ${$('operator-token').value}` } }); clearTimeout(poll); showJob(job); notice('Analysis cancelled.'); }
  catch (error) { notice(error.message, true); $('cancel').disabled = !activeJob; }
});
async function init() {
  try {
    const health = await api('/api/health'); $('environment').textContent = health.mode === 'local' ? 'Local execution · Spark local[2]' : 'AWS execution · S3 + EMR';
    if (health.mode === 'aws') $('run-help').textContent = 'Starts a paid EMR cluster (one primary and two workers). The cluster terminates after the job finishes. Cancel stops the cluster.';
    else $('workflow-help').textContent = 'View saved results, or open Advanced job controls to start a local analysis.';
    const { jobs } = await api('/api/jobs');
    const running = jobs.find(job => ['PENDING', 'RUNNING'].includes(job.state));
    try { await loadResults(); } catch (error) { notice(error.message); }
    if (running) { showJob(running); watchJob(running.id); } else if (jobs[0]) showJob(jobs[0]);
  } catch (error) { notice(error.message, true); }
}
init();
