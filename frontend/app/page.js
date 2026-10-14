'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { Activity, ArrowDown, ArrowRight, Box, Check, CircleAlert, Code2, Cpu, KeyRound, Layers3, LayoutDashboard, ListFilter, Play, Plus, RefreshCw, Search, Server, ShieldCheck, Terminal, Workflow } from 'lucide-react';
import StatsOverview from '@/components/StatsOverview';
import WorkerList from '@/components/WorkerList';
import JobList from '@/components/JobList';
import { SubmitDialog, JobDialog, AccessDialog } from '@/components/Dialogs';

export default function Home() {
  const [data, setData] = useState({ stats: null, workers: [], jobs: [], has_more: false });
  const [view, setView] = useState('overview');
  const [status, setStatus] = useState('');
  const [search, setSearch] = useState('');
  const [offset, setOffset] = useState(0);
  const [token, setToken] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  const [scenarioBusy, setScenarioBusy] = useState(false);
  const [updated, setUpdated] = useState(null);
  const [notice, setNotice] = useState('');
  const [modal, setModal] = useState(null);
  const [selected, setSelected] = useState(null);
  const generation = useRef(0);

  const api = useCallback(async (path, options = {}) => {
    const response = await fetch(`/api/${path}`, { ...options, headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}) }, signal: AbortSignal.timeout(10000) });
    const body = await response.json();
    if (!response.ok) throw new Error(body.error || 'The request could not be completed.');
    return body;
  }, [token]);

  const refresh = useCallback(async () => {
    const id = ++generation.current;
    setBusy(true);
    try {
      const query = new URLSearchParams({ limit: '10', offset: String(offset), ...(status ? { status } : {}), ...(search ? { search } : {}) });
      const [stats, workers, jobs, detail] = await Promise.all([api('stats'), api('workers'), api(`jobs?${query}`), selected ? api(`jobs/${selected}`) : null]);
      if (id !== generation.current) return;
      setData({ stats, workers: workers.workers, jobs: jobs.jobs, has_more: jobs.has_more });
      if (detail) setModal(current => current?.type === 'job' ? { type: 'job', job: detail } : current);
      setError('');
      setUpdated(new Date());
    } catch (err) { if (id === generation.current) setError(err.message); }
    finally { if (id === generation.current) setBusy(false); }
  }, [api, offset, status, search, selected]);

  useEffect(() => {
    const initial = setTimeout(refresh, 150);
    const interval = setInterval(refresh, 2500);
    const requestGeneration = generation;
    return () => { clearTimeout(initial); clearInterval(interval); requestGeneration.current++; };
  }, [refresh]);
  useEffect(() => { if (!notice) return; const timer = setTimeout(() => setNotice(''), 6000); return () => clearTimeout(timer); }, [notice]);

  function filter(value) { setStatus(value); setOffset(0); setView('jobs'); }
  function navigate(next) { setView(next); setStatus(''); setSearch(''); setOffset(0); }
  function closeModal() { setModal(null); setSelected(null); }
  async function selectJob(id) {
    try { const job = await api(`jobs/${id}`); setSelected(id); setModal({ type: 'job', job }); }
    catch (err) { setError(err.message); }
  }
  async function submit(body) {
    const result = await api('jobs', { method: 'POST', body: JSON.stringify(body) });
    setNotice(`Job ${result.job_id.slice(0, 8)} added to the queue.`);
    await refresh();
  }
  async function retry(id) {
    await api(`dead-letter/${id}/retry`, { method: 'POST' });
    setNotice('Job requeued with a fresh retry budget.');
    await refresh();
  }
  async function runScenario() {
    setScenarioBusy(true);
    let submitted = 0;
    try {
      const jobs = [
        ...Array.from({ length: 6 }, (_, i) => ({ task_type: ['send_email', 'resize_image', 'generate_report'][i % 3], payload: { delay_secs: 2 + i % 3 }, priority: i % 3, max_attempts: 5 })),
        { task_type: 'generate_report', payload: { delay_secs: 1, fail_first_n: 2 }, max_attempts: 4 },
        { task_type: 'send_email', payload: { delay_secs: 1, fail_first_n: 2 }, max_attempts: 2 },
      ];
      for (const job of jobs) { await api('jobs', { method: 'POST', body: JSON.stringify(job) }); submitted++; }
      setNotice('8 real jobs queued: 6 successes, 1 automatic retry, and 1 dead letter to inspect.');
      await refresh();
    } catch (err) { setError(`${submitted} of 8 demo jobs submitted. ${err.message}`); }
    finally { setScenarioBusy(false); }
  }

  const connected = data.workers.filter(w => w.is_alive);
  const occupied = connected.filter(w => w.current_job).length;
  const completed = (data.stats?.succeeded || 0) + (data.stats?.['dead-letter'] || 0);
  const success = completed ? Math.round(data.stats.succeeded / completed * 100) : null;
  const navItems = [['overview', 'Overview', LayoutDashboard], ['jobs', 'Job explorer', Layers3], ['workers', 'Worker fleet', Cpu]];

  return <div className="app-shell">
    <aside className="sidebar">
      <Link href="/" className="brand"><span className="brand-symbol"><Workflow size={23} /></span>relay<span className="brand-period">.</span></Link>
      <div className="workspace-label"><span className="workspace-icon">R</span><div>Queue workspace<small>Single broker cluster</small></div><span className="version">v1</span></div>
      <p className="nav-caption">WORKSPACE</p>
      <nav aria-label="Main navigation">{navItems.map(([id, label, Icon]) => <button key={id} className={view === id ? 'nav-item active' : 'nav-item'} onClick={() => navigate(id)}><Icon size={18} />{label}{id === 'workers' && <span>{connected.length}</span>}</button>)}</nav>
      <div className="sidebar-bottom"><div className="engine-card"><ShieldCheck size={19} /><strong>Built for recovery.</strong><p>Durable jobs. Explicit acknowledgments. No silent losses.</p><div><span className="small-dot" />SQLite · WAL persistence</div></div><button className="nav-item" onClick={() => setModal({ type: 'access' })}><KeyRound size={17} />Connection settings</button><div className="sidebar-footer"><Code2 size={14} />Built from first principles</div></div>
    </aside>

    <div className="main-shell"><header className="topbar"><div><span className="muted">Workspace</span><span className="breadcrumb-slash">/</span>{navItems.find(item => item[0] === view)[1]}</div><div className="topbar-right"><span className={`connection ${error ? 'disconnected' : updated ? '' : 'connecting'}`}><span />{error ? 'Disconnected' : updated ? 'Live connection' : 'Connecting'}</span><button className="icon-button" onClick={refresh} disabled={busy} aria-label="Refresh dashboard"><RefreshCw size={16} className={busy ? 'spin' : ''} /></button></div></header>
      <main>
        <section className="page-heading"><div><span className="eyebrow">YOUR DISTRIBUTED CONTROL ROOM</span><h1>{view === 'overview' ? 'Every job. Under control.' : view === 'jobs' ? 'Follow the work.' : 'Meet your worker fleet.'}</h1><p>{view === 'overview' ? 'From the first dispatch to the final acknowledgment. See it all happen.' : view === 'jobs' ? 'Inspect payloads, trace outcomes, and give failed jobs another chance.' : 'Independent workers. Shared purpose. Automatic recovery.'}</p></div><button className="button primary" onClick={() => setModal({ type: 'submit' })}><Plus size={17} />Submit job</button></section>
        {error && <div className="error-banner" role="alert"><CircleAlert size={18} /><div><strong>Connection needs attention</strong><p>{error} {updated && 'Showing the last successful snapshot.'}</p></div><button className="button" onClick={() => setModal({ type: 'access' })}>Connection settings</button></div>}
        <StatsOverview stats={data.stats} onFilter={filter} />
        {view === 'overview' && <section className="scenario-banner"><div className="scenario-icon"><Play size={20} /></div><div><span className="eyebrow">DON’T JUST READ ABOUT IT</span><h3>Put the queue through its paces.</h3><p>A live mix of successful jobs, retries, and a dead letter. Watch the whole lifecycle.</p></div><button className="button" disabled={scenarioBusy || !updated || !!error} onClick={runScenario}>{scenarioBusy ? 'Queueing scenario…' : 'Run demo scenario'}<ArrowRight size={16} /></button></section>}
        {view !== 'workers' && <div className={view === 'overview' ? 'content-grid' : ''}>
          <section className="panel jobs-panel"><div className="panel-heading"><div><span className="eyebrow">THE WORK, IN MOTION</span><h2>{status === 'dead-letter' ? 'Dead-letter queue' : 'Job activity'}<span className="count">{data.stats?.total ?? '—'}</span></h2></div><Activity size={18} /></div>
            <div className="table-toolbar"><label className="search-box"><Search size={16} /><input aria-label="Search jobs" placeholder="Search job ID or task…" value={search} onChange={e => { setSearch(e.target.value); setOffset(0); }} /></label><label className="filter-box"><ListFilter size={15} /><select aria-label="Filter by status" value={status} onChange={e => { setStatus(e.target.value); setOffset(0); }}><option value="">All statuses</option>{['queued', 'running', 'succeeded', 'failed', 'dead-letter'].map(s => <option key={s} value={s}>{s === 'failed' ? 'Retry scheduled' : s.replace('-', ' ')}</option>)}</select></label></div>
            <JobList jobs={data.jobs} onSelect={selectJob} offset={offset} hasMore={data.has_more} onPage={setOffset} loading={busy} />
          </section>
          {view === 'overview' && <div className="right-column"><WorkerList workers={data.workers} compact /><section className="panel health-panel"><span className="eyebrow">CLUSTER PULSE</span><div className="health-line"><span>Worker utilization</span><strong>{connected.length ? Math.round(occupied / connected.length * 100) : 0}%</strong></div><div className="meter"><span style={{ width: `${connected.length ? occupied / connected.length * 100 : 0}%` }} /></div><div className="health-line"><span>Terminal success rate</span><strong>{success === null ? '—' : `${success}%`}</strong></div><small>{completed} completed jobs · {data.stats?.executions || 0} attempts in current budgets</small></section></div>}
        </div>}
        {view === 'workers' && <WorkerList workers={data.workers} />}
        {view === 'overview' && <section className="architecture"><div><span className="eyebrow">SMALL SURFACE. STRONG FUNDAMENTALS.</span><h2>One queue. A resilient pipeline.</h2><p>At-least-once execution, with durable state at every handoff.</p></div><div className="pipeline"><span><Terminal size={17} />REST API</span><ArrowRight size={16} /><span><Box size={17} />SQLite queue</span><ArrowRight size={16} /><span><Server size={17} />TCP workers</span><ArrowRight size={16} /><span><Check size={17} />ACK / retry</span></div><div className="pipeline-note"><ArrowDown size={13} />Worker disappears? The broker requeues its in-flight job.</div></section>}
        <footer className="page-footer"><span>RELAY / JOB ORCHESTRATION</span><span>{updated ? `Last synced ${updated.toLocaleTimeString()} · refreshes every 2.5s` : 'Waiting for the broker'}</span></footer>
      </main>
    </div>
    {notice && <div className="toast" role="status"><Check size={18} />{notice}</div>}
    {modal?.type === 'submit' && <SubmitDialog onClose={closeModal} onSubmit={submit} />}
    {modal?.type === 'job' && <JobDialog job={modal.job} onClose={closeModal} onRetry={retry} />}
    {modal?.type === 'access' && <AccessDialog onClose={closeModal} onSave={setToken} />}
  </div>;
}
