'use client';

import { useEffect, useId, useRef, useState } from 'react';
import { X, Send, RotateCcw, KeyRound } from 'lucide-react';
import { Status } from '@/lib/format';

export function Dialog({ title, subtitle, onClose, children }) {
  const ref = useRef(null);
  const headingId = useId();
  useEffect(() => { ref.current.showModal(); }, []);
  return <dialog ref={ref} aria-labelledby={headingId} className="dialog" onCancel={onClose} onClick={event => { if (event.target === ref.current) onClose(); }}>
    <div className="dialog-heading"><div><span className="eyebrow">{subtitle}</span><h2 id={headingId}>{title}</h2></div><button className="icon-button" onClick={onClose} aria-label="Close dialog"><X size={20} /></button></div>
    {children}
  </dialog>;
}

const templates = {
  send_email: { to: 'hello@example.com', subject: 'Hello from Relay', delay_secs: 2 },
  resize_image: { width: 1920, height: 1080, new_width: 320, new_height: 240, delay_secs: 3 },
  generate_report: { rows: 250, delay_secs: 2 },
};

export function SubmitDialog({ onClose, onSubmit }) {
  const [task, setTask] = useState('send_email');
  const [payload, setPayload] = useState(JSON.stringify(templates.send_email, null, 2));
  const [priority, setPriority] = useState(0);
  const [attempts, setAttempts] = useState(5);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  async function submit(event) {
    event.preventDefault();
    setError('');
    setBusy(true);
    try {
      const parsed = JSON.parse(payload);
      if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') throw new Error('Payload must be a JSON object.');
      await onSubmit({ task_type: task, payload: parsed, priority: Number(priority), max_attempts: Number(attempts) });
      onClose();
    } catch (err) { setError(err.message); }
    finally { setBusy(false); }
  }
  return <Dialog title="Give your workers something to do." subtitle="NEW JOB" onClose={onClose}>
    <form onSubmit={submit} className="dialog-body">
      <label>Task type<select value={task} onChange={e => { setTask(e.target.value); setPayload(JSON.stringify(templates[e.target.value], null, 2)); }}>{Object.keys(templates).map(t => <option key={t}>{t}</option>)}</select></label>
      <div className="form-row"><label>Priority<input type="number" min="-1000" max="1000" required value={priority} onChange={e => setPriority(e.target.value)} /><small>Higher values run first</small></label><label>Maximum attempts<input type="number" min="1" max="100" required value={attempts} onChange={e => setAttempts(e.target.value)} /><small>Includes the first execution</small></label></div>
      <label>Payload <span className="muted">JSON</span><textarea spellCheck={false} rows={9} value={payload} onChange={e => setPayload(e.target.value)} /></label>
      <p className="form-hint">These tasks simulate email, image processing, and report generation. Add <code>{'"fail_first_n": 2'}</code> to observe automatic retries.</p>
      {error && <div className="error-banner" role="alert">{error}</div>}
      <div className="dialog-actions"><button className="button" type="button" onClick={onClose}>Cancel</button><button className="button primary" disabled={busy}><Send size={15} />{busy ? 'Submitting…' : 'Submit job'}</button></div>
    </form>
  </Dialog>;
}

export function JobDialog({ job, onClose, onRetry }) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  async function retry() {
    setBusy(true);
    try { await onRetry(job.job_id); } catch (err) { setError(err.message); }
    finally { setBusy(false); }
  }
  return <Dialog title={job.task_type} subtitle="JOB INSPECTOR" onClose={onClose}><div className="dialog-body">
    <div className="detail-summary"><code>{job.job_id}</code><Status value={job.status} /></div>
    <div className="detail-grid">{[['Attempts', `${job.attempts} / ${job.max_attempts}`], ['Priority', job.priority], ['Worker', job.worker_id?.slice(0, 12) || 'Unassigned'], ['Created', new Date(job.created_at).toLocaleString()], ['Started', job.started_at ? new Date(job.started_at).toLocaleString() : '—'], ['Retry due', job.status === 'failed' ? new Date(job.scheduled_at).toLocaleString() : '—'], ['Finished', job.finished_at ? new Date(job.finished_at).toLocaleString() : '—']].map(([label, value]) => <div key={label}><small>{label}</small><span>{value}</span></div>)}</div>
    <h3>Payload</h3><pre>{JSON.stringify(job.payload, null, 2)}</pre>
    {job.result !== null && <><h3>Result</h3><pre>{JSON.stringify(job.result, null, 2)}</pre></>}
    {job.last_error && <><h3>Last error / recovery reason</h3><pre className="error-code">{job.last_error}</pre></>}
    {error && <p className="error-banner" role="alert">{error}</p>}
    {job.status === 'dead-letter' && <div className="dialog-actions"><button className="button primary" onClick={retry} disabled={busy}><RotateCcw size={15} />{busy ? 'Requeueing…' : 'Retry with a fresh budget'}</button></div>}
  </div></Dialog>;
}

export function AccessDialog({ onClose, onSave }) {
  const [token, setToken] = useState('');
  return <Dialog title="Connect to a protected broker" subtitle="CONNECTION" onClose={onClose}><form className="dialog-body" onSubmit={e => { e.preventDefault(); onSave(token); onClose(); }}>
    <p className="muted">For a server configured with DJQ_API_KEY, enter its token. It stays in memory for this tab and is cleared when you reload.</p>
    <label>API token<input type="password" autoComplete="off" value={token} onChange={e => setToken(e.target.value)} /></label>
    <div className="dialog-actions"><button className="button primary"><KeyRound size={15} />Connect</button></div>
  </form></Dialog>;
}
