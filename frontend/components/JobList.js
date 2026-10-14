import { ChevronLeft, ChevronRight, Inbox, ArrowUpRight } from 'lucide-react';
import { relativeTime, Status } from '@/lib/format';

export default function JobList({ jobs, onSelect, offset, hasMore, onPage, loading }) {
  return <>
    <div className="table-scroll"><table className="job-table"><thead><tr><th>Job / Task</th><th>Status</th><th>Priority</th><th>Attempts</th><th>Created</th><th aria-label="Details" /></tr></thead>
      <tbody>{jobs.map(job => <tr key={job.job_id}>
        <td><button className="job-link" onClick={() => onSelect(job.job_id)}><strong>{job.task_type}</strong><code>{job.job_id.slice(0, 12)}</code></button></td>
        <td><Status value={job.status} /></td><td><span className="priority">{job.priority > 0 ? '↑ ' : ''}{job.priority}</span></td>
        <td><span className="attempt-count">{job.attempts}<span> / {job.max_attempts}</span></span></td><td className="muted nowrap">{relativeTime(job.created_at)}</td>
        <td><button className="icon-button" aria-label={`Inspect job ${job.job_id}`} onClick={() => onSelect(job.job_id)}><ArrowUpRight size={16} /></button></td>
      </tr>)}</tbody></table></div>
    {jobs.length === 0 && <div className="empty-state"><Inbox size={30} /><h3>{loading ? 'Loading your queue…' : 'No jobs here yet'}</h3><p>{loading ? 'Fetching the latest state from the broker.' : 'Submit a job or run a demo scenario to see the system in action.'}</p></div>}
    <div className="pagination"><span>{jobs.length ? `${offset + 1}–${offset + jobs.length}` : '0'} jobs on this page</span><div><button className="icon-button" disabled={offset === 0 || loading} onClick={() => onPage(Math.max(0, offset - 10))} aria-label="Previous page"><ChevronLeft size={16} /></button><button className="icon-button" disabled={!hasMore || loading} onClick={() => onPage(offset + 10)} aria-label="Next page"><ChevronRight size={16} /></button></div></div>
  </>;
}
