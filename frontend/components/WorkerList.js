import { Server, Cpu, ArrowUpRight } from 'lucide-react';
import { relativeTime } from '@/lib/format';

export default function WorkerList({ workers, compact = false }) {
  const visible = compact ? workers.slice(0, 4) : workers;
  const active = workers.filter(w => w.is_alive).length;
  return <section className="panel worker-panel">
    <div className="panel-heading"><div><span className="eyebrow">COMPUTE</span><h2>Worker fleet <span className="count">{active}</span></h2></div><Server size={18} /></div>
    {visible.length === 0 ? <div className="empty-state"><Cpu size={28} /><h3>No workers connected</h3><p>Start the local demo to bring your fleet online.</p><code>python scripts/demo.py</code></div> :
      <div className="worker-list">{visible.map(worker => <article className="worker" key={worker.worker_id}>
        <div className={`worker-symbol ${worker.is_alive ? 'alive' : ''}`}><Cpu size={19} /></div>
        <div className="worker-info"><div className="worker-title"><strong>{worker.name}</strong><span className={`worker-state ${worker.is_alive ? 'alive' : ''}`}>{worker.is_alive ? worker.current_job ? 'BUSY' : 'IDLE' : 'OFFLINE'}</span></div>
          <p>{worker.current_job ? `${worker.current_job_type} · ${worker.current_job.slice(0, 8)}` : worker.is_alive ? 'Ready for the next job' : 'Session disconnected'}</p>
          <div className="capabilities">{worker.capabilities.map(cap => <span key={cap}>{cap.replaceAll('_', ' ')}</span>)}</div>
          <small>Heartbeat {worker.heartbeat_age_secs.toFixed(1)}s · Joined {relativeTime(worker.connected_at)}</small>
        </div>
      </article>)}</div>}
    <div className="panel-footer"><span className="small-dot" />Capability-based dispatch<ArrowUpRight size={13} /></div>
  </section>;
}
