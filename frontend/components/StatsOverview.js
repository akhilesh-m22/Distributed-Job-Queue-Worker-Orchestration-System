import { ArrowUpRight, Clock3, Radio, CheckCircle2, RotateCcw, CircleAlert } from 'lucide-react';

export default function StatsOverview({ stats, onFilter }) {
  const cards = [
    ['queued', 'Queued', 'Waiting for a worker', Clock3],
    ['running', 'Running', 'In flight right now', Radio],
    ['succeeded', 'Succeeded', 'Durably acknowledged', CheckCircle2],
    ['failed', 'Retry scheduled', 'Backing off automatically', RotateCcw],
    ['dead-letter', 'Dead letter', 'Needs your attention', CircleAlert],
  ];
  return <div className="stats-grid">{cards.map(([key, label, caption, Icon]) =>
    <button className={`stat-card stat-${key}`} key={key} onClick={() => onFilter(key)}>
      <div className="stat-label"><span><Icon size={15} />{label}</span><ArrowUpRight size={15} /></div>
      <strong>{stats ? (stats[key] || 0).toLocaleString() : '—'}</strong>
      <small>{caption}</small>
    </button>
  )}</div>;
}
