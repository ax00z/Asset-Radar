import { useMemo, useState } from 'react';
import { useStore } from '../store/useStore';
import { List, ChevronLeft, ChevronRight } from 'lucide-react';

const PAGE_SIZE = 10;

export function RecentIncidents() {
  const filteredRecords = useStore((s) => s.filteredRecords());
  const setSelectedRecord = useStore((s) => s.setSelectedRecord);
  const [page, setPage] = useState(0);

  const sorted = useMemo(
    () => [...filteredRecords].sort((a, b) => {
      const da = new Date(a.year, a.month - 1, a.day, a.hour).getTime();
      const db = new Date(b.year, b.month - 1, b.day, b.hour).getTime();
      return db - da;
    }),
    [filteredRecords]
  );

  const totalPages = Math.ceil(sorted.length / PAGE_SIZE);
  const pageRecords = sorted.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  // Compute the 6-month window for display
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - 6);
  const windowStr = `${cutoff.toLocaleDateString('en-CA')} to ${new Date().toLocaleDateString('en-CA')}`;

  return (
    <div className="bg-slate-800/50 backdrop-blur border border-slate-700/30 rounded-xl overflow-hidden">
      <div className="flex items-center justify-between px-4 py-3 border-b border-slate-700/30">
        <div className="flex items-center gap-2">
          <List className="w-4 h-4 text-cyan-400" />
          <h3 className="text-sm font-semibold text-white">Recent Incidents</h3>
          <span className="text-[10px] text-slate-500">{sorted.length.toLocaleString()} total</span>
          <span className="text-[9px] text-slate-600 hidden sm:inline">• {windowStr}</span>
        </div>
        <div className="flex items-center gap-2">
          <button
            disabled={page === 0}
            onClick={() => setPage((p) => Math.max(0, p - 1))}
            className="w-6 h-6 rounded flex items-center justify-center text-slate-400 hover:text-white disabled:opacity-30 disabled:cursor-not-allowed hover:bg-slate-700 transition-colors"
          >
            <ChevronLeft className="w-3.5 h-3.5" />
          </button>
          <span className="text-[10px] text-slate-500 font-mono">
            {page + 1}/{Math.max(totalPages, 1)}
          </span>
          <button
            disabled={page >= totalPages - 1}
            onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
            className="w-6 h-6 rounded flex items-center justify-center text-slate-400 hover:text-white disabled:opacity-30 disabled:cursor-not-allowed hover:bg-slate-700 transition-colors"
          >
            <ChevronRight className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="text-left text-[10px] uppercase tracking-wider text-slate-500 bg-slate-800/80">
              <th className="px-4 py-2">Type</th>
              <th className="px-4 py-2">Date</th>
              <th className="px-4 py-2">Time</th>
              <th className="px-4 py-2 hidden sm:table-cell">Neighbourhood</th>
              <th className="px-4 py-2 hidden md:table-cell">Premise</th>
              <th className="px-4 py-2 hidden lg:table-cell">Status</th>
            </tr>
          </thead>
          <tbody>
            {pageRecords.map((r, idx) => (
              <tr
                key={`${r.id}-${page}-${idx}`}
                onClick={() => setSelectedRecord(r)}
                className="border-t border-slate-700/20 hover:bg-slate-700/30 cursor-pointer transition-colors"
              >
                <td className="px-4 py-2">
                  <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-medium ${
                    r.type === 'auto'
                      ? 'bg-red-500/15 text-red-400 border border-red-500/20'
                      : 'bg-blue-500/15 text-blue-400 border border-blue-500/20'
                  }`}>
                    {r.type === 'auto' ? '🚗' : '🚲'}
                    {r.type === 'auto' ? 'Auto' : 'Bike'}
                  </span>
                </td>
                <td className="px-4 py-2 text-xs text-slate-300 font-mono">
                  {r.date}
                </td>
                <td className="px-4 py-2 text-xs text-slate-400 font-mono">
                  {String(r.hour).padStart(2, '0')}:00
                </td>
                <td className="px-4 py-2 text-xs text-slate-400 hidden sm:table-cell truncate max-w-[200px]">
                  {r.neighbourhood}
                </td>
                <td className="px-4 py-2 text-xs text-slate-500 hidden md:table-cell">
                  {r.premiseType}
                </td>
                <td className="px-4 py-2 hidden lg:table-cell">
                  <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${
                    r.status === 'CLEAR' || r.status === 'CLEARED'
                      ? 'bg-green-500/15 text-green-400'
                      : 'bg-amber-500/15 text-amber-400'
                  }`}>
                    {r.status}
                  </span>
                </td>
              </tr>
            ))}
            {pageRecords.length === 0 && (
              <tr>
                <td colSpan={6} className="px-4 py-8 text-center text-sm text-slate-500">
                  No incidents found
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
