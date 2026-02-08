import { Shield, Radio, Database, Wifi, MapPin, BarChart3 } from 'lucide-react';
import { useStore } from '../store/useStore';
import type { TheftFilter, ViewMode } from '../types';

export function Header() {
  const { filter, setFilter, viewMode, setViewMode, dataSource, records, loading } = useStore();

  const filterOptions: { value: TheftFilter; label: string; icon: string }[] = [
    { value: 'all', label: 'All Thefts', icon: '🔍' },
    { value: 'auto', label: 'Auto Theft', icon: '🚗' },
    { value: 'bike', label: 'Bike Theft', icon: '🚲' },
  ];

  const viewOptions: { value: ViewMode; label: string; Icon: typeof MapPin }[] = [
    { value: 'scatter', label: 'Points', Icon: MapPin },
    { value: 'heatmap', label: 'Heatmap', Icon: BarChart3 },
  ];

  return (
    <header className="bg-gradient-to-r from-slate-900 via-slate-800 to-slate-900 border-b border-slate-700/50 shadow-2xl">
      <div className="max-w-[1800px] mx-auto px-4 py-3">
        <div className="flex flex-col lg:flex-row items-start lg:items-center justify-between gap-3">
          {/* Brand */}
          <div className="flex items-center gap-3">
            <div className="relative">
              <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-red-500 to-orange-600 flex items-center justify-center shadow-lg shadow-red-500/25">
                <Shield className="w-5 h-5 text-white" />
              </div>
              <div className="absolute -top-0.5 -right-0.5 w-3 h-3 bg-green-400 rounded-full border-2 border-slate-900 animate-pulse" />
            </div>
            <div>
              <h1 className="text-lg font-bold text-white tracking-tight flex items-center gap-2">
                Toronto Asset Safety Radar
                <span className="text-[10px] font-medium bg-red-500/20 text-red-400 px-1.5 py-0.5 rounded-full border border-red-500/30">
                  v2
                </span>
              </h1>
              <div className="flex items-center gap-2 text-xs text-slate-400">
                <Radio className="w-3 h-3 text-green-400 animate-pulse" />
                <span>Live Theft Intelligence Dashboard</span>
                {!loading && (
                  <span className="flex items-center gap-1 ml-1">
                    {dataSource === 'static' ? (
                      <Database className="w-3 h-3 text-blue-400" />
                    ) : (
                      <Wifi className="w-3 h-3 text-amber-400" />
                    )}
                    <span className={dataSource === 'static' ? 'text-blue-400' : 'text-amber-400'}>
                      {dataSource === 'static' ? 'Cached' : 'Live API'}
                    </span>
                    <span className="text-slate-500">• {records.length.toLocaleString()} records (6mo)</span>
                  </span>
                )}
              </div>
            </div>
          </div>

          {/* Controls */}
          <div className="flex flex-wrap items-center gap-2">
            {/* Filter Toggle */}
            <div className="flex bg-slate-800/80 rounded-lg p-0.5 border border-slate-700/50">
              {filterOptions.map((opt) => (
                <button
                  key={opt.value}
                  onClick={() => setFilter(opt.value)}
                  className={`px-3 py-1.5 text-xs font-medium rounded-md transition-all duration-200 ${
                    filter === opt.value
                      ? 'bg-gradient-to-r from-red-500 to-orange-500 text-white shadow-lg shadow-red-500/25'
                      : 'text-slate-400 hover:text-white hover:bg-slate-700/50'
                  }`}
                >
                  <span className="mr-1">{opt.icon}</span>
                  {opt.label}
                </button>
              ))}
            </div>

            {/* View Toggle */}
            <div className="flex bg-slate-800/80 rounded-lg p-0.5 border border-slate-700/50">
              {viewOptions.map((opt) => (
                <button
                  key={opt.value}
                  onClick={() => setViewMode(opt.value)}
                  className={`flex items-center gap-1 px-3 py-1.5 text-xs font-medium rounded-md transition-all duration-200 ${
                    viewMode === opt.value
                      ? 'bg-gradient-to-r from-blue-500 to-cyan-500 text-white shadow-lg shadow-blue-500/25'
                      : 'text-slate-400 hover:text-white hover:bg-slate-700/50'
                  }`}
                >
                  <opt.Icon className="w-3 h-3" />
                  {opt.label}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}
