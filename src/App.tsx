import { useEffect, useCallback, useState, useMemo } from 'react';
import { useStore } from './store/useStore';
import { fetchAllData, fetchLiveDataOnly } from './services/dataService';
import { getDemoData } from './services/demoData';
import { Header } from './components/Header';
import { StatsCards } from './components/StatsCards';
import { TheftMap } from './components/TheftMap';
import { Charts } from './components/Charts';
import { RecentIncidents } from './components/RecentIncidents';
import { DetailPanel } from './components/DetailPanel';
import { LoadingScreen } from './components/LoadingScreen';
import { ErrorScreen } from './components/ErrorScreen';
import { RefreshCw, Wifi, Database, AlertTriangle, CheckCircle2, XCircle } from 'lucide-react';

type DataMode = 'auto' | 'demo';

export function App() {
  const {
    loading, error, records,
    setRecords, setLoading, setError,
    setDataSource, setLastUpdated,
  } = useStore();

  const [loadingMsg, setLoadingMsg] = useState('Initializing radar systems...');
  const [dataMode, setDataMode] = useState<DataMode>('auto');
  const [liveTestResult, setLiveTestResult] = useState<{
    status: 'idle' | 'testing' | 'success' | 'error';
    message: string;
    autoCount: number;
    bikeCount: number;
  }>({ status: 'idle', message: '', autoCount: 0, bikeCount: 0 });

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    setLoadingMsg('Connecting to data sources...');

    try {
      setLoadingMsg('Fetching theft data (static → live fallback)...');
      const { records: fetchedRecords, source } = await fetchAllData();
      if (fetchedRecords.length === 0) throw new Error('No records returned');
      setRecords(fetchedRecords);
      setDataSource(source);
      setLastUpdated(new Date().toISOString());
      setDataMode('auto');
      setLoadingMsg('Rendering visualization...');
      await new Promise((r) => setTimeout(r, 300));
      setLoading(false);
    } catch {
      // Use demo data as ultimate fallback
      setLoadingMsg('API unavailable, loading demo data...');
      await new Promise((r) => setTimeout(r, 500));
      const demoRecords = getDemoData();
      setRecords(demoRecords);
      setDataSource('live');
      setLastUpdated(new Date().toISOString());
      setDataMode('demo');
      setLoading(false);
    }
  }, [setRecords, setLoading, setError, setDataSource, setLastUpdated]);

  const testLiveData = useCallback(async () => {
    setLiveTestResult({ status: 'testing', message: 'Connecting to Toronto Police ArcGIS API...', autoCount: 0, bikeCount: 0 });

    try {
      const { records: liveRecords } = await fetchLiveDataOnly();
      const autoCount = liveRecords.filter(r => r.type === 'auto').length;
      const bikeCount = liveRecords.filter(r => r.type === 'bike').length;

      if (liveRecords.length === 0) {
        setLiveTestResult({
          status: 'error',
          message: 'API responded but returned 0 records. Check API availability.',
          autoCount: 0,
          bikeCount: 0,
        });
      } else {
        setLiveTestResult({
          status: 'success',
          message: `Live API working! Fetched ${liveRecords.length.toLocaleString()} records.`,
          autoCount,
          bikeCount,
        });
        // Replace current data with live data
        setRecords(liveRecords);
        setDataSource('live');
        setLastUpdated(new Date().toISOString());
        setDataMode('auto');
      }
    } catch (err) {
      setLiveTestResult({
        status: 'error',
        message: `Live API failed: ${err instanceof Error ? err.message : 'Unknown error'}. This is usually CORS if running in a browser without the scraper.`,
        autoCount: 0,
        bikeCount: 0,
      });
    }
  }, [setRecords, setDataSource, setLastUpdated]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  // Compute data summary
  const dataSummary = useMemo(() => {
    const autoCount = records.filter(r => r.type === 'auto').length;
    const bikeCount = records.filter(r => r.type === 'bike').length;
    const cutoff = new Date();
    cutoff.setMonth(cutoff.getMonth() - 6);
    const oldestStr = records.length > 0
      ? records.reduce((oldest, r) => {
          const d = new Date(r.year, r.month - 1, r.day);
          return d < oldest ? d : oldest;
        }, new Date()).toLocaleDateString('en-CA')
      : 'N/A';
    const newestStr = records.length > 0
      ? records.reduce((newest, r) => {
          const d = new Date(r.year, r.month - 1, r.day);
          return d > newest ? d : newest;
        }, new Date(0)).toLocaleDateString('en-CA')
      : 'N/A';
    return { autoCount, bikeCount, oldestStr, newestStr };
  }, [records]);

  if (loading) return <LoadingScreen message={loadingMsg} />;
  if (error) return <ErrorScreen error={error} onRetry={loadData} />;

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950">
      <Header />

      {/* Demo mode banner */}
      {dataMode === 'demo' && (
        <div className="bg-amber-500/10 border-b border-amber-500/20 px-4 py-2">
          <div className="max-w-[1800px] mx-auto flex items-center justify-center gap-2">
            <AlertTriangle className="w-3.5 h-3.5 text-amber-400" />
            <p className="text-xs text-amber-300">
              <span className="font-semibold">Demo Mode:</span> Showing simulated data. Click "Test Live API" below to check real data connectivity.
            </p>
          </div>
        </div>
      )}

      <main className="max-w-[1800px] mx-auto px-4 py-4 space-y-4">
        {/* Stats Cards */}
        <StatsCards />

        {/* Map + Side Panel */}
        <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
          {/* Map - takes 2/3 */}
          <div className="xl:col-span-2 h-[500px] lg:h-[600px]">
            <TheftMap />
          </div>

          {/* Side panel */}
          <div className="space-y-4">
            {/* System Status */}
            <div className="bg-slate-800/50 backdrop-blur border border-slate-700/30 rounded-xl p-4">
              <h3 className="text-sm font-semibold text-white mb-3 flex items-center gap-2">
                <span className="w-2 h-2 rounded-full bg-green-400 animate-pulse" />
                System Status
              </h3>
              <div className="space-y-2">
                <StatusRow label="Data Pipeline" status="operational" />
                <StatusRow label="Map Renderer" status="operational" />
                <StatusRow label="Analytics Engine" status="operational" />
                <StatusRow
                  label="Live API"
                  status={dataMode === 'demo' ? 'fallback' : 'operational'}
                />
              </div>

              {/* Data summary */}
              <div className="mt-3 pt-3 border-t border-slate-700/30 space-y-1.5">
                <div className="flex items-center justify-between">
                  <span className="text-[10px] text-slate-500">Source</span>
                  <span className="text-[10px] text-slate-300 flex items-center gap-1">
                    {dataMode === 'demo' ? (
                      <><Database className="w-3 h-3 text-amber-400" /> Demo</>
                    ) : (
                      <><Wifi className="w-3 h-3 text-green-400" /> Live/Cached</>
                    )}
                  </span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-[10px] text-slate-500">🚗 Auto Thefts</span>
                  <span className="text-[10px] text-red-400 font-mono">{dataSummary.autoCount.toLocaleString()}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-[10px] text-slate-500">🚲 Bike Thefts</span>
                  <span className="text-[10px] text-blue-400 font-mono">{dataSummary.bikeCount.toLocaleString()}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-[10px] text-slate-500">Date Range</span>
                  <span className="text-[10px] text-slate-400 font-mono">{dataSummary.oldestStr} → {dataSummary.newestStr}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-[10px] text-slate-500">Window</span>
                  <span className="text-[10px] text-cyan-400">Last 6 months</span>
                </div>
              </div>
            </div>

            {/* Live API Test Panel */}
            <div className="bg-slate-800/50 backdrop-blur border border-slate-700/30 rounded-xl p-4">
              <h3 className="text-sm font-semibold text-white mb-3 flex items-center gap-2">
                <Wifi className="w-4 h-4 text-cyan-400" />
                Live Data Test
              </h3>
              <p className="text-[10px] text-slate-500 mb-3">
                Test if the browser can directly reach the Toronto Police ArcGIS API. This verifies auto &amp; bike theft endpoints.
              </p>
              <button
                onClick={testLiveData}
                disabled={liveTestResult.status === 'testing'}
                className="w-full flex items-center justify-center gap-2 px-3 py-2 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 text-white text-xs font-medium rounded-lg transition-all disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {liveTestResult.status === 'testing' ? (
                  <RefreshCw className="w-3.5 h-3.5 animate-spin" />
                ) : (
                  <Wifi className="w-3.5 h-3.5" />
                )}
                {liveTestResult.status === 'testing' ? 'Testing...' : 'Test Live API & Load Data'}
              </button>

              {/* Test result */}
              {liveTestResult.status !== 'idle' && liveTestResult.status !== 'testing' && (
                <div className={`mt-3 p-2.5 rounded-lg border text-[10px] ${
                  liveTestResult.status === 'success'
                    ? 'bg-green-500/10 border-green-500/20 text-green-300'
                    : 'bg-red-500/10 border-red-500/20 text-red-300'
                }`}>
                  <div className="flex items-start gap-2">
                    {liveTestResult.status === 'success' ? (
                      <CheckCircle2 className="w-3.5 h-3.5 text-green-400 shrink-0 mt-0.5" />
                    ) : (
                      <XCircle className="w-3.5 h-3.5 text-red-400 shrink-0 mt-0.5" />
                    )}
                    <div>
                      <p className="font-medium">{liveTestResult.message}</p>
                      {liveTestResult.status === 'success' && (
                        <p className="text-slate-400 mt-1">
                          🚗 Auto: {liveTestResult.autoCount.toLocaleString()} • 🚲 Bike: {liveTestResult.bikeCount.toLocaleString()}
                        </p>
                      )}
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* Quick insights */}
            <QuickInsights />
          </div>
        </div>

        {/* Charts */}
        <Charts />

        {/* Recent Incidents Table */}
        <RecentIncidents />

        {/* Footer */}
        <footer className="text-center py-6 border-t border-slate-800">
          <p className="text-xs text-slate-500">
            Toronto Asset Safety Radar v2 • Data from Toronto Police Service Open Data
          </p>
          <p className="text-[10px] text-slate-600 mt-1">
            Built with React + Canvas • Decoupled Analytics Architecture • 6-Month Rolling Window
          </p>
        </footer>
      </main>

      {/* Detail modal */}
      <DetailPanel />
    </div>
  );
}

function StatusRow({ label, status }: { label: string; status: 'operational' | 'fallback' | 'error' }) {
  const colors = {
    operational: 'bg-green-400',
    fallback: 'bg-amber-400',
    error: 'bg-red-400',
  };
  const labels = {
    operational: 'Operational',
    fallback: 'Demo Fallback',
    error: 'Error',
  };
  return (
    <div className="flex items-center justify-between">
      <span className="text-xs text-slate-400">{label}</span>
      <div className="flex items-center gap-1.5">
        <div className={`w-1.5 h-1.5 rounded-full ${colors[status]}`} />
        <span className={`text-[10px] ${status === 'operational' ? 'text-green-400' : status === 'fallback' ? 'text-amber-400' : 'text-red-400'}`}>
          {labels[status]}
        </span>
      </div>
    </div>
  );
}

function QuickInsights() {
  const filteredRecords = useStore((s) => s.filteredRecords());

  // Time-of-day breakdown
  const timeBreakdown = {
    morning: filteredRecords.filter((r) => r.hour >= 6 && r.hour < 12).length,
    afternoon: filteredRecords.filter((r) => r.hour >= 12 && r.hour < 18).length,
    evening: filteredRecords.filter((r) => r.hour >= 18 && r.hour < 24).length,
    night: filteredRecords.filter((r) => r.hour >= 0 && r.hour < 6).length,
  };
  const total = Math.max(filteredRecords.length, 1);

  return (
    <div className="bg-slate-800/50 backdrop-blur border border-slate-700/30 rounded-xl p-4">
      <h3 className="text-sm font-semibold text-white mb-3">⏰ Time Distribution</h3>
      <div className="space-y-2.5">
        <TimeBar label="🌅 Morning (6-12)" count={timeBreakdown.morning} total={total} color="bg-amber-500" />
        <TimeBar label="☀️ Afternoon (12-18)" count={timeBreakdown.afternoon} total={total} color="bg-orange-500" />
        <TimeBar label="🌆 Evening (18-24)" count={timeBreakdown.evening} total={total} color="bg-red-500" />
        <TimeBar label="🌙 Night (0-6)" count={timeBreakdown.night} total={total} color="bg-indigo-500" />
      </div>
    </div>
  );
}

function TimeBar({ label, count, total, color }: { label: string; count: number; total: number; color: string }) {
  const pct = (count / total) * 100;
  return (
    <div>
      <div className="flex items-center justify-between mb-1">
        <span className="text-[10px] text-slate-400">{label}</span>
        <span className="text-[10px] text-slate-500 font-mono">{count} ({pct.toFixed(0)}%)</span>
      </div>
      <div className="h-1.5 bg-slate-700 rounded-full overflow-hidden">
        <div
          className={`h-full ${color} rounded-full transition-all duration-700`}
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}
