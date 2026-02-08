import { useMemo } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  AreaChart, Area, PieChart, Pie, Cell,
} from 'recharts';
import { useStore } from '../store/useStore';
import { Clock, MapPin, Building2, TrendingUp } from 'lucide-react';

const COLORS = {
  auto: '#ef4444',
  bike: '#3b82f6',
  accent: '#f59e0b',
};

const PIE_COLORS = ['#ef4444', '#3b82f6', '#10b981', '#f59e0b', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16'];

/** Generate the last 6 calendar month labels in order, e.g. ["Feb '25", "Mar '25", ...] */
function getLast6MonthLabels(): { key: string; label: string }[] {
  const result: { key: string; label: string }[] = [];
  const now = new Date();
  const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  for (let i = 5; i >= 0; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    const label = `${monthNames[d.getMonth()]} '${String(d.getFullYear()).slice(2)}`;
    result.push({ key, label });
  }
  return result;
}

export function Charts() {
  const filteredRecords = useStore((s) => s.filteredRecords());

  const hourlyData = useMemo(() => {
    const counts = Array.from({ length: 24 }, (_, i) => ({
      hour: `${String(i).padStart(2, '0')}:00`,
      auto: 0,
      bike: 0,
    }));
    filteredRecords.forEach((r) => {
      if (r.hour >= 0 && r.hour < 24) {
        counts[r.hour][r.type]++;
      }
    });
    return counts;
  }, [filteredRecords]);

  // Monthly trend — only the last 6 months, properly labeled
  const monthlyData = useMemo(() => {
    const months = getLast6MonthLabels();
    const countsMap = new Map<string, { auto: number; bike: number }>();
    months.forEach((m) => countsMap.set(m.key, { auto: 0, bike: 0 }));

    filteredRecords.forEach((r) => {
      const key = `${r.year}-${String(r.month).padStart(2, '0')}`;
      const entry = countsMap.get(key);
      if (entry) {
        entry[r.type]++;
      }
    });

    return months.map((m) => ({
      month: m.label,
      auto: countsMap.get(m.key)?.auto || 0,
      bike: countsMap.get(m.key)?.bike || 0,
    }));
  }, [filteredRecords]);

  const topNeighbourhoods = useMemo(() => {
    const map = new Map<string, { auto: number; bike: number }>();
    filteredRecords.forEach((r) => {
      const entry = map.get(r.neighbourhood) || { auto: 0, bike: 0 };
      entry[r.type]++;
      map.set(r.neighbourhood, entry);
    });
    return Array.from(map.entries())
      .sort((a, b) => (b[1].auto + b[1].bike) - (a[1].auto + a[1].bike))
      .slice(0, 8)
      .map(([name, counts]) => ({
        name: name.length > 20 ? name.slice(0, 18) + '…' : name,
        auto: counts.auto,
        bike: counts.bike,
        total: counts.auto + counts.bike,
      }));
  }, [filteredRecords]);

  const premiseData = useMemo(() => {
    const map = new Map<string, number>();
    filteredRecords.forEach((r) => {
      map.set(r.premiseType, (map.get(r.premiseType) || 0) + 1);
    });
    return Array.from(map.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 6)
      .map(([name, value]) => ({ name, value }));
  }, [filteredRecords]);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (!active || !payload?.length) return null;
    return (
      <div className="bg-slate-900/95 backdrop-blur border border-slate-700/50 rounded-lg px-3 py-2 shadow-xl">
        <p className="text-xs font-medium text-slate-300 mb-1">{label}</p>
        {payload.map((p: { name: string; value: number; color: string }, i: number) => (
          <div key={i} className="flex items-center gap-2 text-xs">
            <div className="w-2 h-2 rounded-full" style={{ backgroundColor: p.color }} />
            <span className="text-slate-400 capitalize">{p.name}:</span>
            <span className="text-white font-medium">{p.value}</span>
          </div>
        ))}
      </div>
    );
  };

  // Compute 6-month window label for display
  const cutoffDate = new Date();
  cutoffDate.setMonth(cutoffDate.getMonth() - 6);
  const windowLabel = `${cutoffDate.toLocaleDateString('en-CA', { month: 'short', year: 'numeric' })} – Present`;

  return (
    <div>
      {/* Window label */}
      <div className="flex items-center gap-2 mb-3">
        <div className="h-px flex-1 bg-slate-700/50" />
        <span className="text-[10px] text-slate-500 font-medium tracking-wider uppercase">
          Analytics — Last 6 Months ({windowLabel})
        </span>
        <div className="h-px flex-1 bg-slate-700/50" />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* Hourly Distribution */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700/30 rounded-xl p-4">
          <div className="flex items-center gap-2 mb-3">
            <Clock className="w-4 h-4 text-amber-400" />
            <h3 className="text-sm font-semibold text-white">Hourly Distribution</h3>
            <span className="text-[9px] text-slate-500 ml-auto">Last 6 months</span>
          </div>
          <ResponsiveContainer width="100%" height={180}>
            <BarChart data={hourlyData} barGap={0} barCategoryGap="15%">
              <XAxis
                dataKey="hour"
                tick={{ fontSize: 9, fill: '#94a3b8' }}
                axisLine={false}
                tickLine={false}
                interval={3}
              />
              <YAxis tick={{ fontSize: 9, fill: '#94a3b8' }} axisLine={false} tickLine={false} width={30} />
              <Tooltip content={<CustomTooltip />} />
              <Bar dataKey="auto" name="Auto" stackId="a" fill={COLORS.auto} radius={[0, 0, 0, 0]} />
              <Bar dataKey="bike" name="Bike" stackId="a" fill={COLORS.bike} radius={[2, 2, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
          <div className="flex items-center justify-center gap-4 mt-2">
            <div className="flex items-center gap-1.5">
              <div className="w-2.5 h-2.5 rounded-sm bg-red-500" />
              <span className="text-[10px] text-slate-400">Auto</span>
            </div>
            <div className="flex items-center gap-1.5">
              <div className="w-2.5 h-2.5 rounded-sm bg-blue-500" />
              <span className="text-[10px] text-slate-400">Bike</span>
            </div>
          </div>
        </div>

        {/* Monthly Trend — only 6 months */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700/30 rounded-xl p-4">
          <div className="flex items-center gap-2 mb-3">
            <TrendingUp className="w-4 h-4 text-green-400" />
            <h3 className="text-sm font-semibold text-white">Monthly Trend</h3>
            <span className="text-[9px] text-slate-500 ml-auto">6-month window</span>
          </div>
          <ResponsiveContainer width="100%" height={180}>
            <AreaChart data={monthlyData}>
              <XAxis
                dataKey="month"
                tick={{ fontSize: 9, fill: '#94a3b8' }}
                axisLine={false}
                tickLine={false}
              />
              <YAxis tick={{ fontSize: 9, fill: '#94a3b8' }} axisLine={false} tickLine={false} width={30} />
              <Tooltip content={<CustomTooltip />} />
              <Area
                type="monotone"
                dataKey="auto"
                name="Auto"
                stackId="1"
                stroke={COLORS.auto}
                fill={COLORS.auto}
                fillOpacity={0.3}
              />
              <Area
                type="monotone"
                dataKey="bike"
                name="Bike"
                stackId="1"
                stroke={COLORS.bike}
                fill={COLORS.bike}
                fillOpacity={0.3}
              />
            </AreaChart>
          </ResponsiveContainer>
          <div className="flex items-center justify-center gap-4 mt-2">
            <div className="flex items-center gap-1.5">
              <div className="w-2.5 h-2.5 rounded-sm bg-red-500" />
              <span className="text-[10px] text-slate-400">Auto</span>
            </div>
            <div className="flex items-center gap-1.5">
              <div className="w-2.5 h-2.5 rounded-sm bg-blue-500" />
              <span className="text-[10px] text-slate-400">Bike</span>
            </div>
          </div>
        </div>

        {/* Top Neighbourhoods — stacked auto/bike */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700/30 rounded-xl p-4">
          <div className="flex items-center gap-2 mb-3">
            <MapPin className="w-4 h-4 text-red-400" />
            <h3 className="text-sm font-semibold text-white">Top Neighbourhoods</h3>
          </div>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={topNeighbourhoods} layout="vertical">
              <XAxis type="number" tick={{ fontSize: 9, fill: '#94a3b8' }} axisLine={false} tickLine={false} />
              <YAxis
                dataKey="name"
                type="category"
                tick={{ fontSize: 8, fill: '#94a3b8' }}
                axisLine={false}
                tickLine={false}
                width={110}
              />
              <Tooltip content={<CustomTooltip />} />
              <Bar dataKey="auto" name="Auto" stackId="a" fill={COLORS.auto} radius={[0, 0, 0, 0]} />
              <Bar dataKey="bike" name="Bike" stackId="a" fill={COLORS.bike} radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Premise Type */}
        <div className="bg-slate-800/50 backdrop-blur border border-slate-700/30 rounded-xl p-4">
          <div className="flex items-center gap-2 mb-3">
            <Building2 className="w-4 h-4 text-purple-400" />
            <h3 className="text-sm font-semibold text-white">By Premise Type</h3>
          </div>
          <ResponsiveContainer width="100%" height={180}>
            <PieChart>
              <Pie
                data={premiseData}
                cx="50%"
                cy="50%"
                innerRadius={40}
                outerRadius={70}
                paddingAngle={3}
                dataKey="value"
              >
                {premiseData.map((_, index) => (
                  <Cell key={index} fill={PIE_COLORS[index % PIE_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip content={<CustomTooltip />} />
            </PieChart>
          </ResponsiveContainer>
          <div className="flex flex-wrap gap-2 mt-1 justify-center">
            {premiseData.map((d, i) => (
              <div key={d.name} className="flex items-center gap-1">
                <div className="w-2 h-2 rounded-full" style={{ backgroundColor: PIE_COLORS[i % PIE_COLORS.length] }} />
                <span className="text-[9px] text-slate-400">{d.name} ({d.value})</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
