import { useMemo } from 'react';
import { useStore } from '../store/useStore';
import { Car, Bike, AlertTriangle, TrendingUp, Clock, MapPin } from 'lucide-react';

export function StatsCards() {
  const records = useStore((s) => s.records);
  const filteredRecords = useStore((s) => s.filteredRecords());

  const stats = useMemo(() => {
    const autoCount = records.filter((r) => r.type === 'auto').length;
    const bikeCount = records.filter((r) => r.type === 'bike').length;

    // Peak hour from filtered records
    const hourCounts = new Map<number, number>();
    filteredRecords.forEach((r) => {
      hourCounts.set(r.hour, (hourCounts.get(r.hour) || 0) + 1);
    });
    let peakHour = 0;
    let peakCount = 0;
    hourCounts.forEach((count, hour) => {
      if (count > peakCount) {
        peakCount = count;
        peakHour = hour;
      }
    });

    // Hottest neighbourhood from filtered records
    const nhCounts = new Map<string, number>();
    filteredRecords.forEach((r) => {
      nhCounts.set(r.neighbourhood, (nhCounts.get(r.neighbourhood) || 0) + 1);
    });
    let hotspot = 'N/A';
    let hotspotCount = 0;
    nhCounts.forEach((count, nh) => {
      if (count > hotspotCount) {
        hotspotCount = count;
        hotspot = nh;
      }
    });

    // Daily avg from filtered records
    const days = new Set<string>();
    filteredRecords.forEach((r) => days.add(r.date));
    const dailyAvg = days.size > 0 ? (filteredRecords.length / days.size).toFixed(1) : '0';

    return { autoCount, bikeCount, peakHour, peakCount, hotspot, hotspotCount, dailyAvg, total: records.length };
  }, [records, filteredRecords]);

  const cards = [
    {
      label: 'Total (6 Months)',
      value: stats.total.toLocaleString(),
      icon: AlertTriangle,
      bgColor: 'bg-red-500/10',
      iconColor: 'text-red-400',
      sub: `${stats.autoCount} auto + ${stats.bikeCount} bike`,
    },
    {
      label: 'Auto Thefts',
      value: stats.autoCount.toLocaleString(),
      icon: Car,
      bgColor: 'bg-red-500/10',
      iconColor: 'text-red-400',
      sub: `${((stats.autoCount / Math.max(stats.total, 1)) * 100).toFixed(0)}% of total`,
    },
    {
      label: 'Bike Thefts',
      value: stats.bikeCount.toLocaleString(),
      icon: Bike,
      bgColor: 'bg-blue-500/10',
      iconColor: 'text-blue-400',
      sub: `${((stats.bikeCount / Math.max(stats.total, 1)) * 100).toFixed(0)}% of total`,
    },
    {
      label: 'Peak Hour',
      value: `${String(stats.peakHour).padStart(2, '0')}:00`,
      icon: Clock,
      bgColor: 'bg-amber-500/10',
      iconColor: 'text-amber-400',
      sub: `${stats.peakCount} incidents`,
    },
    {
      label: 'Daily Average',
      value: stats.dailyAvg,
      icon: TrendingUp,
      bgColor: 'bg-green-500/10',
      iconColor: 'text-green-400',
      sub: 'per day',
    },
    {
      label: '#1 Hotspot',
      value: stats.hotspot.length > 15 ? stats.hotspot.slice(0, 13) + '…' : stats.hotspot,
      icon: MapPin,
      bgColor: 'bg-purple-500/10',
      iconColor: 'text-purple-400',
      sub: `${stats.hotspotCount} incidents`,
    },
  ];

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
      {cards.map((card) => (
        <div
          key={card.label}
          className="bg-slate-800/50 backdrop-blur border border-slate-700/30 rounded-xl p-3 hover:border-slate-600/50 transition-all duration-300 group"
        >
          <div className="flex items-center justify-between mb-2">
            <div className={`w-8 h-8 rounded-lg ${card.bgColor} flex items-center justify-center group-hover:scale-110 transition-transform`}>
              <card.icon className={`w-4 h-4 ${card.iconColor}`} />
            </div>
          </div>
          <div className="text-lg font-bold text-white leading-tight">{card.value}</div>
          <div className="text-[10px] text-slate-400 mt-0.5">{card.label}</div>
          {card.sub && (
            <div className="text-[9px] text-slate-500 mt-0.5">{card.sub}</div>
          )}
        </div>
      ))}
    </div>
  );
}
