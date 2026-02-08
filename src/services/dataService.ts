import type { TheftRecord } from '../types';

// ──────────────────────────────────────────────
// CORRECT ENDPOINTS: Use dedicated Auto Theft endpoint (NOT the MCI aggregate)
// The MCI endpoint requires MCI_CATEGORY filter which often fails.
// The dedicated Auto_Theft endpoint contains ONLY auto theft records.
// ──────────────────────────────────────────────
const AUTO_THEFT_API =
  'https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/Auto_Theft_Open_Data/FeatureServer/0/query';
const BIKE_THEFT_API =
  'https://services.arcgis.com/S9th0jAJ7bqgIRjw/arcgis/rest/services/Bicycle_Thefts_Open_Data/FeatureServer/0/query';

const PAGE_SIZE = 2000;

function sixMonthsAgoDate(): Date {
  const d = new Date();
  d.setMonth(d.getMonth() - 6);
  d.setHours(0, 0, 0, 0);
  return d;
}

function currentYear(): number {
  return new Date().getFullYear();
}

function monthNameToNum(m: string | number): number {
  if (typeof m === 'number') return m;
  const months: Record<string, number> = {
    January: 1, February: 2, March: 3, April: 4, May: 5, June: 6,
    July: 7, August: 8, September: 9, October: 10, November: 11, December: 12,
  };
  return months[m] || 1;
}

function parseEpochDate(val: unknown): Date | null {
  if (typeof val === 'number' && val > 1_000_000_000) {
    return new Date(val > 1e12 ? val : val * 1000);
  }
  if (typeof val === 'string' && val.length >= 10) {
    const d = new Date(val);
    if (!isNaN(d.getTime())) return d;
  }
  return null;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function parseRecord(f: any, theftType: 'auto' | 'bike'): TheftRecord | null {
  const a = f.attributes || f;
  const lat = f.geometry?.y ?? a.LAT_WGS84 ?? a.Y ?? 0;
  const lng = f.geometry?.x ?? a.LONG_WGS84 ?? a.X ?? 0;

  // Discard invalid coordinates
  if ((lat === 0 && lng === 0) || lat == null || lng == null) return null;
  const latNum = Number(lat);
  const lngNum = Number(lng);
  if (isNaN(latNum) || isNaN(lngNum)) return null;
  if (latNum < 41 || latNum > 57 || lngNum < -95 || lngNum > -73) return null;

  // Parse date
  const rawDate = a.OCC_DATE ?? a.REPORT_DATE ?? '';
  const parsedDate = parseEpochDate(rawDate);
  const rawYear = a.OCC_YEAR || currentYear();
  const rawMonth = a.OCC_MONTH != null ? monthNameToNum(a.OCC_MONTH) : (parsedDate ? parsedDate.getMonth() + 1 : 1);
  const rawDay = a.OCC_DAY || (parsedDate ? parsedDate.getDate() : 1);
  const hour = a.OCC_HOUR ?? 12;

  let dateStr: string;
  let year: number;
  let month: number;
  let day: number;

  if (parsedDate && !isNaN(parsedDate.getTime())) {
    year = parsedDate.getFullYear();
    month = parsedDate.getMonth() + 1;
    day = parsedDate.getDate();
    dateStr = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
  } else {
    year = Number(rawYear) || currentYear();
    month = Number(rawMonth) || 1;
    day = Number(rawDay) || 1;
    dateStr = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
  }

  // 6-month enforcement: discard records older than 6 months
  const cutoff = sixMonthsAgoDate();
  const recordDate = new Date(year, month - 1, day);
  if (recordDate < cutoff) return null;
  // Also discard future dates
  if (recordDate > new Date()) return null;

  const eventId = a.EVENT_UNIQUE_ID || a.OBJECTID || '';

  return {
    id: `${theftType}-${eventId}`,
    type: theftType,
    date: dateStr,
    year,
    month,
    day,
    hour: Number(hour) || 0,
    neighbourhood: String(a.NEIGHBOURHOOD_158 || a.NEIGHBOURHOOD_140 || a.HOOD_158 || a.NEIGHBOURHOOD || 'Unknown').trim(),
    premiseType: String(a.PREMISES_TYPE || a.PREMISE_TYPE || 'Unknown').trim(),
    lat: latNum,
    lng: lngNum,
    status: String(a.STATUS || 'Unknown').trim(),
  };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function fetchPaginated(url: string, where: string): Promise<any[]> {
  const allFeatures: unknown[] = [];
  let offset = 0;

  // eslint-disable-next-line no-constant-condition
  while (true) {
    const params = new URLSearchParams({
      where,
      outFields: '*',
      outSR: '4326',
      f: 'json',
      resultRecordCount: String(PAGE_SIZE),
      resultOffset: String(offset),
      // NO orderByFields — critical to avoid API 400 errors
    });

    const resp = await fetch(`${url}?${params.toString()}`);
    if (!resp.ok) throw new Error(`API returned ${resp.status}`);
    const json = await resp.json();
    if (json.error) throw new Error(json.error.message || 'ArcGIS API Error');

    const features = json.features || [];
    if (features.length === 0) break;

    allFeatures.push(...features);
    offset += PAGE_SIZE;

    // Stop if no more pages
    if (!json.exceededTransferLimit && features.length < PAGE_SIZE) break;

    // Safety: don't fetch more than 50 pages (100k records)
    if (offset >= PAGE_SIZE * 50) break;
  }

  return allFeatures;
}

/**
 * Try multiple WHERE clause strategies in order until one works.
 * This mirrors what the Python scraper does.
 */
async function fetchWithFallback(url: string): Promise<unknown[]> {
  const year = currentYear();
  const cutoffYear = year - 1;

  // Strategy list: most selective to broadest
  const strategies: Array<{ where: string; label: string }> = [
    { where: `OCC_YEAR >= ${cutoffYear}`, label: `OCC_YEAR >= ${cutoffYear}` },
    { where: `OCC_YEAR >= ${year}`, label: `OCC_YEAR >= ${year} (current year only)` },
    { where: '1=1', label: 'unfiltered (all records, will filter in JS)' },
  ];

  for (const { where, label } of strategies) {
    try {
      console.log(`  [dataService] Trying: ${label}`);
      const features = await fetchPaginated(url, where);
      if (features.length > 0) {
        console.log(`  [dataService] ✅ Got ${features.length} features with: ${label}`);
        return features;
      }
      console.log(`  [dataService] 0 records with: ${label}, trying next...`);
    } catch (err) {
      console.warn(`  [dataService] Strategy "${label}" failed:`, err);
    }
  }

  throw new Error('All API query strategies failed');
}

async function fetchAutoTheftsLive(): Promise<TheftRecord[]> {
  console.log('[dataService] Fetching AUTO thefts from dedicated endpoint...');
  const features = await fetchWithFallback(AUTO_THEFT_API);
  const records = features
    .map((f) => parseRecord(f, 'auto'))
    .filter((r): r is TheftRecord => r !== null);
  console.log(`[dataService] Auto thefts parsed: ${records.length} valid records`);
  return records;
}

async function fetchBikeTheftsLive(): Promise<TheftRecord[]> {
  console.log('[dataService] Fetching BIKE thefts from dedicated endpoint...');
  const features = await fetchWithFallback(BIKE_THEFT_API);
  const records = features
    .map((f) => parseRecord(f, 'bike'))
    .filter((r): r is TheftRecord => r !== null);
  console.log(`[dataService] Bike thefts parsed: ${records.length} valid records`);
  return records;
}

async function fetchStaticJSON(path: string): Promise<TheftRecord[]> {
  const resp = await fetch(path);
  if (!resp.ok) throw new Error(`Static file not found: ${path}`);
  const data: TheftRecord[] = await resp.json();
  // Enforce 6-month window on static data too
  const cutoff = sixMonthsAgoDate();
  const now = new Date();
  return data.filter((r) => {
    const d = new Date(r.year, r.month - 1, r.day);
    return d >= cutoff && d <= now;
  });
}

function deduplicateRecords(records: TheftRecord[]): TheftRecord[] {
  const seen = new Set<string>();
  const result: TheftRecord[] = [];
  for (const r of records) {
    const key = `${r.type}-${r.date}-${r.lat.toFixed(5)}-${r.lng.toFixed(5)}-${r.hour}-${r.neighbourhood}`;
    if (!seen.has(key)) {
      seen.add(key);
      result.push(r);
    }
  }
  return result;
}

function ensureUniqueIds(records: TheftRecord[]): TheftRecord[] {
  const idCount = new Map<string, number>();
  return records.map((r) => {
    const count = idCount.get(r.id) || 0;
    idCount.set(r.id, count + 1);
    if (count > 0) {
      return { ...r, id: `${r.id}-${count}` };
    }
    return r;
  });
}

export async function fetchAllData(): Promise<{
  records: TheftRecord[];
  source: 'static' | 'live';
}> {
  // Strategy: Static First, Live Fallback
  try {
    const [auto, bike] = await Promise.all([
      fetchStaticJSON('/data/auto_thefts.json'),
      fetchStaticJSON('/data/bike_thefts.json'),
    ]);
    if (auto.length === 0 && bike.length === 0) {
      throw new Error('Static files are empty');
    }
    let records = deduplicateRecords([...auto, ...bike]);
    records = ensureUniqueIds(records);
    records.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
    console.log(`[dataService] Loaded ${records.length} records from static files (auto: ${auto.length}, bike: ${bike.length})`);
    return { records, source: 'static' };
  } catch (staticErr) {
    console.log('[dataService] Static files unavailable, falling back to live API...', staticErr);
    // Live fallback
    const [auto, bike] = await Promise.all([
      fetchAutoTheftsLive(),
      fetchBikeTheftsLive(),
    ]);
    let records = deduplicateRecords([...auto, ...bike]);
    records = ensureUniqueIds(records);
    records.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
    console.log(`[dataService] Loaded ${records.length} records from live API (auto: ${auto.length}, bike: ${bike.length})`);
    return { records, source: 'live' };
  }
}

export async function fetchLiveDataOnly(): Promise<{
  records: TheftRecord[];
  source: 'live';
}> {
  const [auto, bike] = await Promise.all([
    fetchAutoTheftsLive(),
    fetchBikeTheftsLive(),
  ]);
  let records = deduplicateRecords([...auto, ...bike]);
  records = ensureUniqueIds(records);
  records.sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());
  console.log(`[dataService] Live test: ${records.length} total (auto: ${auto.length}, bike: ${bike.length})`);
  return { records, source: 'live' };
}
