import { useRef, useEffect, useState, useCallback } from 'react';
import { useStore } from '../store/useStore';
import type { TheftRecord } from '../types';
import { ZoomIn, ZoomOut, RotateCcw, Crosshair } from 'lucide-react';

// Toronto bounds
const TORONTO_CENTER = { lat: 43.7, lng: -79.4 };
const MIN_ZOOM = 0.4;
const MAX_ZOOM = 12;

interface MapState {
  centerLat: number;
  centerLng: number;
  zoom: number;
}

function latlngToPixel(
  lat: number,
  lng: number,
  state: MapState,
  width: number,
  height: number
): [number, number] {
  const scale = state.zoom * 800;
  const x = width / 2 + (lng - state.centerLng) * scale;
  const y = height / 2 - (lat - state.centerLat) * scale * 1.3;
  return [x, y];
}

function pixelToLatLng(
  px: number,
  py: number,
  state: MapState,
  width: number,
  height: number
): { lat: number; lng: number } {
  const scale = state.zoom * 800;
  const lng = (px - width / 2) / scale + state.centerLng;
  const lat = -(py - height / 2) / (scale * 1.3) + state.centerLat;
  return { lat, lng };
}

export function TheftMap() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const animFrameRef = useRef<number>(0);
  const { viewMode, selectedRecord, setSelectedRecord } = useStore();
  const filteredRecords = useStore((s) => s.filteredRecords());

  const [mapState, setMapState] = useState<MapState>({
    centerLat: TORONTO_CENTER.lat,
    centerLng: TORONTO_CENTER.lng,
    zoom: 2.5,
  });
  const [isDragging, setIsDragging] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });
  const [hoveredRecord, setHoveredRecord] = useState<TheftRecord | null>(null);
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });
  const [dimensions, setDimensions] = useState({ width: 800, height: 600 });

  // Resize observer
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    const ro = new ResizeObserver((entries) => {
      const { width, height } = entries[0].contentRect;
      setDimensions({ width: Math.floor(width), height: Math.floor(height) });
    });
    ro.observe(container);
    return () => ro.disconnect();
  }, []);

  // Draw map
  const draw = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const { width, height } = dimensions;
    const dpr = window.devicePixelRatio || 1;
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    // Background
    const bgGrad = ctx.createLinearGradient(0, 0, 0, height);
    bgGrad.addColorStop(0, '#0f172a');
    bgGrad.addColorStop(1, '#1e293b');
    ctx.fillStyle = bgGrad;
    ctx.fillRect(0, 0, width, height);

    // Grid lines
    ctx.strokeStyle = 'rgba(71, 85, 105, 0.15)';
    ctx.lineWidth = 0.5;
    const gridStep = 0.02;
    const topLeft = pixelToLatLng(0, 0, mapState, width, height);
    const bottomRight = pixelToLatLng(width, height, mapState, width, height);

    for (let lat = Math.floor(topLeft.lat / gridStep) * gridStep; lat >= bottomRight.lat; lat -= gridStep) {
      const [, y] = latlngToPixel(lat, 0, mapState, width, height);
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(width, y);
      ctx.stroke();
    }
    for (let lng = Math.floor(topLeft.lng / gridStep) * gridStep; lng <= bottomRight.lng; lng += gridStep) {
      const [x] = latlngToPixel(0, lng, mapState, width, height);
      ctx.beginPath();
      ctx.moveTo(x, 0);
      ctx.lineTo(x, height);
      ctx.stroke();
    }

    // Draw Toronto boundary approximation (simplified polygon)
    const torontoBoundary = [
      [43.855, -79.639], [43.855, -79.115], [43.58, -79.115],
      [43.58, -79.26], [43.60, -79.44], [43.59, -79.50],
      [43.60, -79.55], [43.63, -79.60], [43.70, -79.639],
    ];
    ctx.beginPath();
    torontoBoundary.forEach(([lat, lng], i) => {
      const [x, y] = latlngToPixel(lat, lng, mapState, width, height);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.closePath();
    ctx.fillStyle = 'rgba(30, 41, 59, 0.5)';
    ctx.fill();
    ctx.strokeStyle = 'rgba(100, 116, 139, 0.4)';
    ctx.lineWidth = 1.5;
    ctx.stroke();

    // Major roads (approximation)
    const roads = [
      [[43.86, -79.39], [43.58, -79.39]], // Yonge St
      [[43.73, -79.64], [43.73, -79.12]], // Bloor St / Danforth
      [[43.65, -79.64], [43.65, -79.12]], // Lakeshore
      [[43.78, -79.64], [43.78, -79.12]], // 401 approx
      [[43.86, -79.49], [43.58, -79.49]], // Bathurst
      [[43.86, -79.33], [43.58, -79.33]], // DVP approx
    ];
    ctx.strokeStyle = 'rgba(100, 116, 139, 0.2)';
    ctx.lineWidth = 1;
    roads.forEach((road) => {
      ctx.beginPath();
      road.forEach(([lat, lng], i) => {
        const [x, y] = latlngToPixel(lat, lng, mapState, width, height);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      });
      ctx.stroke();
    });

    // Lake Ontario
    const lake = [
      [43.63, -79.64], [43.60, -79.55], [43.59, -79.45],
      [43.60, -79.35], [43.58, -79.20], [43.58, -79.11],
      [43.50, -79.11], [43.50, -79.64],
    ];
    ctx.beginPath();
    lake.forEach(([lat, lng], i) => {
      const [x, y] = latlngToPixel(lat, lng, mapState, width, height);
      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });
    ctx.closePath();
    const lakeGrad = ctx.createLinearGradient(0, height * 0.7, 0, height);
    lakeGrad.addColorStop(0, 'rgba(14, 116, 144, 0.15)');
    lakeGrad.addColorStop(1, 'rgba(14, 116, 144, 0.05)');
    ctx.fillStyle = lakeGrad;
    ctx.fill();
    ctx.strokeStyle = 'rgba(14, 116, 144, 0.3)';
    ctx.lineWidth = 1;
    ctx.stroke();

    // Label for Lake Ontario
    const [lakeX, lakeY] = latlngToPixel(43.55, -79.38, mapState, width, height);
    ctx.fillStyle = 'rgba(14, 116, 144, 0.5)';
    ctx.font = `${Math.max(10, 12 * mapState.zoom / 2.5)}px system-ui`;
    ctx.textAlign = 'center';
    ctx.fillText('Lake Ontario', lakeX, lakeY);

    // Neighbourhood labels at higher zoom
    if (mapState.zoom > 2) {
      const labels = [
        [43.77, -79.41, 'North York'],
        [43.72, -79.26, 'Scarborough'],
        [43.71, -79.52, 'Etobicoke'],
        [43.66, -79.38, 'Downtown'],
        [43.69, -79.35, 'East York'],
        [43.65, -79.44, 'Liberty Village'],
        [43.68, -79.30, 'The Beaches'],
        [43.76, -79.33, 'Don Mills'],
      ];
      ctx.fillStyle = 'rgba(148, 163, 184, 0.45)';
      ctx.font = `${Math.max(9, 10 * mapState.zoom / 2.5)}px system-ui`;
      ctx.textAlign = 'center';
      labels.forEach(([lat, lng, name]) => {
        const [x, y] = latlngToPixel(lat as number, lng as number, mapState, width, height);
        if (x > 0 && x < width && y > 0 && y < height) {
          ctx.fillText(name as string, x, y);
        }
      });
    }

    if (viewMode === 'heatmap') {
      // Heatmap rendering
      const cellSize = Math.max(3, 15 / mapState.zoom * 2.5);
      const grid = new Map<string, number>();
      let maxCount = 0;

      filteredRecords.forEach((r) => {
        const [x, y] = latlngToPixel(r.lat, r.lng, mapState, width, height);
        if (x < -50 || x > width + 50 || y < -50 || y > height + 50) return;
        const gx = Math.floor(x / cellSize);
        const gy = Math.floor(y / cellSize);
        const key = `${gx},${gy}`;
        const count = (grid.get(key) || 0) + 1;
        grid.set(key, count);
        if (count > maxCount) maxCount = count;
      });

      if (maxCount > 0) {
        grid.forEach((count, key) => {
          const [gxS, gyS] = key.split(',');
          const gx = parseInt(gxS);
          const gy = parseInt(gyS);
          const intensity = count / maxCount;
          const radius = cellSize * (1 + intensity * 2);

          const gradient = ctx.createRadialGradient(
            gx * cellSize + cellSize / 2,
            gy * cellSize + cellSize / 2,
            0,
            gx * cellSize + cellSize / 2,
            gy * cellSize + cellSize / 2,
            radius
          );

          if (intensity > 0.7) {
            gradient.addColorStop(0, `rgba(239, 68, 68, ${0.6 * intensity})`);
            gradient.addColorStop(0.4, `rgba(249, 115, 22, ${0.4 * intensity})`);
            gradient.addColorStop(1, 'rgba(249, 115, 22, 0)');
          } else if (intensity > 0.3) {
            gradient.addColorStop(0, `rgba(249, 115, 22, ${0.5 * intensity})`);
            gradient.addColorStop(0.5, `rgba(234, 179, 8, ${0.3 * intensity})`);
            gradient.addColorStop(1, 'rgba(234, 179, 8, 0)');
          } else {
            gradient.addColorStop(0, `rgba(34, 197, 94, ${0.4 * intensity})`);
            gradient.addColorStop(0.5, `rgba(34, 197, 94, ${0.2 * intensity})`);
            gradient.addColorStop(1, 'rgba(34, 197, 94, 0)');
          }

          ctx.fillStyle = gradient;
          ctx.beginPath();
          ctx.arc(
            gx * cellSize + cellSize / 2,
            gy * cellSize + cellSize / 2,
            radius,
            0,
            Math.PI * 2
          );
          ctx.fill();
        });
      }
    } else {
      // Scatter plot rendering
      const pointRadius = Math.max(2, 3 * mapState.zoom / 2.5);
      filteredRecords.forEach((r) => {
        const [x, y] = latlngToPixel(r.lat, r.lng, mapState, width, height);
        if (x < -10 || x > width + 10 || y < -10 || y > height + 10) return;

        const isHovered = hoveredRecord?.id === r.id;
        const isSelected = selectedRecord?.id === r.id;
        const color = r.type === 'auto' ? 'rgba(239, 68, 68, 0.7)' : 'rgba(59, 130, 246, 0.7)';
        const glowColor = r.type === 'auto' ? 'rgba(239, 68, 68, 0.2)' : 'rgba(59, 130, 246, 0.2)';

        // Glow
        if (isHovered || isSelected) {
          ctx.beginPath();
          ctx.arc(x, y, pointRadius * 4, 0, Math.PI * 2);
          ctx.fillStyle = glowColor;
          ctx.fill();
        }

        ctx.beginPath();
        ctx.arc(x, y, isHovered || isSelected ? pointRadius * 1.8 : pointRadius, 0, Math.PI * 2);
        ctx.fillStyle = color;
        ctx.fill();

        if (isHovered || isSelected) {
          ctx.strokeStyle = r.type === 'auto' ? '#ef4444' : '#3b82f6';
          ctx.lineWidth = 1.5;
          ctx.stroke();
        }
      });
    }

    // Scale bar
    ctx.fillStyle = 'rgba(148, 163, 184, 0.6)';
    ctx.font = '10px system-ui';
    ctx.textAlign = 'left';
    const scaleKm = (1 / (mapState.zoom * 800)) * 100 * 111;
    ctx.fillText(`~${scaleKm.toFixed(1)} km`, 15, height - 15);
    ctx.strokeStyle = 'rgba(148, 163, 184, 0.4)';
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(15, height - 22);
    ctx.lineTo(115, height - 22);
    ctx.stroke();

    // Compass
    ctx.fillStyle = 'rgba(148, 163, 184, 0.5)';
    ctx.font = 'bold 12px system-ui';
    ctx.textAlign = 'center';
    ctx.fillText('N', width - 25, 25);
    ctx.beginPath();
    ctx.moveTo(width - 25, 28);
    ctx.lineTo(width - 25, 45);
    ctx.strokeStyle = 'rgba(148, 163, 184, 0.4)';
    ctx.lineWidth = 1.5;
    ctx.stroke();
    ctx.beginPath();
    ctx.moveTo(width - 25, 28);
    ctx.lineTo(width - 22, 34);
    ctx.lineTo(width - 28, 34);
    ctx.closePath();
    ctx.fillStyle = 'rgba(148, 163, 184, 0.5)';
    ctx.fill();

  }, [dimensions, mapState, filteredRecords, viewMode, hoveredRecord, selectedRecord]);

  useEffect(() => {
    animFrameRef.current = requestAnimationFrame(draw);
    return () => cancelAnimationFrame(animFrameRef.current);
  }, [draw]);

  // Mouse handlers
  const handleMouseDown = (e: React.MouseEvent) => {
    setIsDragging(true);
    setDragStart({ x: e.clientX, y: e.clientY });
  };

  const handleMouseMove = (e: React.MouseEvent) => {
    const rect = canvasRef.current?.getBoundingClientRect();
    if (!rect) return;
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    setMousePos({ x: mx, y: my });

    if (isDragging) {
      const dx = e.clientX - dragStart.x;
      const dy = e.clientY - dragStart.y;
      const scale = mapState.zoom * 800;
      setMapState((prev) => ({
        ...prev,
        centerLng: prev.centerLng - dx / scale,
        centerLat: prev.centerLat + dy / (scale * 1.3),
      }));
      setDragStart({ x: e.clientX, y: e.clientY });
    } else if (viewMode === 'scatter') {
      // Find hovered point
      let found: TheftRecord | null = null;
      const threshold = 8;
      for (const r of filteredRecords) {
        const [px, py] = latlngToPixel(r.lat, r.lng, mapState, dimensions.width, dimensions.height);
        const dist = Math.sqrt((mx - px) ** 2 + (my - py) ** 2);
        if (dist < threshold) {
          found = r;
          break;
        }
      }
      setHoveredRecord(found);
    }
  };

  const handleMouseUp = () => setIsDragging(false);
  const handleMouseLeave = () => {
    setIsDragging(false);
    setHoveredRecord(null);
  };

  const handleClick = (e: React.MouseEvent) => {
    if (viewMode !== 'scatter') return;
    const rect = canvasRef.current?.getBoundingClientRect();
    if (!rect) return;
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    const threshold = 10;
    for (const r of filteredRecords) {
      const [px, py] = latlngToPixel(r.lat, r.lng, mapState, dimensions.width, dimensions.height);
      if (Math.sqrt((mx - px) ** 2 + (my - py) ** 2) < threshold) {
        setSelectedRecord(r);
        return;
      }
    }
    setSelectedRecord(null);
  };

  const handleWheel = (e: React.WheelEvent) => {
    e.preventDefault();
    const delta = e.deltaY > 0 ? -0.2 : 0.2;
    setMapState((prev) => ({
      ...prev,
      zoom: Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, prev.zoom + delta * prev.zoom * 0.15)),
    }));
  };

  const zoomIn = () =>
    setMapState((prev) => ({ ...prev, zoom: Math.min(MAX_ZOOM, prev.zoom * 1.3) }));
  const zoomOut = () =>
    setMapState((prev) => ({ ...prev, zoom: Math.max(MIN_ZOOM, prev.zoom / 1.3) }));
  const resetView = () =>
    setMapState({ centerLat: TORONTO_CENTER.lat, centerLng: TORONTO_CENTER.lng, zoom: 2.5 });

  return (
    <div ref={containerRef} className="relative w-full h-full min-h-[400px] rounded-xl overflow-hidden border border-slate-700/50 shadow-2xl">
      <canvas
        ref={canvasRef}
        className={`w-full h-full ${isDragging ? 'cursor-grabbing' : 'cursor-grab'}`}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseLeave}
        onClick={handleClick}
        onWheel={handleWheel}
      />

      {/* Map Controls */}
      <div className="absolute top-3 right-3 flex flex-col gap-1">
        <button onClick={zoomIn} className="w-8 h-8 bg-slate-800/90 backdrop-blur rounded-lg flex items-center justify-center text-slate-300 hover:text-white hover:bg-slate-700 transition-colors border border-slate-700/50">
          <ZoomIn className="w-4 h-4" />
        </button>
        <button onClick={zoomOut} className="w-8 h-8 bg-slate-800/90 backdrop-blur rounded-lg flex items-center justify-center text-slate-300 hover:text-white hover:bg-slate-700 transition-colors border border-slate-700/50">
          <ZoomOut className="w-4 h-4" />
        </button>
        <button onClick={resetView} className="w-8 h-8 bg-slate-800/90 backdrop-blur rounded-lg flex items-center justify-center text-slate-300 hover:text-white hover:bg-slate-700 transition-colors border border-slate-700/50">
          <RotateCcw className="w-4 h-4" />
        </button>
      </div>

      {/* Crosshair center indicator */}
      <div className="absolute top-3 left-3 flex flex-col gap-1">
        <div className="bg-slate-800/80 backdrop-blur rounded-lg px-2 py-1 flex items-center gap-1 border border-slate-700/50">
          <Crosshair className="w-3 h-3 text-slate-400" />
          <span className="text-[10px] text-slate-400 font-mono">
            {mapState.centerLat.toFixed(4)}, {mapState.centerLng.toFixed(4)}
          </span>
        </div>
        <div className="bg-slate-800/80 backdrop-blur rounded-lg px-2 py-1 flex items-center gap-1.5 border border-slate-700/50">
          <span className="text-[10px] text-cyan-400">📊</span>
          <span className="text-[10px] text-slate-400">
            {filteredRecords.length.toLocaleString()} pts • Last 6 months
          </span>
        </div>
      </div>

      {/* Legend */}
      <div className="absolute bottom-3 right-3 bg-slate-800/90 backdrop-blur rounded-lg px-3 py-2 border border-slate-700/50">
        {viewMode === 'scatter' ? (
          <div className="flex flex-col gap-1">
            <div className="flex items-center gap-2">
              <div className="w-2.5 h-2.5 rounded-full bg-red-500" />
              <span className="text-[10px] text-slate-300">Auto Theft</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="w-2.5 h-2.5 rounded-full bg-blue-500" />
              <span className="text-[10px] text-slate-300">Bike Theft</span>
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-1">
            <span className="text-[10px] text-slate-400 font-medium">Risk Density</span>
            <div className="flex items-center gap-1">
              <div className="w-14 h-2 rounded-full bg-gradient-to-r from-green-500 via-yellow-500 to-red-500" />
            </div>
            <div className="flex justify-between">
              <span className="text-[9px] text-slate-500">Low</span>
              <span className="text-[9px] text-slate-500">High</span>
            </div>
          </div>
        )}
      </div>

      {/* Tooltip */}
      {hoveredRecord && viewMode === 'scatter' && (
        <div
          className="absolute z-50 pointer-events-none bg-slate-900/95 backdrop-blur rounded-lg px-3 py-2 border border-slate-600/50 shadow-xl"
          style={{ left: mousePos.x + 15, top: mousePos.y - 10 }}
        >
          <div className="flex items-center gap-2 mb-1">
            <span className="text-sm">{hoveredRecord.type === 'auto' ? '🚗' : '🚲'}</span>
            <span className="text-xs font-semibold text-white capitalize">{hoveredRecord.type} Theft</span>
          </div>
          <div className="text-[10px] text-slate-400 space-y-0.5">
            <div>📍 {hoveredRecord.neighbourhood}</div>
            <div>🏢 {hoveredRecord.premiseType}</div>
            <div>📅 {hoveredRecord.year}-{String(hoveredRecord.month).padStart(2, '0')}-{String(hoveredRecord.day).padStart(2, '0')}</div>
            <div>🕐 {String(hoveredRecord.hour).padStart(2, '0')}:00</div>
          </div>
        </div>
      )}
    </div>
  );
}
