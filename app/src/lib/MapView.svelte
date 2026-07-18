<script module lang="ts">
  export interface CellRow { cell: string; value: number; }
</script>

<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import maplibregl from 'maplibre-gl';
  import { MapboxOverlay } from '@deck.gl/mapbox';
  import { H3HexagonLayer } from '@deck.gl/geo-layers';
  import { GeoJsonLayer } from '@deck.gl/layers';

  let {
    rows        = [] as CellRow[],
    loading     = false,
    metricLabel = 'value',
    outlineWKT  = '' as string,
    bbox        = null as [[number,number],[number,number]] | null,
    dark        = false,
  } = $props();

  // ── Basemap style using OBIS vector tiles ────────────────────────────────
  const SOURCES = {
    land_polygons: {
      type: 'vector',
      tiles: ['https://tiles.obis.org/land_tiles/{z}/{x}/{y}.pbf'],
      minzoom: 0, maxzoom: 14,
    },
    coastlines: {
      type: 'vector',
      tiles: ['https://tiles.obis.org/coastlines_tiles/{z}/{x}/{y}.pbf'],
      minzoom: 0, maxzoom: 14,
    },
  };

  const BASEMAP_STYLE = {
    version: 8,
    glyphs: 'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf',
    sources: SOURCES,
    layers: dark ? [
      { id: 'background', type: 'background',
        paint: { 'background-color': '#0f1923' } },
      { id: 'land_polygons', type: 'fill',
        source: 'land_polygons', 'source-layer': 'land',
        paint: { 'fill-color': '#1e2e42', 'fill-opacity': 1 } },
      { id: 'coastlines', type: 'line',
        source: 'coastlines', 'source-layer': 'coastlines',
        paint: { 'line-color': '#3d5470', 'line-width': 0.5, 'line-opacity': 0.9 } },
    ] : [
      { id: 'background', type: 'background',
        paint: { 'background-color': '#e8ecf0' } },
      { id: 'land_polygons', type: 'fill',
        source: 'land_polygons', 'source-layer': 'land',
        paint: { 'fill-color': '#f8fafc', 'fill-opacity': 1 } },
      { id: 'coastlines', type: 'line',
        source: 'coastlines', 'source-layer': 'coastlines',
        paint: { 'line-color': '#334155', 'line-width': 0.4, 'line-opacity': 0.85 } },
    ],
  } as any;

  const RAMP: [number,number,number][] = [
    [8,  104, 172],
    [14, 168, 189],
    [68, 214, 153],
    [246,209,  69],
    [220,  50,  32],
  ];

  function toColor(v: number, min: number, max: number): [number,number,number,number] {
    if (max === min) return [...RAMP[RAMP.length-1], 210] as any;
    const t = (Math.log1p(v) - Math.log1p(min)) / (Math.log1p(max) - Math.log1p(min));
    const s = Math.max(0, Math.min(1, t)) * (RAMP.length - 1);
    const lo = Math.floor(s), hi = Math.min(lo + 1, RAMP.length - 1), f = s - lo;
    return [
      Math.round(RAMP[lo][0] + f * (RAMP[hi][0] - RAMP[lo][0])),
      Math.round(RAMP[lo][1] + f * (RAMP[hi][1] - RAMP[lo][1])),
      Math.round(RAMP[lo][2] + f * (RAMP[hi][2] - RAMP[lo][2])),
      210,
    ];
  }

  // ── Stats for colour scale ────────────────────────────────────────────────
  const stats = $derived(() => {
    if (!rows.length) return { min: 0, max: 1 };
    let min = Infinity, max = -Infinity;
    for (const r of rows) {
      if (r.value < min) min = r.value;
      if (r.value > max) max = r.value;
    }
    return { min, max };
  });

  // ── Auto-zoom via bbox prop ────────────────────────────────────────────────
  $effect(() => {
    if (bbox && map) {
      map.fitBounds(bbox, { padding: 60, maxZoom: 10, duration: 700 });
    }
  });

  // ── WKT outline → GeoJSON ─────────────────────────────────────────────────
  function wktToGeoJSON(wkt: string): GeoJSON.Geometry | null {
    try {
      wkt = wkt.trim();
      if (wkt.startsWith('POLYGON')) {
        return { type: 'Polygon', coordinates: parsePolygonCoords(wkt.replace(/^POLYGON\s*/, '')) };
      }
      if (wkt.startsWith('MULTIPOLYGON')) {
        const inner = wkt.replace(/^MULTIPOLYGON\s*\(\(/, '').replace(/\)\)$/, '');
        const polys = inner.split(')), ((').map(p => parsePolygonCoords('((' + p + '))'));
        return { type: 'MultiPolygon', coordinates: polys };
      }
    } catch { /* ignore */ }
    return null;
  }

  function parsePolygonCoords(s: string): number[][][] {
    const body = s.replace(/^\s*\(\s*/, '').replace(/\s*\)\s*$/, '');
    return body.split(/\)\s*,\s*\(/).map(ring =>
      ring.replace(/[()]/g, '').trim().split(/\s*,\s*/).map(pair => {
        const [lng, lat] = pair.trim().split(/\s+/).map(Number);
        return [lng, lat];
      })
    );
  }

  const outlineGeoJSON = $derived(() => {
    if (!outlineWKT) return null;
    const geom = wktToGeoJSON(outlineWKT);
    if (!geom) return null;
    return { type: 'Feature', geometry: geom, properties: {} } as GeoJSON.Feature;
  });

  // ── Tooltip ───────────────────────────────────────────────────────────────
  let tooltip = $state<{ x: number; y: number; cell: string; value: number } | null>(null);

  // ── Map refs ──────────────────────────────────────────────────────────────
  let container: HTMLDivElement;
  let map:         maplibregl.Map | null = null;
  let overlay:     MapboxOverlay  | null = null;
  let overlayReady = $state(false);

  function buildLayers() {
    const { min, max } = stats();

    const layers: any[] = [
      new H3HexagonLayer<CellRow>({
        id:           'cells',
        data:         rows,
        getHexagon:   d => d.cell,
        getFillColor: d => toColor(d.value, min, max),
        extruded:  false,
        coverage:  0.92,
        pickable:  true,
        onHover: (info: any) => {
          if (info.object) {
            const rect = container.getBoundingClientRect();
            tooltip = {
              x: info.x + rect.left,
              y: info.y + rect.top,
              cell: info.object.cell,
              value: info.object.value,
            };
          } else {
            tooltip = null;
          }
        },
        updateTriggers: { getFillColor: [rows.length, min, max] },
      }),
    ];

    const outline = outlineGeoJSON();
    if (outline) {
      layers.push(new GeoJsonLayer({
        id:   'area-outline',
        data: outline,
        stroked:            true,
        filled:             true,
        getFillColor:       [56, 189, 248, 18],
        getLineColor:       [56, 189, 248, 200],
        getLineWidth:       2,
        lineWidthMinPixels: 1.5,
        lineWidthMaxPixels: 3,
      }));
    }

    return layers;
  }

  $effect(() => {
    void rows;
    void overlayReady;
    void outlineWKT;
    if (overlay) {
      overlay.setProps({ layers: buildLayers() });
    }
  });

  $effect(() => {
    const outline = outlineGeoJSON();
    if (!outline || !map) return;
    const coords: number[][] = [];
    const g = outline.geometry;
    if (g.type === 'Polygon')      g.coordinates.flat().forEach(c => coords.push(c));
    if (g.type === 'MultiPolygon') g.coordinates.flat(2).forEach(c => coords.push(c));
    if (!coords.length) return;
    const lngs = coords.map(c => c[0]);
    const lats  = coords.map(c => c[1]);
    map.fitBounds(
      [[Math.min(...lngs), Math.min(...lats)], [Math.max(...lngs), Math.max(...lats)]],
      { padding: 80, maxZoom: 10, duration: 600 }
    );
  });

  // ── Legend helpers ────────────────────────────────────────────────────────
  function fmtLegend(n: number): string {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(0) + 'k';
    return n.toLocaleString();
  }

  const legendTicks = $derived(() => {
    const { min, max } = stats();
    if (max === min) return [min];
    const logMin = Math.log1p(min), logMax = Math.log1p(max);
    return [0, 0.25, 0.5, 0.75, 1].map(t =>
      Math.round(Math.expm1(logMin + t * (logMax - logMin)))
    );
  });

  const legendGradient = $derived(() =>
    'linear-gradient(to right, ' +
    RAMP.map((c, i) => `rgb(${c.join(',')}) ${Math.round(i / (RAMP.length - 1) * 100)}%`).join(', ') +
    ')'
  );

  function fmt(n: number) {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'k';
    return n.toLocaleString();
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────
  onMount(() => {
    map = new maplibregl.Map({
      container,
      style:     BASEMAP_STYLE,
      center:    [0, 20],
      zoom:      2,
      antialias: true,
    } as any);
    map.addControl(new maplibregl.NavigationControl(), 'top-right');
    map.addControl(new maplibregl.ScaleControl({ unit: 'metric' }), 'bottom-left');
    map.on('load', () => {
      overlay = new MapboxOverlay({ interleaved: true, layers: [] });
      map!.addControl(overlay as any);
      overlayReady = true;
    });
  });

  onDestroy(() => { overlay?.finalize(); map?.remove(); });
</script>

<div class="root" bind:this={container} onmouseleave={() => { tooltip = null; }}></div>

{#if loading}
  <div class="veil">
    <div class="spinner"></div>
    <span>Loading overview…</span>
  </div>
{/if}

{#if rows.length > 0}
  <div class="legend">
    <div class="legend-label">{metricLabel} · log scale</div>
    <div class="legend-bar" style="background: {legendGradient()}"></div>
    <div class="legend-ticks">
      {#each legendTicks() as t}
        <span>{fmtLegend(t)}</span>
      {/each}
    </div>
  </div>
{/if}

{#if tooltip}
  <div class="tip" style="left:{tooltip.x + 14}px; top:{tooltip.y - 10}px">
    <div class="tip-row"><span>Cell</span><code>{tooltip.cell.slice(0, 15)}</code></div>
    <div class="tip-row"><span>{metricLabel}</span><strong>{fmt(tooltip.value)}</strong></div>
  </div>
{/if}

<style>
  .root { position: absolute; inset: 0; }

  .veil {
    position: absolute; inset: 0; z-index: 10;
    display: flex; flex-direction: column; align-items: center; justify-content: center;
    gap: .75rem; background: rgba(240,244,248,.75); backdrop-filter: blur(6px);
    color: #64748b; font-size: .8rem; font-family: 'IBM Plex Mono', monospace;
  }
  .spinner {
    width: 32px; height: 32px;
    border: 2px solid #e2e8f0; border-top-color: #0854a8;
    border-radius: 50%; animation: spin .75s linear infinite;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  .legend {
    position: absolute; bottom: 2rem; right: .75rem; width: 180px;
    background: rgba(255,255,255,.92); border: 1px solid rgba(0,0,0,.1);
    border-radius: 7px; padding: .55rem .7rem .45rem;
    display: flex; flex-direction: column; gap: .3rem;
    font-family: 'IBM Plex Mono', monospace;
    pointer-events: none; z-index: 5;
    box-shadow: 0 2px 8px rgba(0,0,0,.12);
  }
  .legend-label { font-size: .58rem; text-transform: uppercase; letter-spacing: .08em; color: #64748b; }
  .legend-bar   { height: 7px; border-radius: 3px; }
  .legend-ticks { display: flex; justify-content: space-between; font-size: .58rem; color: #94a3b8; }

  .tip {
    position: fixed; pointer-events: none; z-index: 50;
    background: rgba(255,255,255,.97); border: 1px solid rgba(0,80,160,.2);
    border-radius: 7px; padding: .5rem .8rem;
    font-family: 'IBM Plex Mono', monospace; font-size: .7rem;
    color: #334155; line-height: 1.9;
    box-shadow: 0 4px 16px rgba(0,0,0,.12);
  }
  .tip-row { display: flex; justify-content: space-between; gap: 1rem; align-items: baseline; }
  .tip-row span   { color: #94a3b8; font-size: .6rem; text-transform: uppercase; }
  .tip-row strong { color: #0854a8; }
  .tip-row code   { color: #334155; font-size: .65rem; }
</style>
