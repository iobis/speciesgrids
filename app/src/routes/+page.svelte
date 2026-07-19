<script lang="ts">
  import { onMount } from 'svelte';
  import { browser } from '$app/environment';
  import { dataState, loadOverview, loadPrecomputedQuery } from '$lib/data.svelte';
  import type { CellRow } from '$lib/MapView.svelte';
  import ExploreTab from '$lib/ExploreTab.svelte';
  import obislogo from '$lib/images/logo_simple.png';
  import ioclogo from '$lib/images/ioc_logo_black_2.svg';
  import { resolve } from '$app/paths';

  let MapView: any = $state(null);

  // ── View state ────────────────────────────────────────────────────────────
  type Tab = 'overview' | 'explore';
  let activeTab = $state<Tab>('overview');
  let rows      = $state<CellRow[]>([]);
  let loading   = $state(false);
  let error     = $state<string | null>(null);

  // ── Explore state ─────────────────────────────────────────────────────────
  const S3 = `'s3://obis-products/speciesgrids/h3_7/data.parquet'`;

  interface MapConfig { cellCol: string; valueCol: string; metricLabel: string; }
  interface ExploreQuery {
    id: string; title: string; description: string; sql: string;
    file: string; statLabel: string; statFn: (rows: any[]) => string;
    colorCol?: string;
    mapConfig?: MapConfig;
  }

  const EXPLORE_QUERIES: ExploreQuery[] = [
    {
      id: 'rare-species', title: 'Rare species',
      file: 'rare_species.arrow',
      description: 'Species with fewer than 5 total occurrence records across all H3 cells — the rarest observations in the dataset.',
      sql: `SELECT species, AphiaID, SUM(records)::INTEGER AS total_records,\n       COUNT(*)::INTEGER AS total_cells, MAX(max_year)::INTEGER AS last_seen\nFROM read_parquet(${S3})\nGROUP BY species, AphiaID\nHAVING SUM(records) < 5\nORDER BY total_records ASC, species ASC`,
      statLabel: 'species with < 5 records',
      statFn: r => fmtN(r.length),
      colorCol: 'total_records',
    },
    {
      id: 'lost-species', title: 'Potentially lost species',
      file: 'lost_species.arrow',
      description: 'Species with no recorded observation after 1965 — potentially data-deficient, range-shifted, or locally extinct.',
      sql: `SELECT species, AphiaID, MAX(max_year)::INTEGER AS last_seen,\n       SUM(records)::INTEGER AS total_records, COUNT(*)::INTEGER AS total_cells\nFROM read_parquet(${S3})\nGROUP BY species, AphiaID\nHAVING MAX(max_year) <= 1965\nORDER BY last_seen ASC, total_records DESC`,
      statLabel: 'species last seen before 1965',
      statFn: r => fmtN(r.length),
      colorCol: 'total_records',
    },
    {
      id: 'blue-whale', title: 'Blue whale records',
      file: 'blue_whale.arrow',
      description: 'All H3 cells where the blue whale has been observed, with record counts and observation time range.',
      sql: `SELECT cell, records AS record_count, min_year, max_year\nFROM read_parquet(${S3})\nWHERE species = 'Balaenoptera musculus'\nORDER BY records DESC`,
      statLabel: 'total occurrence records',
      statFn: r => fmtN(r.reduce((s: number, row: any) => s + Number(row.record_count), 0)),
      mapConfig: { cellCol: 'cell', valueCol: 'record_count', metricLabel: 'records' },
    },
    {
      id: 'top-places', title: 'Top 100 places',
      file: 'top_places.arrow',
      description: 'The 100 H3 resolution-4 cells (~300 km) with the most occurrence records.',
      sql: `SELECT h3_cell_to_parent(cell, 4) AS cell_r4,\n       SUM(records)::BIGINT AS total_records, COUNT(DISTINCT species)::INT AS total_species\nFROM read_parquet(${S3})\nGROUP BY cell_r4\nORDER BY total_records DESC\nLIMIT 100`,
      statLabel: 'top cells by records',
      statFn: r => fmtN(r.length),
      mapConfig: { cellCol: 'cell_r4', valueCol: 'total_records', metricLabel: 'records' },
    },
    {
      id: 'gadus', title: 'Atlantic cod',
      file: 'gadus.arrow',
      description: 'All H3 cells where Atlantic cod (Gadus morhua) has been observed, with record counts.',
      sql: `SELECT cell, records\nFROM read_parquet(${S3})\nWHERE species = 'Gadus morhua'\nORDER BY records DESC`,
      statLabel: 'total occurrence records',
      statFn: r => fmtN(r.reduce((s: number, row: any) => s + Number(row.records), 0)),
      mapConfig: { cellCol: 'cell', valueCol: 'records', metricLabel: 'records' },
    },
  ];

  type ExploreState = 'idle' | 'running' | 'done' | 'error';
  let exploreStates  = $state<Record<string, ExploreState>>(Object.fromEntries(EXPLORE_QUERIES.map(q => [q.id, 'idle'])));
  let exploreResults = $state<Record<string, any[]>>(Object.fromEntries(EXPLORE_QUERIES.map(q => [q.id, []])));
  let exploreErrors  = $state<Record<string, string>>(Object.fromEntries(EXPLORE_QUERIES.map(q => [q.id, ''])));
  let activeExploreId = $state<string | null>(null);

  // Select a query: activate immediately and load if not yet loaded
  function selectQuery(q: ExploreQuery) {
    activeExploreId = q.id;
    if (exploreStates[q.id] === 'idle' || exploreStates[q.id] === 'error') {
      runExplore(q);
    }
  }

  async function runExplore(q: ExploreQuery) {
    activeExploreId      = q.id;
    exploreStates[q.id]  = 'running';
    exploreErrors[q.id]  = '';
    exploreResults[q.id] = [];
    try {
      exploreResults[q.id] = await loadPrecomputedQuery(q.file);
      exploreStates[q.id]  = 'done';
    } catch (e) {
      exploreErrors[q.id]  = String(e);
      exploreStates[q.id]  = 'error';
    }
  }

  function downloadExploreCSV() {
    if (!activeExploreId) return;
    const r = exploreResults[activeExploreId];
    if (!r.length) return;
    const cols   = Object.keys(r[0]);
    const header = cols.join(',');
    const body   = r.map(row =>
      cols.map(c => {
        const v = String(row[c] ?? '');
        return v.includes(',') || v.includes('"') ? `"${v.replace(/"/g, '""')}"` : v;
      }).join(',')
    ).join('\n');
    const blob = new Blob([header + '\n' + body], { type: 'text/csv' });
    const a = Object.assign(document.createElement('a'), {
      href: URL.createObjectURL(blob),
      download: `obis_${activeExploreId}_${Date.now()}.csv`,
    });
    a.click();
    URL.revokeObjectURL(a.href);
  }

  function fmtN(n: number): string {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'k';
    return n.toLocaleString();
  }

  const activeExploreQuery = $derived(() => EXPLORE_QUERIES.find(q => q.id === activeExploreId) ?? null);
  const activeExploreRows  = $derived(() => activeExploreId ? exploreResults[activeExploreId] : []);
  const activeExploreCols  = $derived(() => activeExploreRows().length ? Object.keys(activeExploreRows()[0]) : []);
  const activeExploreStat  = $derived(() => {
    const q = activeExploreQuery();
    if (!q || exploreStates[q.id] !== 'done') return '';
    return q.statFn(exploreResults[q.id]);
  });

  // Map rows for H3-cell queries
  const exploreMapRows = $derived(() => {
    const q = activeExploreQuery();
    if (!q?.mapConfig || exploreStates[q.id] !== 'done') return [] as CellRow[];
    return exploreResults[q.id].map((row: any) => ({
      cell:  String(row[q.mapConfig!.cellCol]),
      value: Number(row[q.mapConfig!.valueCol]),
    })) as CellRow[];
  });
  const hasMapQuery = $derived(() => !!activeExploreQuery()?.mapConfig);

  // ── Copy SQL ──────────────────────────────────────────────────────────────
  let copied = $state(false);
  function copySQL() {
    const sql = activeTab === 'explore'
      ? (activeExploreQuery()?.sql ?? '')
      : OVERVIEW_SQL;
    navigator.clipboard.writeText(sql);
    copied = true;
    setTimeout(() => copied = false, 1500);
  }

  const OVERVIEW_SQL =
`SELECT COUNT(DISTINCT species) AS total_species,
       h3_cell_to_parent(cell, 4) AS cell_h4
FROM read_parquet('s3://obis-products/speciesgrids/h3_7/data.parquet')
GROUP BY cell_h4`;

  // ── Boot ──────────────────────────────────────────────────────────────────
  onMount(async () => {
    if (!browser) return;
    MapView = (await import('$lib/MapView.svelte')).default;
    loading = true;
    try {
      rows = await loadOverview();
    } catch (e) {
      error = String(e);
    } finally {
      loading = false;
    }
  });

  function fmt(n: number) {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'k';
    return n.toLocaleString();
  }
</script>

<svelte:head>
  <title>OBIS Species Grid Explorer</title>
  <link rel="stylesheet" href="https://unpkg.com/maplibre-gl@5.0.0/dist/maplibre-gl.css"/>
  <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:ital,wght@0,400;0,500;1,400&family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet"/>
</svelte:head>

<div class="layout">

  <!-- ════════════════ SIDEBAR ════════════════ -->
  <aside class="sidebar">

    <div class="brand">
      <span class="logo"><span style="font-weight: bold;">speciesgrids</span> explorer</span>
      <a href={resolve(`/docs/`)} class="docs-link">Docs ↗</a>
    </div>

    <div class="db-status" data-s={dataState.status}>
      <span class="dot"></span>
      {#if dataState.status === 'loading'} Loading overview…
      {:else if dataState.status === 'ready'} {fmt(rows.length)} cells loaded
      {:else if dataState.status === 'error'} {dataState.error}
      {:else} Idle
      {/if}
    </div>

    <div class="tabs">
      <button class:active={activeTab === 'overview'} onclick={() => activeTab = 'overview'}>
        Overview
      </button>
      <button class:active={activeTab === 'explore'} onclick={() => activeTab = 'explore'}>
        Explore
      </button>
    </div>

    <!-- ════ OVERVIEW TAB ════ -->
    {#if activeTab === 'overview'}
      <div class="tab-body">
        <p class="tab-desc">
          Global overview showing the number of distinct marine species
          observed per H3 resolution-4 cell (~300 km), loaded from a
          pre-aggregated Arrow file.
        </p>
        <div class="stat-pill">
          {rows.length ? fmt(rows.length) + ' cells loaded' : loading ? 'Loading…' : error ? 'Error' : '—'}
        </div>
        <p class="hint">
          Switch to the <strong>Explore</strong> tab to browse
          pre-computed queries against the full dataset.
        </p>
      </div>
    {/if}

    <!-- ════ EXPLORE TAB — flat query buttons ════ -->
    {#if activeTab === 'explore'}
      <div class="query-list">
        {#each EXPLORE_QUERIES as q (q.id)}
          {@const state = exploreStates[q.id]}
          <button
            class="query-btn"
            class:active={activeExploreId === q.id}
            disabled={state === 'running'}
            onclick={() => selectQuery(q)}
          >
            <span class="q-indicator" data-s={state}>
              {#if state === 'running'}⟳{:else if state === 'done'}✓{:else if state === 'error'}✗{:else}○{/if}
            </span>
            <span class="q-title">{q.title}</span>
          </button>
        {/each}
      </div>

      {#if activeExploreId && exploreStates[activeExploreId] === 'done'}
        <div class="explore-stat-row">
          <span class="e-stat-num">{activeExploreStat()}</span>
          <span class="e-stat-lbl">{activeExploreQuery()?.statLabel}</span>
        </div>
      {/if}
    {/if}

    <div class="logo-footer">
      <img src={ioclogo} alt="" style="height: 30px;">
      <img src={obislogo} alt="" style="height: 27px;">
    </div>

    <div class="sidebar-footer">
      <a href={resolve(`/docs/`)}>Documentation</a> ·
      <a href="https://obis.org" target="_blank" rel="noopener">obis.org</a> ·
      <a href="https://github.com/iobis/speciesgrids" target="_blank" rel="noopener">GitHub</a>
    </div>

  </aside>

  <!-- ════════════════ MAIN ════════════════ -->
  <div class="main">

    {#if activeTab === 'explore'}
      <!-- Explore: table + optional map panel side by side -->
      <div class="explore-content" class:has-map={hasMapQuery()}>
        <div class="explore-table">
          <ExploreTab
            title={activeExploreQuery()?.title ?? ''}
            statNumber={activeExploreStat()}
            statLabel={activeExploreQuery()?.statLabel ?? ''}
            cols={activeExploreCols()}
            rows={activeExploreRows()}
            sql={activeExploreQuery()?.sql ?? ''}
            colorCol={activeExploreQuery()?.colorCol}
            loading={!!activeExploreId && exploreStates[activeExploreId] === 'running'}
            error={activeExploreId ? exploreErrors[activeExploreId] : ''}
            onDownload={downloadExploreCSV}
          />
        </div>

        {#if hasMapQuery() && MapView}
          {@const M = MapView}
          <div class="explore-map-wrap">
            <M
              rows={exploreMapRows()}
              metricLabel={activeExploreQuery()?.mapConfig?.metricLabel ?? 'value'}
              dark={true}
            />
          </div>
        {/if}
      </div>

    {:else}
      <!-- Overview: map -->
      <div class="map-wrap">
        {#if MapView}
          {@const M = MapView}
          <M {rows} {loading} metricLabel="species" />
        {:else}
          <div class="init-screen">
            <div class="init-spin"></div>
            <span>Initialising…</span>
          </div>
        {/if}

        {#if rows.length > 0}
          <div class="map-badge">
            H3 res-4 · number of species · {fmt(rows.length)} cells
          </div>
        {/if}

        {#if error}
          <div class="error-toast">{error}</div>
        {/if}
      </div>

      <!-- SQL panel (overview only) -->
      <div class="sql-panel">
        <div class="sql-header">
          <span class="sql-label">SQL</span>
          <span class="sql-sub">
            {loading ? 'loading…' : rows.length ? `${fmt(rows.length)} cells · pre-computed` : 'overview loaded from Arrow file'}
          </span>
          <button class="copy-btn" onclick={copySQL}>
            {copied ? '✓ Copied' : 'Copy'}
          </button>
        </div>
        <textarea class="sql-box" readonly value={OVERVIEW_SQL} rows={6} spellcheck="false"></textarea>
      </div>
    {/if}

  </div>
</div>

<style>
  :global(*,*::before,*::after){box-sizing:border-box;margin:0;padding:0}
  :global(body){
    background:#f0f4f8; color:#1e293b;
    font-family:'IBM Plex Sans',sans-serif; overflow:hidden;
  }

  .layout { display:flex; height:100vh; width:100vw; }

  /* ── Sidebar ──────────────────────────────────────────────────────── */
  .sidebar {
    width:240px; min-width:240px; height:100vh;
    background:#ffffff;
    border-right:1px solid #e2e8f0;
    display:flex; flex-direction:column;
    overflow-y:auto; overflow-x:hidden;
    scrollbar-width:thin; scrollbar-color:rgba(0,0,0,.08) transparent;
    font-family:'IBM Plex Mono',monospace; font-size:.78rem;
  }

  .brand {
    display:flex; align-items:center; justify-content:space-between;
    flex-wrap:nowrap; gap:.4rem;
    padding:.75rem .9rem .65rem;
    border-bottom:1px solid #e2e8f0;
    overflow:hidden;
  }
  .logo {
    font-size:.8rem; color:#0854a8; letter-spacing:.01em;
    white-space:nowrap; min-width:0; overflow:hidden; text-overflow:ellipsis;
  }
  .docs-link { font-size:.62rem; color:#94a3b8; text-decoration:none; flex-shrink:0; white-space:nowrap; }
  .docs-link:hover { color:#0854a8; }

  .db-status {
    display:flex; align-items:center; gap:.4rem;
    padding:.35rem 1rem; font-size:.62rem; color:#94a3b8;
    border-bottom:1px solid #f1f5f9;
  }
  .db-status[data-s="ready"]   { color:#16a34a; }
  .db-status[data-s="loading"] { color:#0854a8; }
  .db-status[data-s="error"]   { color:#dc2626; }
  .dot { width:5px;height:5px;border-radius:50%;background:currentColor;flex-shrink:0; }
  .db-status[data-s="ready"] .dot { animation:blink 2.5s infinite; }
  @keyframes blink { 0%,100%{opacity:1} 50%{opacity:.2} }

  /* Tabs */
  .tabs { display:flex; border-bottom:1px solid #e2e8f0; }
  .tabs button {
    flex:1; padding:.52rem .2rem;
    background:none; border:none; border-bottom:2px solid transparent;
    color:#94a3b8; font-family:inherit; font-size:.62rem; font-weight:600;
    cursor:pointer; text-transform:uppercase; letter-spacing:.07em;
    transition:all .15s;
  }
  .tabs button.active { color:#0854a8; border-bottom-color:#0854a8; }
  .tabs button:hover:not(.active) { color:#475569; }

  /* Overview tab body */
  .tab-body {
    padding:.85rem 1rem;
    display:flex; flex-direction:column; gap:.75rem;
    flex:1;
  }
  .tab-desc { font-size:.72rem; color:#64748b; line-height:1.55; font-family:'IBM Plex Sans',sans-serif; }
  .hint { font-size:.67rem; color:#94a3b8; line-height:1.5; font-family:'IBM Plex Sans',sans-serif; }
  .hint strong { color:#64748b; }
  .stat-pill {
    font-size:.7rem; color:#16a34a;
    background:rgba(22,163,74,.06);
    border:1px solid rgba(22,163,74,.2);
    border-radius:20px; padding:.22rem .7rem; width:fit-content;
  }

  /* ── Explore query buttons ──────────────────────────────────────────── */
  .query-list {
    display: flex;
    flex-direction: column;
    padding: .5rem .6rem;
    gap: .25rem;
    flex: 1;
  }

  .query-btn {
    display: flex;
    align-items: center;
    gap: .55rem;
    width: 100%;
    padding: .6rem .75rem;
    background: none;
    border: 1px solid transparent;
    border-radius: 7px;
    cursor: pointer;
    text-align: left;
    font-family: 'IBM Plex Mono', monospace;
    font-size: .72rem;
    color: #475569;
    transition: all .15s;
  }
  .query-btn:hover:not(:disabled) { background: #f1f5f9; color: #1e293b; }
  .query-btn.active {
    background: rgba(8,84,168,.06);
    border-color: rgba(8,84,168,.25);
    color: #0854a8;
  }
  .query-btn:disabled { opacity: .5; cursor: not-allowed; }

  .q-indicator {
    font-size: .6rem;
    width: 1em;
    flex-shrink: 0;
    color: #94a3b8;
  }
  .q-indicator[data-s="done"]    { color: #16a34a; }
  .q-indicator[data-s="running"] { color: #0854a8; animation: spin .9s linear infinite; display:inline-block; }
  .q-indicator[data-s="error"]   { color: #dc2626; }

  .q-title { font-weight: 500; }

  .explore-stat-row {
    display: flex;
    flex-direction: column;
    padding: .6rem 1rem .5rem;
    border-top: 1px solid #f1f5f9;
    gap: .15rem;
  }
  .e-stat-num {
    font-size: 1.3rem;
    font-weight: 700;
    color: #0854a8;
    font-family: 'IBM Plex Mono', monospace;
    line-height: 1;
  }
  .e-stat-lbl {
    font-size: .6rem;
    color: #94a3b8;
    font-family: 'IBM Plex Sans', sans-serif;
  }

  /* Footer */
  .sidebar-footer {
    padding:.85rem 1rem;
    font-size:.58rem; color:#cbd5e1; line-height:2;
    border-top:1px solid #f1f5f9;
  }
  .sidebar-footer a { color:#94a3b8; text-decoration:none; }
  .sidebar-footer a:hover { color:#0854a8; }

  .logo-footer {
    display: flex;
    gap: .8rem;
    padding: .85rem 1rem;
  }

  /* ── Main ──────────────────────────────────────────────────────────── */
  .main { flex:1; display:flex; flex-direction:column; overflow:hidden; }

  /* Overview map */
  .map-wrap { flex:1; position:relative; overflow:hidden; }

  .init-screen {
    position:absolute; inset:0;
    display:flex; align-items:center; justify-content:center;
    gap:.75rem; color:#94a3b8; font-size:.8rem;
  }
  .init-spin {
    width:24px; height:24px;
    border:2px solid #e2e8f0; border-top-color:#0854a8;
    border-radius:50%; animation:spin .9s linear infinite;
  }
  @keyframes spin { to{transform:rotate(360deg)} }

  .map-badge {
    position:absolute; top:.75rem; left:50%;
    transform:translateX(-50%);
    background:rgba(255,255,255,.92);
    border:1px solid #e2e8f0;
    border-radius:20px; padding:.3rem .9rem;
    font-family:'IBM Plex Mono',monospace; font-size:.65rem; color:#64748b;
    pointer-events:none;
    box-shadow: 0 1px 4px rgba(0,0,0,.08);
  }

  .error-toast {
    position:absolute; bottom:1.5rem; left:50%; transform:translateX(-50%);
    background:rgba(220,38,38,.07); border:1px solid rgba(220,38,38,.25);
    border-radius:7px; padding:.55rem 1.25rem;
    font-size:.72rem; color:#dc2626; max-width:480px; text-align:center;
    font-family:'IBM Plex Mono',monospace;
  }

  /* Explore layout: table [+ map panel] */
  .explore-content {
    flex: 1;
    display: flex;
    overflow: hidden;
  }

  .explore-table {
    flex: 1;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    min-width: 0;
  }

  .explore-map-wrap {
    width: 420px;
    min-width: 420px;
    flex-shrink: 0;
    position: relative;
    border-left: 1px solid #e2e8f0;
  }

  /* ── SQL panel ─────────────────────────────────────────────────────── */
  .sql-panel {
    height:160px; min-height:160px;
    border-top:1px solid #e2e8f0;
    background:#f8fafc;
    display:flex; flex-direction:column;
  }
  .sql-header {
    display:flex; align-items:center; gap:.6rem;
    padding:.4rem .85rem;
    border-bottom:1px solid #e2e8f0;
  }
  .sql-label {
    font-family:'IBM Plex Mono',monospace; font-size:.58rem; font-weight:600;
    text-transform:uppercase; letter-spacing:.12em; color:#94a3b8;
  }
  .sql-sub { font-family:'IBM Plex Mono',monospace; font-size:.58rem; color:#cbd5e1; flex:1; }
  .copy-btn {
    padding:.15rem .55rem;
    background:#fff; border:1px solid #e2e8f0;
    border-radius:4px; color:#64748b;
    font-size:.6rem; font-family:'IBM Plex Mono',monospace; cursor:pointer;
  }
  .copy-btn:hover { color:#0854a8; border-color:#0854a8; }
  .sql-box {
    flex:1; resize:none; background:transparent; border:none; outline:none;
    color:#0854a8; font-family:'IBM Plex Mono',monospace;
    font-size:.72rem; line-height:1.7; padding:.55rem .85rem; cursor:default;
  }
</style>
