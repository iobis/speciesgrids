<script lang="ts">
  /**
   * ExploreTab.svelte — Full-width result panel for the Explore tab.
   *
   * Receives the active query result from the parent (+page.svelte).
   * Shows: headline stat · paginated scrollable table · CSV download.
   */

  export interface ExploreRow { [col: string]: unknown; }

  const PAGE_SIZE = 500;

  let {
    title       = '',
    statNumber  = '',
    statLabel   = '',
    cols        = [] as string[],
    rows        = [] as ExploreRow[],
    sql         = '',
    loading     = false,
    error       = '',
    colorCol    = undefined as string | undefined,
    onDownload  = () => {},
  }: {
    title:       string;
    statNumber:  string;
    statLabel:   string;
    cols:        string[];
    rows:        ExploreRow[];
    sql:         string;
    loading:     boolean;
    error:       string;
    colorCol?:   string;
    onDownload:  () => void;
  } = $props();

  let sqlCopied = $state(false);
  function copySql() {
    navigator.clipboard.writeText(sql);
    sqlCopied = true;
    setTimeout(() => sqlCopied = false, 1500);
  }

  // ── Pagination ────────────────────────────────────────────────────────────
  let currentPage = $state(0);

  $effect(() => {
    // Reset to first page whenever the rows dataset changes.
    // Reading rows.length is enough to establish the dependency.
    void rows.length;
    currentPage = 0;
  });

  const totalPages = $derived(Math.max(1, Math.ceil(rows.length / PAGE_SIZE)));

  const pageRows = $derived.by(() => {
    const start = currentPage * PAGE_SIZE;
    return rows.slice(start, start + PAGE_SIZE);
  });

  const pageStart = $derived(currentPage * PAGE_SIZE + 1);
  const pageEnd   = $derived(Math.min((currentPage + 1) * PAGE_SIZE, rows.length));

  function fmtCount(n: number): string {
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(1).replace(/\.0$/, '') + 'M';
    if (n >= 1_000)     return n.toLocaleString();
    return String(n);
  }

  // ── Column display labels ─────────────────────────────────────────────────
  const COL_LABELS: Record<string, string> = {
    species: 'Species', AphiaID: 'OBIS', total_records: 'Records',
    total_cells: 'Cells', last_seen: 'Last seen', record_count: 'Records',
    min_year: 'First obs.', max_year: 'Last obs.', cell: 'H3 cell',
    cell_r4: 'H3 cell (res-4)', total_species: 'Species',
  };
  function colLabel(c: string) { return COL_LABELS[c] ?? c; }

  // ── Never abbreviate these columns ───────────────────────────────────────
  const NO_ABBREV = new Set(['AphiaID', 'last_seen', 'min_year', 'max_year']);

  function fmtVal(col: string, v: unknown): string {
    if (v === null || v === undefined) return '—';
    if (NO_ABBREV.has(col)) return String(v);
    const n = Number(v);
    if (!isNaN(n) && Math.abs(n) >= 10_000) {
      if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(1) + 'M';
      return (n / 1e3).toFixed(1) + 'k';
    }
    return String(v);
  }

  // ── Heat colour scale ─────────────────────────────────────────────────────
  // single-hue purple: fewest records → saturated violet, most → soft lavender
  const STOPS: [number, number, number][] = [
    [109,  40, 217],   // purple-700 — fewest records
    [167, 139, 250],   // violet-400 — mid
    [237, 233, 254],   // violet-100 — most records
  ];

  // Compute log1p min/max for the colorCol across ALL rows (not just this page).
  const colorRange = $derived.by(() => {
    if (!colorCol) return { min: 0, max: 1 };
    let min = Infinity;
    let max = -Infinity;
    for (const row of rows) {
      const n = Number(row[colorCol!]);
      if (!isNaN(n)) {
        const v = Math.log1p(n);
        if (v < min) min = v;
        if (v > max) max = v;
      }
    }
    if (!isFinite(min) || min === max) return { min: 0, max: 1 };
    return { min, max };
  });

  /**
   * Interpolate along the 3-stop scale.
   * t ∈ [0, 1]: 0 = red, 0.5 = amber, 1 = blue.
   */
  function interpolateStops(t: number): [number, number, number] {
    // First half: stop[0] → stop[1]; second half: stop[1] → stop[2]
    if (t <= 0.5) {
      const f = t * 2;
      return [
        Math.round(STOPS[0][0] + f * (STOPS[1][0] - STOPS[0][0])),
        Math.round(STOPS[0][1] + f * (STOPS[1][1] - STOPS[0][1])),
        Math.round(STOPS[0][2] + f * (STOPS[1][2] - STOPS[0][2])),
      ];
    } else {
      const f = (t - 0.5) * 2;
      return [
        Math.round(STOPS[1][0] + f * (STOPS[2][0] - STOPS[1][0])),
        Math.round(STOPS[1][1] + f * (STOPS[2][1] - STOPS[1][1])),
        Math.round(STOPS[1][2] + f * (STOPS[2][2] - STOPS[1][2])),
      ];
    }
  }

  /** Returns inline style string for a heat cell, or '' if not applicable. */
  function heatStyle(col: string, v: unknown): string {
    if (!colorCol || col !== colorCol) return '';
    const n = Number(v);
    if (isNaN(n)) return '';
    const { min, max } = colorRange;
    const logV = Math.log1p(n);
    const t = max === min ? 0.5 : (logV - min) / (max - min);
    const [r, g, b] = interpolateStops(Math.max(0, Math.min(1, t)));
    // Relative luminance (sRGB) for WCAG contrast decision.
    const toLinear = (c: number) => {
      const s = c / 255;
      return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
    };
    const lum = 0.2126 * toLinear(r) + 0.7152 * toLinear(g) + 0.0722 * toLinear(b);
    const textColor = lum > 0.5 ? '#1e293b' : '#ffffff';
    return `background:rgb(${r},${g},${b});color:${textColor};`;
  }

  /** CSS border-bottom colour for the colorCol header (saturated purple end). */
  const headerUnderlineStyle = $derived(
    colorCol ? `border-bottom-color:rgb(${STOPS[0][0]},${STOPS[0][1]},${STOPS[0][2]});` : ''
  );
</script>

<div class="panel">

  {#if !title}
    <!-- Nothing selected yet -->
    <div class="empty">
      <div class="empty-icon">◈</div>
      <p>Select a query from the left to explore the data.</p>
    </div>

  {:else if loading}
    <div class="empty">
      <div class="spinner"></div>
      <p>Loading pre-computed results…</p>
    </div>

  {:else if error}
    <div class="empty error-state">
      <p class="err">{error}</p>
    </div>

  {:else}
    <!-- Header bar: stat + download -->
    <div class="result-header">
      <div class="stat">
        <span class="stat-num">{statNumber}</span>
        <span class="stat-lbl">{statLabel}</span>
      </div>
      {#if rows.length > 0}
        <button class="csv-btn" onclick={onDownload}>
          ↓ Download CSV
        </button>
      {/if}
    </div>

    <!-- Full-height table -->
    {#if rows.length > 0}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              {#each cols as c (c)}
                <th
                  style={c === colorCol ? headerUnderlineStyle : ''}
                  class:heat-header={c === colorCol}
                >{colLabel(c)}</th>
              {/each}
            </tr>
          </thead>
          <tbody>
            {#each pageRows as row, i (currentPage * PAGE_SIZE + i)}
              <tr>
                {#each cols as c (c)}
                  {#if c === 'AphiaID' && row[c] != null}
                    <td>
                      <a
                        href="https://obis.org/taxon/{row[c]}"
                        target="_blank"
                        rel="noopener"
                        class="obis-link"
                        title="View on OBIS"
                      >{row[c]}</a>
                    </td>
                  {:else if c === 'species' && row['AphiaID'] != null}
                    <td>
                      <a
                        href="https://obis.org/taxon/{row['AphiaID']}"
                        target="_blank"
                        rel="noopener"
                        class="species-link"
                        title="View on OBIS"
                      ><em>{String(row[c])}</em></a>
                    </td>
                  {:else}
                    <td style={heatStyle(c, row[c])} class:heat-cell={c === colorCol}>{fmtVal(c, row[c])}</td>
                  {/if}
                {/each}
              </tr>
            {/each}
          </tbody>
        </table>
      </div>

      <!-- Pagination bar -->
      {#if rows.length > PAGE_SIZE}
        <div class="pagination">
          <span class="page-info">
            Rows {fmtCount(pageStart)}–{fmtCount(pageEnd)} of {fmtCount(rows.length)}
          </span>
          <div class="page-nav">
            <button
              class="page-btn"
              disabled={currentPage === 0}
              onclick={() => currentPage--}
            >‹ Prev</button>
            <button
              class="page-btn"
              disabled={currentPage >= totalPages - 1}
              onclick={() => currentPage++}
            >Next ›</button>
          </div>
        </div>
      {/if}
    {:else if !loading}
      <div class="empty"><p>No results returned.</p></div>
    {/if}
  {/if}

  {#if sql && title && !loading}
    <div class="sql-ref">
      <div class="sql-ref-header">
        <span class="sql-ref-label">SQL reference</span>
        <span class="sql-ref-sub">pre-computed · not run in browser</span>
        <button class="sql-copy-btn" onclick={copySql}>{sqlCopied ? '✓ Copied' : 'Copy'}</button>
      </div>
      <textarea class="sql-ref-box" readonly value={sql} rows={4} spellcheck="false"></textarea>
    </div>
  {/if}

</div>

<style>
  .panel {
    display: flex;
    flex-direction: column;
    height: 100%;
    background: #fff;
    overflow: hidden;
  }

  /* ── Empty / loading state ───────────────────────────────────────────── */
  .empty {
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: .75rem;
    color: #94a3b8;
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: .85rem;
    text-align: center;
    padding: 2rem;
  }
  .empty p { max-width: 320px; line-height: 1.6; }
  .empty-icon { font-size: 2rem; color: #cbd5e1; }

  .spinner {
    width: 28px; height: 28px;
    border: 2px solid #e2e8f0; border-top-color: #0854a8;
    border-radius: 50%; animation: spin .75s linear infinite;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  .err { color: #dc2626; font-family: 'IBM Plex Mono', monospace; font-size: .78rem; }

  /* ── Result header ───────────────────────────────────────────────────── */
  .result-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: .75rem 1.25rem;
    border-bottom: 1px solid #e2e8f0;
    flex-shrink: 0;
    background: #f8fafc;
  }

  .stat { display: flex; align-items: baseline; gap: .55rem; }

  .stat-num {
    font-size: 1.6rem;
    font-weight: 700;
    color: #0854a8;
    font-family: 'IBM Plex Mono', monospace;
    line-height: 1;
  }

  .stat-lbl {
    font-size: .72rem;
    color: #94a3b8;
    font-family: 'IBM Plex Sans', sans-serif;
  }

  .csv-btn {
    padding: .35rem .9rem;
    background: #fff;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    color: #0854a8;
    font-size: .72rem;
    font-weight: 600;
    font-family: 'IBM Plex Mono', monospace;
    cursor: pointer;
    transition: all .15s;
  }
  .csv-btn:hover { background: #0854a8; color: #fff; border-color: #0854a8; }

  /* ── Table ───────────────────────────────────────────────────────────── */
  .table-wrap {
    flex: 1;
    overflow: auto;
    scrollbar-width: thin;
    scrollbar-color: rgba(0,0,0,.1) transparent;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: .75rem;
    font-family: 'IBM Plex Mono', monospace;
  }

  th {
    position: sticky;
    top: 0;
    background: #f8fafc;
    color: #64748b;
    font-size: .6rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .07em;
    padding: .5rem .9rem;
    text-align: left;
    border-bottom: 2px solid #e2e8f0;
    white-space: nowrap;
    z-index: 1;
  }

  /* colorCol header gets a 3px bottom border in the high-end stop colour */
  .heat-header {
    border-bottom-width: 3px;
    border-bottom-style: solid;
    /* border-bottom-color supplied via inline style */
  }

  td {
    padding: .38rem .9rem;
    color: #334155;
    border-bottom: 1px solid #f1f5f9;
    white-space: nowrap;
  }

  tr:hover td { background: #f8fafc; }

  /* Heat cells: override hover background so the colour stays visible */
  tr:hover td.heat-cell { background: transparent; }

  /* Species name link */
  .species-link {
    color: #0f172a;
    text-decoration: none;
  }
  .species-link:hover { color: #0854a8; text-decoration: underline; }

  /* ── Pagination bar ──────────────────────────────────────────────────── */
  .pagination {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: .45rem 1rem;
    border-top: 1px solid #e2e8f0;
    flex-shrink: 0;
    background: #f8fafc;
    gap: .75rem;
  }

  .page-info {
    font-family: 'IBM Plex Mono', monospace;
    font-size: .65rem;
    color: #64748b;
  }

  .page-nav {
    display: flex;
    gap: .4rem;
  }

  .page-btn {
    padding: .2rem .65rem;
    background: #fff;
    border: 1px solid #e2e8f0;
    border-radius: 4px;
    color: #475569;
    font-size: .65rem;
    font-family: 'IBM Plex Mono', monospace;
    cursor: pointer;
    transition: all .15s;
  }
  .page-btn:hover:not(:disabled) { background: #0854a8; color: #fff; border-color: #0854a8; }
  .page-btn:disabled { opacity: .35; cursor: default; }

  /* ── SQL reference footer ───────────────────────────────────────────── */
  .sql-ref {
    flex-shrink: 0;
    border-top: 1px solid #e2e8f0;
    background: #f8fafc;
    display: flex;
    flex-direction: column;
  }
  .sql-ref-header {
    display: flex;
    align-items: center;
    gap: .6rem;
    padding: .35rem .85rem;
    border-bottom: 1px solid #e2e8f0;
  }
  .sql-ref-label {
    font-family: 'IBM Plex Mono', monospace;
    font-size: .58rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: .12em;
    color: #94a3b8;
  }
  .sql-ref-sub {
    font-family: 'IBM Plex Mono', monospace;
    font-size: .58rem;
    color: #cbd5e1;
    flex: 1;
  }
  .sql-copy-btn {
    padding: .15rem .55rem;
    background: #fff;
    border: 1px solid #e2e8f0;
    border-radius: 4px;
    color: #64748b;
    font-size: .6rem;
    font-family: 'IBM Plex Mono', monospace;
    cursor: pointer;
  }
  .sql-copy-btn:hover { color: #0854a8; border-color: #0854a8; }
  .sql-ref-box {
    resize: none;
    background: transparent;
    border: none;
    outline: none;
    color: #0854a8;
    font-family: 'IBM Plex Mono', monospace;
    font-size: .72rem;
    line-height: 1.7;
    padding: .55rem .85rem;
    cursor: default;
  }

  /* AphiaID link — small pill style */
  .obis-link {
    display: inline-block;
    padding: .1rem .4rem;
    background: rgba(8,84,168,.06);
    border: 1px solid rgba(8,84,168,.2);
    border-radius: 4px;
    color: #0854a8;
    font-size: .65rem;
    text-decoration: none;
    transition: all .15s;
  }
  .obis-link:hover {
    background: #0854a8;
    color: #fff;
  }
</style>
