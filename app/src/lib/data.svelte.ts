import { tableFromIPC } from 'apache-arrow';
import { asset } from '$app/paths';

export const dataState = $state({
  status: 'idle' as 'idle' | 'loading' | 'ready' | 'error',
  error:  null as string | null,
});

async function fetchTable(path: string) {
  const resp = await fetch(asset(path));
  if (!resp.ok) throw new Error(`HTTP ${resp.status} — ${path}`);
  return tableFromIPC(await resp.arrayBuffer());
}

export async function loadOverview(): Promise<{ cell: string; value: number }[]> {
  dataState.status = 'loading';
  try {
    const table  = await fetchTable('/overview_h3_4.arrow');
    const cells  = table.getChild('cell')!;
    const values = table.getChild('total_species')!;
    const rows: { cell: string; value: number }[] = [];
    for (let i = 0; i < table.numRows; i++) {
      rows.push({ cell: String(cells.get(i)), value: Number(values.get(i)) });
    }
    dataState.status = 'ready';
    return rows;
  } catch (err) {
    dataState.error  = String(err);
    dataState.status = 'error';
    throw err;
  }
}

export async function loadPrecomputedQuery(file: string): Promise<Record<string, unknown>[]> {
  const path = '/' + file.replace(/\.parquet$/, '.arrow');
  const table  = await fetchTable(path);
  const fields = table.schema.fields.map(f => f.name);
  const cols   = fields.map(name => table.getChild(name)!);
  const rows: Record<string, unknown>[] = [];
  for (let i = 0; i < table.numRows; i++) {
    const row: Record<string, unknown> = {};
    for (let c = 0; c < fields.length; c++) {
      const v = cols[c].get(i);
      row[fields[c]] = typeof v === 'bigint' ? Number(v) : v;
    }
    rows.push(row);
  }
  return rows;
}
