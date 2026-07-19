"""H3 kernel density surfaces derived from the speciesgrids occurrence product.

This module is intentionally separate from the OBIS/GBIF aggregation pipeline.
It reads the finished GeoParquet product and writes a compact density parquet:

    build/density_3/data.parquet
        AphiaID  int32
        h3       uint64   (H3 index at ``resolution``)
        density  uint16   (max-normalized × 65535; rows below store_cutoff dropped)

Performance: there are only ~40k distinct presence cells at H3 res 3, each reused
by many species. Kernels are therefore precomputed once per unique cell and
species maps are assembled by summing those cached kernels.

Thermal suitability is not stored here; apps can evaluate a per-species thermal
profile against a shared temperature map at request time.
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Iterable

import h3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from speciesgrids.db import connect
from speciesgrids.progress import configure_logging, progress_bar, run_busy, stage


logger = logging.getLogger(__name__)

DENSITY_SCALE = 65535
EARTH_RADIUS_KM = 6371.0
DEFAULT_RESOLUTION = 3
DEFAULT_MAX_RINGS = 50
DEFAULT_SD_KM = 1000.0
DEFAULT_KERNEL_CUTOFF = 1e-8
DEFAULT_STORE_CUTOFF = 1e-3
DEFAULT_ROW_GROUP_SIZE = 128_000
DEFAULT_WORKERS = max(1, (os.cpu_count() or 2) - 1)
DEFAULT_SHARD_BATCH_SIZE = 2_000

# Filled in worker processes via initializer (read-only kernel cache).
_WORKER_KERNELS: dict[int, tuple[np.ndarray, np.ndarray]] | None = None
_WORKER_UNIVERSE: np.ndarray | None = None
_WORKER_LATLNG: tuple[int, dict[str, tuple[float, float]]] | None = None


def normalize_density(densities: dict) -> dict:
    """Normalize kernel mass so values sum to 1 (Speedy convention)."""
    total = sum(densities.values())
    if total <= 0:
        return densities
    return {k: v / total for k, v in densities.items()}


def _haversine_km_vec(lat0: float, lon0: float, lats: np.ndarray, lons: np.ndarray) -> np.ndarray:
    """Vectorized great-circle distance in km."""
    lat0_r = math.radians(lat0)
    lon0_r = math.radians(lon0)
    lats_r = np.radians(lats)
    lons_r = np.radians(lons)
    dlat = lats_r - lat0_r
    dlon = lons_r - lon0_r
    a = np.sin(dlat * 0.5) ** 2 + np.cos(lat0_r) * np.cos(lats_r) * np.sin(dlon * 0.5) ** 2
    return EARTH_RADIUS_KM * 2.0 * np.arcsin(np.minimum(1.0, np.sqrt(a)))


def _gaussian_pdf_km(distances: np.ndarray, sd: float) -> np.ndarray:
    inv = 1.0 / (sd * math.sqrt(2.0 * math.pi))
    half_inv_var = 0.5 / (sd * sd)
    return inv * np.exp(-half_inv_var * distances * distances)


def _max_distance_km(sd: float, density_cutoff: float) -> float:
    """Distance at which N(0, sd) PDF drops to ``density_cutoff``."""
    inv = 1.0 / (sd * math.sqrt(2.0 * math.pi))
    ratio = density_cutoff / inv
    if ratio <= 0 or ratio >= 1:
        return 0.0
    return sd * math.sqrt(-2.0 * math.log(ratio))


def _all_cells_at_resolution(resolution: int) -> list[str]:
    cells: set[str] = set()
    for res0 in h3.get_res0_cells():
        cells.update(h3.cell_to_children(res0, resolution))
    return sorted(cells)


def _precompute_latlng(resolution: int) -> dict[str, tuple[float, float]]:
    return {cell: h3.cell_to_latlng(cell) for cell in _all_cells_at_resolution(resolution)}


def calculate_density(
    cells: list[str],
    *,
    max_rings: int = DEFAULT_MAX_RINGS,
    sd: float = DEFAULT_SD_KM,
    density_cutoff: float = DEFAULT_KERNEL_CUTOFF,
    latlng: dict[str, tuple[float, float]] | None = None,
) -> pd.DataFrame:
    """Gaussian H3 density kernel (Speedy-compatible, vectorized).

    Returns columns ``h3`` (hex string) and ``density`` (sum-normalized floats).
    """
    raw = _accumulate_raw(
        cells,
        max_rings=max_rings,
        sd=sd,
        density_cutoff=density_cutoff,
        latlng=latlng,
    )
    normalized = normalize_density(raw)
    if not normalized:
        return pd.DataFrame(columns=["h3", "density"])
    return pd.DataFrame({"h3": list(normalized.keys()), "density": list(normalized.values())})


def _accumulate_raw(
    cells: list[str],
    *,
    max_rings: int,
    sd: float,
    density_cutoff: float,
    latlng: dict[str, tuple[float, float]] | None = None,
) -> dict[str, float]:
    if not cells:
        return {}
    d_max = _max_distance_km(sd, density_cutoff)
    densities: dict[str, float] = {}
    for cell in cells:
        lat0, lon0 = latlng[cell] if latlng is not None else h3.cell_to_latlng(cell)
        ring1 = list(h3.grid_ring(cell, 1))
        if ring1:
            lat1, lon1 = latlng[ring1[0]] if latlng is not None else h3.cell_to_latlng(ring1[0])
            step = float(_haversine_km_vec(lat0, lon0, np.array([lat1]), np.array([lon1]))[0])
            k_max = int(min(max_rings, math.ceil(d_max / max(step, 1.0)) + 1))
        else:
            k_max = max_rings
        disk = list(h3.grid_disk(cell, k_max))
        if not disk:
            continue
        if latlng is not None:
            lats = np.fromiter((latlng[c][0] for c in disk), dtype=np.float64, count=len(disk))
            lons = np.fromiter((latlng[c][1] for c in disk), dtype=np.float64, count=len(disk))
        else:
            coords = [h3.cell_to_latlng(c) for c in disk]
            lats = np.fromiter((c[0] for c in coords), dtype=np.float64, count=len(disk))
            lons = np.fromiter((c[1] for c in coords), dtype=np.float64, count=len(disk))
        values = _gaussian_pdf_km(_haversine_km_vec(lat0, lon0, lats, lons), sd)
        for h3_cell, value in zip(disk, values):
            if value >= density_cutoff:
                densities[h3_cell] = densities.get(h3_cell, 0.0) + float(value)
    return densities


def _compute_one_cell_kernel(
    cell: str,
    max_rings: int,
    sd: float,
    density_cutoff: float,
    resolution: int,
) -> tuple[int, np.ndarray, np.ndarray]:
    """Worker: raw kernel for one presence cell → (source_uint64, targets, weights)."""
    global _WORKER_LATLNG
    if _WORKER_LATLNG is None or _WORKER_LATLNG[0] != resolution:
        _WORKER_LATLNG = (resolution, _precompute_latlng(resolution))
    latlng = _WORKER_LATLNG[1]
    raw = _accumulate_raw(
        [cell],
        max_rings=max_rings,
        sd=sd,
        density_cutoff=density_cutoff,
        latlng=latlng,
    )
    if not raw:
        return h3.str_to_int(cell), np.array([], dtype=np.uint64), np.array([], dtype=np.float32)
    targets = np.fromiter((h3.str_to_int(k) for k in raw.keys()), dtype=np.uint64, count=len(raw))
    weights = np.fromiter((raw[k] for k in raw.keys()), dtype=np.float32, count=len(raw))
    return h3.str_to_int(cell), targets, weights


def encode_density_from_maps(
    aphiaid: int,
    targets: np.ndarray,
    values: np.ndarray,
    *,
    store_cutoff: float = DEFAULT_STORE_CUTOFF,
) -> pd.DataFrame:
    """Sum-normalize + max-normalize float densities and encode compact parquet rows."""
    if len(values) == 0:
        return pd.DataFrame(columns=["AphiaID", "h3", "density"])
    total = float(values.sum())
    if total <= 0:
        return pd.DataFrame(columns=["AphiaID", "h3", "density"])
    scaled = values / total
    peak = float(scaled.max())
    if peak <= 0:
        return pd.DataFrame(columns=["AphiaID", "h3", "density"])
    scaled = scaled / peak
    keep = scaled >= store_cutoff
    if not np.any(keep):
        return pd.DataFrame(columns=["AphiaID", "h3", "density"])
    dens_u16 = np.clip(np.rint(scaled[keep] * DENSITY_SCALE), 0, DENSITY_SCALE).astype(np.uint16)
    return pd.DataFrame(
        {
            "AphiaID": np.full(int(keep.sum()), int(aphiaid), dtype=np.int32),
            "h3": targets[keep].astype(np.uint64),
            "density": dens_u16,
        }
    )


def encode_density_rows(
    aphiaid: int,
    density: pd.DataFrame,
    *,
    store_cutoff: float = DEFAULT_STORE_CUTOFF,
) -> pd.DataFrame:
    """Encode a hex-string density frame that is already sum-normalized."""
    if density.empty:
        return pd.DataFrame(columns=["AphiaID", "h3", "density"])
    targets = np.fromiter(
        (h3.str_to_int(c) for c in density["h3"].to_numpy()),
        dtype=np.uint64,
        count=len(density),
    )
    values = density["density"].to_numpy(dtype=np.float64)
    peak = float(values.max()) if len(values) else 0.0
    if peak <= 0:
        return pd.DataFrame(columns=["AphiaID", "h3", "density"])
    scaled = values / peak
    keep = scaled >= store_cutoff
    dens_u16 = np.clip(np.rint(scaled[keep] * DENSITY_SCALE), 0, DENSITY_SCALE).astype(np.uint16)
    return pd.DataFrame(
        {
            "AphiaID": np.full(int(keep.sum()), int(aphiaid), dtype=np.int32),
            "h3": targets[keep],
            "density": dens_u16,
        }
    )


def decode_density(density_u16: np.ndarray | pd.Series) -> np.ndarray:
    """Convert stored uint16 density back to float in [0, 1]."""
    return np.asarray(density_u16, dtype=np.float64) / DENSITY_SCALE


def _write_density_table(path: str, df: pd.DataFrame) -> None:
    table = pa.Table.from_pandas(
        df,
        schema=pa.schema(
            [
                ("AphiaID", pa.int32()),
                ("h3", pa.uint64()),
                ("density", pa.uint16()),
            ]
        ),
        preserve_index=False,
    )
    pq.write_table(table, path, compression="zstd")


def _save_kernel_cache(
    path: str,
    sources: np.ndarray,
    offsets: np.ndarray,
    target_idx: np.ndarray,
    weights: np.ndarray,
    universe: np.ndarray,
) -> None:
    np.savez_compressed(
        path,
        sources=sources.astype(np.uint64),
        offsets=offsets.astype(np.int64),
        target_idx=target_idx.astype(np.uint32),
        weights=weights.astype(np.float32),
        universe=universe.astype(np.uint64),
    )


def _load_kernel_cache(path: str) -> tuple[dict[int, tuple[np.ndarray, np.ndarray]], np.ndarray]:
    """Return ({source_uint64: (target_idx uint32, weights)}, universe uint64)."""
    data = np.load(path)
    if "universe" not in data or "target_idx" not in data:
        raise ValueError(
            f"Kernel cache at {path} is an older format; delete it and rerun to rebuild."
        )
    sources = data["sources"]
    offsets = data["offsets"]
    target_idx = data["target_idx"]
    weights = data["weights"]
    universe = data["universe"]
    out: dict[int, tuple[np.ndarray, np.ndarray]] = {}
    for i, source in enumerate(sources):
        start = int(offsets[i])
        end = int(offsets[i + 1])
        out[int(source)] = (target_idx[start:end], weights[start:end])
    return out, universe


def _init_worker_kernels(cache_path: str) -> None:
    global _WORKER_KERNELS, _WORKER_UNIVERSE
    _WORKER_KERNELS, _WORKER_UNIVERSE = _load_kernel_cache(cache_path)


def _assemble_from_kernels(
    aphiaid: int,
    cell_hexes: list[str],
    kernels: dict[int, tuple[np.ndarray, np.ndarray]],
    universe: np.ndarray,
    store_cutoff: float,
) -> tuple[int, pd.DataFrame]:
    """Sum cached per-cell kernels via a fixed H3 index accumulator."""
    if not cell_hexes:
        return aphiaid, pd.DataFrame(columns=["AphiaID", "h3", "density"])
    acc = np.zeros(len(universe), dtype=np.float64)
    for cell in cell_hexes:
        entry = kernels.get(h3.str_to_int(cell))
        if entry is None or len(entry[0]) == 0:
            continue
        idxs, weights = entry
        np.add.at(acc, idxs, weights)
    nz = np.flatnonzero(acc)
    if len(nz) == 0:
        return aphiaid, pd.DataFrame(columns=["AphiaID", "h3", "density"])
    return aphiaid, encode_density_from_maps(
        aphiaid, universe[nz], acc[nz], store_cutoff=store_cutoff
    )


def _process_one_aphiaid_cached(
    aphiaid: int,
    cells: list[str],
    store_cutoff: float,
) -> tuple[int, pd.DataFrame]:
    """Worker entrypoint using the process-global kernel cache."""
    assert _WORKER_KERNELS is not None and _WORKER_UNIVERSE is not None
    return _assemble_from_kernels(
        aphiaid, cells, _WORKER_KERNELS, _WORKER_UNIVERSE, store_cutoff
    )


@dataclass(frozen=True)
class DensityConfig:
    resolution: int = DEFAULT_RESOLUTION
    max_rings: int = DEFAULT_MAX_RINGS
    sd: float = DEFAULT_SD_KM
    density_cutoff: float = DEFAULT_KERNEL_CUTOFF
    store_cutoff: float = DEFAULT_STORE_CUTOFF
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE
    workers: int = DEFAULT_WORKERS


class DensityBuilder:
    """Build compact H3 density surfaces from a speciesgrids GeoParquet product."""

    def __init__(
        self,
        product_path: str,
        build_dir: str = "build",
        config: DensityConfig | None = None,
    ):
        self.product_path = product_path
        self.build_dir = build_dir
        self.config = config or DensityConfig()
        self.work_dir = os.path.join(build_dir, "work", "density")
        self.shards_dir = os.path.join(self.work_dir, "shards")
        self.completed_path = os.path.join(self.work_dir, "completed_aphiaids.txt")
        self.presence_path = os.path.join(self.work_dir, "presence.parquet")
        self.kernel_cache_path = os.path.join(self.work_dir, "kernel_cache.npz")
        self.output_dir = os.path.join(build_dir, f"density_{self.config.resolution}")
        self.output_path = os.path.join(self.output_dir, "data.parquet")

    def build(
        self,
        *,
        force: bool = False,
        aphiaids: Iterable[int] | None = None,
        limit: int | None = None,
        merge_only: bool = False,
    ) -> str:
        """Compute shards (unless merge_only) and write the sorted single-file product."""
        if not os.path.isfile(self.product_path):
            raise FileNotFoundError(
                f"Occurrence product not found: {self.product_path}. "
                "Run the main speciesgrids build first."
            )

        os.makedirs(self.work_dir, exist_ok=True)
        os.makedirs(self.shards_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)

        if force and not merge_only:
            if os.path.isdir(self.shards_dir):
                shutil.rmtree(self.shards_dir)
            os.makedirs(self.shards_dir, exist_ok=True)
            for path in (self.presence_path, self.completed_path, self.kernel_cache_path, self.output_path):
                if os.path.isfile(path):
                    os.remove(path)

        with stage("Build density product"):
            if not merge_only:
                self._ensure_presence(force=force)
                self._ensure_kernel_cache(force=force)
                todo = self._select_aphiaids(aphiaids=aphiaids, limit=limit)
                self._compute_shards(todo, force=force)

            self._merge_shards()
        return self.output_path

    def _ensure_presence(self, *, force: bool) -> None:
        if os.path.isfile(self.presence_path) and not force:
            logger.info("[dim]Reusing presence cells at %s[/dim]", self.presence_path)
            return

        resolution = self.config.resolution
        with stage(f"Extract presence cells at H3 resolution {resolution}"):

            def _extract():
                con = connect(self.work_dir)
                try:
                    con.execute(
                        f"""
                        copy (
                            select
                                AphiaID::integer as AphiaID,
                                h3_cell_to_parent(cell, {resolution}) as h3
                            from read_parquet('{self.product_path}')
                            where AphiaID is not null and cell is not null
                            group by 1, 2
                        ) to '{self.presence_path}' (format parquet, compression zstd)
                        """
                    )
                finally:
                    con.close()

            run_busy(
                "DuckDB presence extract…",
                _extract,
                detail=f"Parent cells from {self.product_path}",
            )
            con = connect(self.work_dir)
            n = con.execute(
                f"select count(distinct AphiaID) from read_parquet('{self.presence_path}')"
            ).fetchone()[0]
            con.close()
            logger.info("Presence table: %s distinct AphiaIDs → %s", f"{n:,}", self.presence_path)

    def _ensure_kernel_cache(self, *, force: bool) -> None:
        if os.path.isfile(self.kernel_cache_path) and not force:
            try:
                kernels, _universe = _load_kernel_cache(self.kernel_cache_path)
                size_mib = os.path.getsize(self.kernel_cache_path) / 1024**2
                logger.info(
                    "[dim]Reusing kernel cache %s (%s sources, %.1f MiB)[/dim]",
                    self.kernel_cache_path,
                    f"{len(kernels):,}",
                    size_mib,
                )
                return
            except ValueError as exc:
                logger.info("%s — rebuilding", exc)

        cfg = self.config
        con = connect(self.work_dir)
        cells = [
            str(r[0])
            for r in con.execute(
                f"select distinct h3 from read_parquet('{self.presence_path}') order by h3"
            ).fetchall()
        ]
        con.close()
        logger.info(
            "Precomputing kernels for %s unique presence cells (%s workers)",
            f"{len(cells):,}",
            cfg.workers,
        )

        universe_hex = _all_cells_at_resolution(cfg.resolution)
        universe = np.fromiter(
            (h3.str_to_int(c) for c in universe_hex),
            dtype=np.uint64,
            count=len(universe_hex),
        )
        cell_to_idx = {int(h): i for i, h in enumerate(universe)}

        results: list[tuple[int, np.ndarray, np.ndarray]] = []
        with stage(f"Build kernel cache ({len(cells):,} cells)"):
            with progress_bar() as progress:
                task = progress.add_task("Unique-cell kernels", total=len(cells))
                if cfg.workers <= 1:
                    latlng = _precompute_latlng(cfg.resolution)
                    for cell in cells:
                        raw = _accumulate_raw(
                            [cell],
                            max_rings=cfg.max_rings,
                            sd=cfg.sd,
                            density_cutoff=cfg.density_cutoff,
                            latlng=latlng,
                        )
                        if raw:
                            target_idx = np.fromiter(
                                (cell_to_idx[h3.str_to_int(k)] for k in raw.keys()),
                                dtype=np.uint32,
                                count=len(raw),
                            )
                            weights = np.fromiter(
                                (raw[k] for k in raw.keys()),
                                dtype=np.float32,
                                count=len(raw),
                            )
                        else:
                            target_idx = np.array([], dtype=np.uint32)
                            weights = np.array([], dtype=np.float32)
                        results.append((h3.str_to_int(cell), target_idx, weights))
                        progress.advance(task)
                else:
                    with ProcessPoolExecutor(max_workers=cfg.workers) as pool:
                        futures = {
                            pool.submit(
                                _compute_one_cell_kernel,
                                cell,
                                cfg.max_rings,
                                cfg.sd,
                                cfg.density_cutoff,
                                cfg.resolution,
                            ): cell
                            for cell in cells
                        }
                        for future in as_completed(futures):
                            source, targets_u64, weights = future.result()
                            if len(targets_u64):
                                target_idx = np.fromiter(
                                    (cell_to_idx[int(t)] for t in targets_u64),
                                    dtype=np.uint32,
                                    count=len(targets_u64),
                                )
                            else:
                                target_idx = np.array([], dtype=np.uint32)
                            results.append((source, target_idx, weights))
                            progress.advance(task)

        results.sort(key=lambda item: item[0])
        sources = np.fromiter((r[0] for r in results), dtype=np.uint64, count=len(results))
        lengths = [len(r[1]) for r in results]
        offsets = np.zeros(len(results) + 1, dtype=np.int64)
        offsets[1:] = np.cumsum(lengths)
        if offsets[-1] == 0:
            target_idx = np.array([], dtype=np.uint32)
            weights = np.array([], dtype=np.float32)
        else:
            target_idx = np.concatenate([r[1] for r in results])
            weights = np.concatenate([r[2] for r in results])
        _save_kernel_cache(
            self.kernel_cache_path, sources, offsets, target_idx, weights, universe
        )
        size_mib = os.path.getsize(self.kernel_cache_path) / 1024**2
        logger.info(
            "Kernel cache: %s sources, %s entries (%.1f MiB) → %s",
            f"{len(sources):,}",
            f"{int(offsets[-1]):,}",
            size_mib,
            self.kernel_cache_path,
        )

    def _select_aphiaids(
        self,
        *,
        aphiaids: Iterable[int] | None,
        limit: int | None,
    ) -> list[int]:
        con = connect(self.work_dir)
        if aphiaids is not None:
            wanted = sorted({int(a) for a in aphiaids})
            con.register("wanted", pd.DataFrame({"AphiaID": wanted}))
            rows = con.execute(
                f"""
                select distinct p.AphiaID
                from read_parquet('{self.presence_path}') p
                inner join wanted w on w.AphiaID = p.AphiaID
                order by p.AphiaID
                """
            ).fetchall()
        else:
            sql = f"""
                select distinct AphiaID
                from read_parquet('{self.presence_path}')
                order by AphiaID
            """
            if limit is not None:
                sql += f" limit {int(limit)}"
            rows = con.execute(sql).fetchall()
        con.close()
        return [int(r[0]) for r in rows]

    def _load_completed(self) -> set[int]:
        done: set[int] = set()
        if os.path.isfile(self.completed_path):
            with open(self.completed_path, encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if line:
                        done.add(int(line))
        shards = [f for f in os.listdir(self.shards_dir) if f.endswith(".parquet")]
        if shards:
            shard_glob = os.path.join(self.shards_dir, "*.parquet")
            con = connect(self.work_dir)
            rows = con.execute(
                f"select distinct AphiaID from read_parquet('{shard_glob}')"
            ).fetchall()
            con.close()
            done.update(int(r[0]) for r in rows)
        return done

    def _append_completed(self, aphiaids: Iterable[int]) -> None:
        with open(self.completed_path, "a", encoding="utf-8") as handle:
            for aphiaid in aphiaids:
                handle.write(f"{int(aphiaid)}\n")

    def _load_cells_map(self, aphiaids: list[int]) -> dict[int, list[str]]:
        if not aphiaids:
            return {}
        con = connect(self.work_dir)
        con.register("wanted", pd.DataFrame({"AphiaID": np.asarray(aphiaids, dtype=np.int32)}))
        df = con.execute(
            f"""
            select p.AphiaID, p.h3
            from read_parquet('{self.presence_path}') p
            inner join wanted w on w.AphiaID = p.AphiaID
            """
        ).fetchdf()
        con.close()
        out: dict[int, list[str]] = {}
        for aphiaid, group in df.groupby("AphiaID", sort=False):
            out[int(aphiaid)] = group["h3"].tolist()
        return out

    def _compute_shards(self, aphiaids: list[int], *, force: bool) -> None:
        cfg = self.config
        completed_ids = set() if force else self._load_completed()
        pending = [a for a in aphiaids if a not in completed_ids]
        already_done = len(aphiaids) - len(pending)
        logger.info(
            "Density shards: [bold]%s[/bold] selected · [green]%s[/green] already done · "
            "[cyan]%s[/cyan] remaining · %s workers (kernel cache)",
            f"{len(aphiaids):,}",
            f"{already_done:,}",
            f"{len(pending):,}",
            cfg.workers,
        )
        if not pending:
            logger.info("[dim]Nothing left to compute[/dim]")
            return

        # Assembly is memory-bound if each spawned worker reloads the ~1 GiB
        # cache. With the indexed accumulator it is fast enough single-threaded.
        with stage("Load kernel cache into memory"):
            kernels, universe = _load_kernel_cache(self.kernel_cache_path)
            logger.info("Loaded %s cached cell kernels", f"{len(kernels):,}")

        batch_size = DEFAULT_SHARD_BATCH_SIZE
        started = time.perf_counter()
        completed = 0
        failed = 0
        batch_index = len([f for f in os.listdir(self.shards_dir) if f.startswith("batch_")])

        with stage(f"Assemble density shards ({len(pending):,} AphiaIDs)"):
            with progress_bar() as progress:
                task = progress.add_task("Density shards", total=len(pending))
                for batch_start in range(0, len(pending), batch_size):
                    batch_ids = pending[batch_start : batch_start + batch_size]
                    cells_map = self._load_cells_map(batch_ids)
                    jobs = [
                        (aphiaid, cells_map[aphiaid])
                        for aphiaid in batch_ids
                        if cells_map.get(aphiaid)
                    ]
                    empty_ids = [a for a in batch_ids if a not in cells_map or not cells_map[a]]
                    frames: list[pd.DataFrame] = []
                    finished_ids: list[int] = list(empty_ids)

                    if empty_ids:
                        completed += len(empty_ids)
                        progress.advance(task, len(empty_ids))

                    for aphiaid, cells in jobs:
                        try:
                            _, encoded = _assemble_from_kernels(
                                aphiaid,
                                cells,
                                kernels,
                                universe,
                                cfg.store_cutoff,
                            )
                        except Exception:
                            failed += 1
                            logger.exception("Density failed for AphiaID %s", aphiaid)
                            raise
                        finished_ids.append(aphiaid)
                        if not encoded.empty:
                            frames.append(encoded)
                        completed += 1
                        progress.update(
                            task,
                            advance=1,
                            description=f"Density shards · last {aphiaid}",
                        )

                    if frames:
                        batch_path = os.path.join(
                            self.shards_dir, f"batch_{batch_index:05d}.parquet"
                        )
                        _write_density_table(batch_path, pd.concat(frames, ignore_index=True))
                    self._append_completed(finished_ids)
                    batch_index += 1

                    elapsed = time.perf_counter() - started
                    if completed and elapsed >= 15 and (batch_index % 5 == 0):
                        rate = completed / elapsed
                        remaining = len(pending) - completed
                        eta_s = remaining / rate if rate > 0 else float("inf")
                        logger.info(
                            "[dim]… %s/%s shards (%.1f/s) · ETA ~%.1f min[/dim]",
                            f"{completed:,}",
                            f"{len(pending):,}",
                            rate,
                            eta_s / 60.0,
                        )

        elapsed = time.perf_counter() - started
        rate = completed / elapsed if elapsed > 0 else 0.0
        logger.info(
            "Shard assemble finished: %s done in %.1f min (%.1f/s)%s",
            f"{completed:,}",
            elapsed / 60.0,
            rate,
            f" · {failed} failed" if failed else "",
        )

    def _merge_shards(self) -> None:
        shard_glob = os.path.join(self.shards_dir, "*.parquet")
        shards = [f for f in os.listdir(self.shards_dir) if f.endswith(".parquet")]
        if not shards:
            raise FileNotFoundError(
                f"No density shards in {self.shards_dir}. Run without --merge-only first."
            )

        with stage(f"Merge {len(shards):,} shards → {self.output_path}"):
            tmp_path = os.path.join(self.work_dir, "merged_sorted_tmp.parquet")

            def _sort_merge():
                con = connect(self.work_dir)
                try:
                    con.execute(
                        f"""
                        copy (
                            select
                                AphiaID::integer as AphiaID,
                                h3::ubigint as h3,
                                density::usmallint as density
                            from read_parquet('{shard_glob}')
                            order by AphiaID, h3
                        ) to '{tmp_path}' (format parquet, compression zstd)
                        """
                    )
                finally:
                    con.close()

            run_busy(
                "DuckDB sort-merge shards…",
                _sort_merge,
                detail=f"{len(shards):,} shard files",
            )

            con = connect(self.work_dir)
            n_rows = con.execute(f"select count(*) from read_parquet('{tmp_path}')").fetchone()[0]
            con.close()
            logger.info("Sorted merge has %s rows; rewriting row groups…", f"{n_rows:,}")

            pf = pq.ParquetFile(tmp_path)
            n_batches = max(1, (n_rows + self.config.row_group_size - 1) // self.config.row_group_size)
            writer = None
            try:
                with progress_bar() as progress:
                    task = progress.add_task("Write row groups", total=n_batches)
                    for batch in pf.iter_batches(batch_size=self.config.row_group_size):
                        table = pa.Table.from_batches([batch]).cast(
                            pa.schema(
                                [
                                    ("AphiaID", pa.int32()),
                                    ("h3", pa.uint64()),
                                    ("density", pa.uint16()),
                                ]
                            )
                        )
                        if writer is None:
                            writer = pq.ParquetWriter(
                                self.output_path,
                                table.schema,
                                compression="zstd",
                            )
                        writer.write_table(table)
                        progress.advance(task)
            finally:
                if writer is not None:
                    writer.close()
            os.remove(tmp_path)

            size_mib = os.path.getsize(self.output_path) / 1024**2
            logger.info(
                "Wrote %s rows (%.1f MiB) → %s",
                f"{n_rows:,}",
                size_mib,
                self.output_path,
            )


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    parser = argparse.ArgumentParser(
        description="Build compact H3 density surfaces from a speciesgrids product."
    )
    parser.add_argument(
        "--product",
        default=os.path.join("build", "h3_7", "data.parquet"),
        help="Path to occurrence GeoParquet (default: build/h3_7/data.parquet)",
    )
    parser.add_argument("--build-dir", default="build", help="Build root (default: build)")
    parser.add_argument("--resolution", type=int, default=DEFAULT_RESOLUTION)
    parser.add_argument("--max-rings", type=int, default=DEFAULT_MAX_RINGS)
    parser.add_argument("--sd", type=float, default=DEFAULT_SD_KM)
    parser.add_argument("--kernel-cutoff", type=float, default=DEFAULT_KERNEL_CUTOFF)
    parser.add_argument("--store-cutoff", type=float, default=DEFAULT_STORE_CUTOFF)
    parser.add_argument("--row-group-size", type=int, default=DEFAULT_ROW_GROUP_SIZE)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--force", action="store_true", help="Recompute presence, kernels, and shards")
    parser.add_argument(
        "--merge-only",
        action="store_true",
        help="Only merge existing shards into the final file",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N AphiaIDs (for pilots)",
    )
    parser.add_argument(
        "--aphiaids",
        default=None,
        help="Comma-separated AphiaIDs to process (overrides --limit)",
    )
    args = parser.parse_args(argv)

    aphiaids = None
    if args.aphiaids:
        aphiaids = [int(x.strip()) for x in args.aphiaids.split(",") if x.strip()]

    builder = DensityBuilder(
        product_path=args.product,
        build_dir=args.build_dir,
        config=DensityConfig(
            resolution=args.resolution,
            max_rings=args.max_rings,
            sd=args.sd,
            density_cutoff=args.kernel_cutoff,
            store_cutoff=args.store_cutoff,
            row_group_size=args.row_group_size,
            workers=args.workers,
        ),
    )
    out = builder.build(
        force=args.force,
        aphiaids=aphiaids,
        limit=args.limit,
        merge_only=args.merge_only,
    )
    logger.info("Density product ready: %s", out)


if __name__ == "__main__":
    main()
