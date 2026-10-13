"""Thermal KDE profiles + shared H3 SST map for on-the-fly suitability.

Separate from the OBIS/GBIF aggregation and density pipelines. Matches Speedy /
edna_qc thermal suitability:

    suitability(h3) = gaussian_kde(occurrence_SST)(thetao[h3]) / norm_max

Exports under ``build/thermal_{resolution}/``:

    thetao.parquet      # h3 uint64, thetao float32
    profiles.parquet    # AphiaID, bandwidth, n_temps, temps, norm_max
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Iterable

import h3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr
from scipy.stats import gaussian_kde

from speciesgrids.db import connect
from speciesgrids.progress import configure_logging, progress_bar, stage


logger = logging.getLogger(__name__)

DEFAULT_RESOLUTION = 3
DEFAULT_COARSEN = 4
DEFAULT_TIME = "2010-01-01"
DEFAULT_BANDWIDTH = 2.0
DEFAULT_MIN_TEMPS = 3
DEFAULT_WORKERS = max(1, (os.cpu_count() or 2) - 1)
DEFAULT_PROFILE_BATCH = 2_000
TEMPERATURE_ENV = "SPECIESGRIDS_TEMPERATURE"
TEMPERATURE_URL = (
    "https://erddap.bio-oracle.org/erddap/files/"
    "thetao_baseline_2000_2019_depthsurf/climatologydecadedepthsurf.nc"
)

# Process-local temperature vector for profile workers (finite thetao only).
_WORKER_THETAO_FINITE: np.ndarray | None = None


def _all_cells_at_resolution(resolution: int) -> list[str]:
    cells: set[str] = set()
    for res0 in h3.get_res0_cells():
        cells.update(h3.cell_to_children(res0, resolution))
    return sorted(cells)


def load_thetao_xarray(
    temperature_path: str,
    *,
    time: str = DEFAULT_TIME,
    coarsen: int = DEFAULT_COARSEN,
) -> xr.DataArray:
    """Open WOA/Bio-Oracle SST and apply edna_qc time + coarsen settings."""
    xds = xr.open_dataset(temperature_path, engine="netcdf4")
    thetao = xds["thetao_mean"].sel(time=time)
    if coarsen > 1:
        thetao = thetao.coarsen(lat=coarsen, lon=coarsen, boundary="exact").mean()
    return thetao


@dataclass(frozen=True)
class _ThetaoGrid:
    """Numpy view of a coarsened SST field for fast nearest-neighbour lookups."""

    lats: np.ndarray
    lons: np.ndarray
    values: np.ndarray  # shape (n_lat, n_lon)

    @classmethod
    def from_dataarray(cls, thetao: xr.DataArray) -> "_ThetaoGrid":
        return cls(
            lats=np.asarray(thetao["lat"].values, dtype=np.float64),
            lons=np.asarray(thetao["lon"].values, dtype=np.float64),
            values=np.asarray(thetao.values, dtype=np.float64),
        )

    def sample(self, lat: float, lon: float) -> float:
        i = int(np.abs(self.lats - lat).argmin())
        j = int(np.abs(self.lons - lon).argmin())
        v = self.values[i, j]
        return float(v) if np.isfinite(v) else float("nan")


def sample_thetao_at_latlng(thetao: xr.DataArray | _ThetaoGrid, lat: float, lon: float) -> float:
    """Nearest-neighbour SST sample (°C); NaN if missing."""
    if isinstance(thetao, _ThetaoGrid):
        return thetao.sample(lat, lon)
    value = thetao.sel(lat=lat, lon=lon, method="nearest").item()
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return float("nan")
    return float(value)


def sample_cell_mean_thetao(thetao: xr.DataArray | _ThetaoGrid, cell: str) -> float:
    """Mean SST at H3 cell corners (edna_qc ``sample_cell_mean``)."""
    corners = h3.cell_to_boundary(cell)
    vals: list[float] = []
    for lat, lon in corners:
        v = sample_thetao_at_latlng(thetao, lat, lon)
        if not np.isnan(v):
            vals.append(v)
    if not vals:
        latc, lonc = h3.cell_to_latlng(cell)
        return sample_thetao_at_latlng(thetao, latc, lonc)
    return float(np.mean(vals))


def build_thetao_h3_table(
    thetao: xr.DataArray,
    *,
    resolution: int = DEFAULT_RESOLUTION,
) -> pd.DataFrame:
    """Shared H3 SST map: columns ``h3`` (uint64), ``thetao`` (float32)."""
    grid = _ThetaoGrid.from_dataarray(thetao)
    cells = _all_cells_at_resolution(resolution)
    values = np.empty(len(cells), dtype=np.float32)
    with progress_bar() as progress:
        task = progress.add_task("H3 SST map", total=len(cells))
        for i, cell in enumerate(cells):
            values[i] = np.float32(sample_cell_mean_thetao(grid, cell))
            progress.advance(task)
    h3_u64 = np.fromiter((h3.str_to_int(c) for c in cells), dtype=np.uint64, count=len(cells))
    return pd.DataFrame({"h3": h3_u64, "thetao": values}).sort_values("h3").reset_index(drop=True)


def _occurrence_cells(product_path: str) -> pd.DataFrame:
    """Distinct (AphiaID, cell) from the occurrence GeoParquet."""
    con = connect()
    df = con.execute(
        f"""
        select distinct AphiaID::integer as AphiaID, cell
        from read_parquet('{product_path}')
        where AphiaID is not null and cell is not null
        """
    ).fetchdf()
    con.close()
    return df


def _sample_unique_cell_temps(
    thetao: xr.DataArray | _ThetaoGrid,
    cells: list[str],
) -> dict[str, float]:
    """SST at each unique H3-7 occurrence cell centroid."""
    grid = thetao if isinstance(thetao, _ThetaoGrid) else _ThetaoGrid.from_dataarray(thetao)
    out: dict[str, float] = {}
    with progress_bar() as progress:
        task = progress.add_task("Sample SST at occurrence cells", total=len(cells))
        for cell in cells:
            lat, lon = h3.cell_to_latlng(cell)
            out[cell] = grid.sample(lat, lon)
            progress.advance(task)
    return out


def _fit_profile(
    aphiaid: int,
    temps: np.ndarray,
    thetao_finite: np.ndarray,
    bandwidth: float,
) -> tuple[int, float, np.ndarray, float] | None:
    """Return (AphiaID, bandwidth, temps, norm_max) or None if unfit / singular."""
    temps = temps[np.isfinite(temps)]
    if len(temps) < DEFAULT_MIN_TEMPS:
        return None
    # gaussian_kde needs non-zero variance (identical SST → singular covariance).
    if float(np.nanstd(temps)) <= 0.0:
        return None
    try:
        kde = gaussian_kde(temps, bw_method=bandwidth)
        dens = kde.evaluate(thetao_finite)
    except (np.linalg.LinAlgError, ValueError):
        return None
    peak = float(np.nanmax(dens)) if len(dens) else 0.0
    if not np.isfinite(peak) or peak <= 0:
        return None
    return int(aphiaid), float(bandwidth), temps.astype(np.float32), float(peak)


def _init_profile_worker(thetao_finite: np.ndarray) -> None:
    global _WORKER_THETAO_FINITE
    _WORKER_THETAO_FINITE = thetao_finite


def _fit_profile_worker(
    aphiaid: int,
    temps: np.ndarray,
    bandwidth: float,
) -> tuple[int, float, np.ndarray, float] | None:
    assert _WORKER_THETAO_FINITE is not None
    return _fit_profile(aphiaid, temps, _WORKER_THETAO_FINITE, bandwidth)


def suitability_for_aphiaid(
    aphiaid: int,
    profiles: pd.DataFrame,
    thetao_df: pd.DataFrame,
) -> pd.DataFrame:
    """On-the-fly suitability map for one AphiaID.

    Returns a DataFrame with columns ``h3`` (uint64) and ``suitability`` (float64),
    aligned to ``thetao_df`` rows that have finite SST. Raises ``KeyError`` if the
    AphiaID has no profile.
    """
    match = profiles.loc[profiles["AphiaID"] == int(aphiaid)]
    if match.empty:
        raise KeyError(f"No thermal profile for AphiaID {aphiaid}")
    row = match.iloc[0]
    temps = np.asarray(row["temps"], dtype=np.float64)
    bandwidth = float(row["bandwidth"])
    norm_max = float(row["norm_max"])
    kde = gaussian_kde(temps, bw_method=bandwidth)

    thetao = thetao_df["thetao"].to_numpy(dtype=np.float64)
    h3_ids = thetao_df["h3"].to_numpy()
    finite = np.isfinite(thetao)
    suit = np.full(len(thetao), np.nan, dtype=np.float64)
    if finite.any() and norm_max > 0:
        suit[finite] = kde.evaluate(thetao[finite]) / norm_max
    return pd.DataFrame({"h3": h3_ids, "suitability": suit})


@dataclass(frozen=True)
class ThermalConfig:
    resolution: int = DEFAULT_RESOLUTION
    coarsen: int = DEFAULT_COARSEN
    time: str = DEFAULT_TIME
    bandwidth: float = DEFAULT_BANDWIDTH
    workers: int = DEFAULT_WORKERS


class ThermalBuilder:
    """Build shared H3 SST map + per-AphiaID thermal KDE profiles."""

    def __init__(
        self,
        product_path: str,
        temperature_path: str,
        build_dir: str = "build",
        config: ThermalConfig | None = None,
    ):
        self.product_path = product_path
        self.temperature_path = temperature_path
        self.build_dir = build_dir
        self.config = config or ThermalConfig()
        self.work_dir = os.path.join(build_dir, "work", "thermal")
        self.output_dir = os.path.join(build_dir, f"thermal_{self.config.resolution}")
        self.thetao_path = os.path.join(self.output_dir, "thetao.parquet")
        self.profiles_path = os.path.join(self.output_dir, "profiles.parquet")
        self.profiles_partial_dir = os.path.join(self.work_dir, "profile_batches")

    def build(
        self,
        *,
        force: bool = False,
        aphiaids: Iterable[int] | None = None,
        limit: int | None = None,
    ) -> tuple[str, str]:
        if not os.path.isfile(self.product_path):
            raise FileNotFoundError(
                f"Occurrence product not found: {self.product_path}. "
                "Run the main speciesgrids build first."
            )
        if not os.path.isfile(self.temperature_path):
            raise FileNotFoundError(
                f"Temperature NetCDF not found: {self.temperature_path}. "
                f"Download from {TEMPERATURE_URL}"
            )

        os.makedirs(self.work_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.profiles_partial_dir, exist_ok=True)

        if force:
            for path in (self.thetao_path, self.profiles_path):
                if os.path.isfile(path):
                    os.remove(path)
            if os.path.isdir(self.profiles_partial_dir):
                shutil.rmtree(self.profiles_partial_dir)
                os.makedirs(self.profiles_partial_dir, exist_ok=True)

        with stage("Build thermal artefacts"):
            thetao_da = self._load_temperature()
            thetao_df = self._ensure_thetao_map(thetao_da, force=force)
            self._build_profiles(thetao_da, thetao_df, aphiaids=aphiaids, limit=limit, force=force)

        return self.thetao_path, self.profiles_path

    def _load_temperature(self) -> xr.DataArray:
        cfg = self.config
        with stage("Load and coarsen temperature NetCDF"):
            logger.info(
                "Reading %s (time=%s, coarsen=%s)",
                self.temperature_path,
                cfg.time,
                cfg.coarsen,
            )

            thetao = load_thetao_xarray(
                self.temperature_path,
                time=cfg.time,
                coarsen=cfg.coarsen,
            )
            logger.info(
                "thetao grid %s × %s",
                int(thetao.sizes.get("lat", 0)),
                int(thetao.sizes.get("lon", 0)),
            )
            return thetao

    def _ensure_thetao_map(self, thetao: xr.DataArray, *, force: bool) -> pd.DataFrame:
        if os.path.isfile(self.thetao_path) and not force:
            logger.info("[dim]Reusing SST map %s[/dim]", self.thetao_path)
            return pq.read_table(self.thetao_path).to_pandas()

        with stage(f"Build H3-{self.config.resolution} SST map → {self.thetao_path}"):
            df = build_thetao_h3_table(thetao, resolution=self.config.resolution)
            table = pa.Table.from_pandas(
                df,
                schema=pa.schema([("h3", pa.uint64()), ("thetao", pa.float32())]),
                preserve_index=False,
            )
            pq.write_table(table, self.thetao_path, compression="zstd")
            n_finite = int(np.isfinite(df["thetao"].to_numpy()).sum())
            logger.info(
                "Wrote %s H3 cells (%s with SST) → %s (%.1f MiB)",
                f"{len(df):,}",
                f"{n_finite:,}",
                self.thetao_path,
                os.path.getsize(self.thetao_path) / 1024**2,
            )
            return df

    def _done_aphiaids(self) -> set[int]:
        done: set[int] = set()
        if os.path.isfile(self.profiles_path):
            con = connect(self.work_dir)
            rows = con.execute(
                f"select AphiaID from read_parquet('{self.profiles_path}')"
            ).fetchall()
            con.close()
            done.update(int(r[0]) for r in rows)
        batches = [
            os.path.join(self.profiles_partial_dir, f)
            for f in os.listdir(self.profiles_partial_dir)
            if f.endswith(".parquet")
        ]
        if batches:
            con = connect(self.work_dir)
            glob = os.path.join(self.profiles_partial_dir, "*.parquet")
            rows = con.execute(
                f"select distinct AphiaID from read_parquet('{glob}')"
            ).fetchall()
            con.close()
            done.update(int(r[0]) for r in rows)
        return done

    def _build_profiles(
        self,
        thetao: xr.DataArray,
        thetao_df: pd.DataFrame,
        *,
        aphiaids: Iterable[int] | None,
        limit: int | None,
        force: bool,
    ) -> None:
        cfg = self.config
        with stage("Load occurrence cells from product"):
            presence = _occurrence_cells(self.product_path)
            logger.info(
                "Occurrence pairs: %s rows, %s AphiaIDs",
                f"{len(presence):,}",
                f"{presence['AphiaID'].nunique():,}",
            )

        if aphiaids is not None:
            wanted = {int(a) for a in aphiaids}
            presence = presence[presence["AphiaID"].isin(wanted)]
        all_ids = sorted(presence["AphiaID"].unique().tolist())
        if limit is not None:
            all_ids = all_ids[: int(limit)]
            presence = presence[presence["AphiaID"].isin(all_ids)]

        done = set() if force else self._done_aphiaids()
        pending_ids = [a for a in all_ids if a not in done]
        logger.info(
            "Thermal profiles: [bold]%s[/bold] selected · [green]%s[/green] done · "
            "[cyan]%s[/cyan] remaining",
            f"{len(all_ids):,}",
            f"{len(all_ids) - len(pending_ids):,}",
            f"{len(pending_ids):,}",
        )
        if not pending_ids:
            if not os.path.isfile(self.profiles_path):
                self._merge_profiles()
            else:
                logger.info("[dim]Profiles already complete[/dim]")
            return

        with stage("Sample SST at unique occurrence cell centroids"):
            unique_cells = sorted(presence["cell"].astype(str).unique().tolist())
            cell_temps = _sample_unique_cell_temps(thetao, unique_cells)

        thetao_finite = thetao_df["thetao"].to_numpy(dtype=np.float64)
        thetao_finite = thetao_finite[np.isfinite(thetao_finite)]
        if len(thetao_finite) == 0:
            raise RuntimeError("No finite SST values in H3 temperature map")

        # Group presence cells per pending AphiaID
        presence_pending = presence[presence["AphiaID"].isin(pending_ids)]
        cells_by_id: dict[int, list[str]] = {
            int(a): g["cell"].astype(str).tolist()
            for a, g in presence_pending.groupby("AphiaID", sort=False)
        }

        batch_index = len(
            [f for f in os.listdir(self.profiles_partial_dir) if f.endswith(".parquet")]
        )
        completed = 0
        skipped = 0

        with stage(f"Fit thermal KDEs ({len(pending_ids):,} AphiaIDs)"):
            with progress_bar() as progress:
                task = progress.add_task("Thermal profiles", total=len(pending_ids))
                for batch_start in range(0, len(pending_ids), DEFAULT_PROFILE_BATCH):
                    batch_ids = pending_ids[batch_start : batch_start + DEFAULT_PROFILE_BATCH]
                    jobs: list[tuple[int, np.ndarray]] = []
                    for aphiaid in batch_ids:
                        temps = np.array(
                            [
                                cell_temps[c]
                                for c in cells_by_id.get(aphiaid, [])
                                if c in cell_temps and np.isfinite(cell_temps[c])
                            ],
                            dtype=np.float64,
                        )
                        jobs.append((aphiaid, temps))

                    rows: list[dict] = []
                    if cfg.workers <= 1:
                        for aphiaid, temps in jobs:
                            result = _fit_profile(aphiaid, temps, thetao_finite, cfg.bandwidth)
                            if result is None:
                                skipped += 1
                            else:
                                aid, bw, tarr, norm_max = result
                                rows.append(
                                    {
                                        "AphiaID": aid,
                                        "bandwidth": np.float32(bw),
                                        "n_temps": np.uint32(len(tarr)),
                                        "temps": tarr.tolist(),
                                        "norm_max": np.float32(norm_max),
                                    }
                                )
                            completed += 1
                            progress.update(
                                task,
                                advance=1,
                                description=f"Thermal profiles · last {aphiaid}",
                            )
                    else:
                        with ProcessPoolExecutor(
                            max_workers=cfg.workers,
                            initializer=_init_profile_worker,
                            initargs=(thetao_finite,),
                        ) as pool:
                            futures = {
                                pool.submit(
                                    _fit_profile_worker, aphiaid, temps, cfg.bandwidth
                                ): aphiaid
                                for aphiaid, temps in jobs
                            }
                            for future in as_completed(futures):
                                aphiaid = futures[future]
                                result = future.result()
                                if result is None:
                                    skipped += 1
                                else:
                                    aid, bw, tarr, norm_max = result
                                    rows.append(
                                        {
                                            "AphiaID": aid,
                                            "bandwidth": np.float32(bw),
                                            "n_temps": np.uint32(len(tarr)),
                                            "temps": tarr.tolist(),
                                            "norm_max": np.float32(norm_max),
                                        }
                                    )
                                completed += 1
                                progress.update(
                                    task,
                                    advance=1,
                                    description=f"Thermal profiles · last {aphiaid}",
                                )

                    if rows:
                        batch_path = os.path.join(
                            self.profiles_partial_dir, f"batch_{batch_index:05d}.parquet"
                        )
                        self._write_profile_batch(batch_path, rows)
                        batch_index += 1

        logger.info(
            "Profile fit finished: %s processed, %s skipped (< %s temps or singular KDE)",
            f"{completed:,}",
            f"{skipped:,}",
            DEFAULT_MIN_TEMPS,
        )
        self._merge_profiles()

    def _write_profile_batch(self, path: str, rows: list[dict]) -> None:
        df = pd.DataFrame(rows)
        table = pa.Table.from_pydict(
            {
                "AphiaID": df["AphiaID"].astype(np.int32),
                "bandwidth": df["bandwidth"].astype(np.float32),
                "n_temps": df["n_temps"].astype(np.uint32),
                "temps": df["temps"].tolist(),
                "norm_max": df["norm_max"].astype(np.float32),
            },
            schema=pa.schema(
                [
                    ("AphiaID", pa.int32()),
                    ("bandwidth", pa.float32()),
                    ("n_temps", pa.uint32()),
                    ("temps", pa.list_(pa.float32())),
                    ("norm_max", pa.float32()),
                ]
            ),
        )
        pq.write_table(table, path, compression="zstd")

    def _merge_profiles(self) -> None:
        batches = [
            f
            for f in os.listdir(self.profiles_partial_dir)
            if f.endswith(".parquet")
        ]
        if not batches and os.path.isfile(self.profiles_path):
            return
        if not batches:
            # Write empty schema file
            table = pa.table(
                {
                    "AphiaID": pa.array([], type=pa.int32()),
                    "bandwidth": pa.array([], type=pa.float32()),
                    "n_temps": pa.array([], type=pa.uint32()),
                    "temps": pa.array([], type=pa.list_(pa.float32())),
                    "norm_max": pa.array([], type=pa.float32()),
                }
            )
            pq.write_table(table, self.profiles_path, compression="zstd")
            logger.info("Wrote empty profiles → %s", self.profiles_path)
            return

        with stage(f"Merge {len(batches):,} profile batches → {self.profiles_path}"):
            paths = [
                os.path.join(self.profiles_partial_dir, f) for f in sorted(batches)
            ]
            table = pq.read_table(paths)
            # Deduplicate by AphiaID (keep last) in case of resumed overlaps.
            df = table.to_pandas()
            df = df.drop_duplicates(subset=["AphiaID"], keep="last").sort_values("AphiaID")
            out = pa.Table.from_pandas(
                df,
                schema=pa.schema(
                    [
                        ("AphiaID", pa.int32()),
                        ("bandwidth", pa.float32()),
                        ("n_temps", pa.uint32()),
                        ("temps", pa.list_(pa.float32())),
                        ("norm_max", pa.float32()),
                    ]
                ),
                preserve_index=False,
            )
            pq.write_table(out, self.profiles_path, compression="zstd")
            logger.info(
                "Wrote %s profiles → %s (%.1f MiB)",
                f"{out.num_rows:,}",
                self.profiles_path,
                os.path.getsize(self.profiles_path) / 1024**2,
            )


def main(argv: list[str] | None = None) -> None:
    configure_logging()
    parser = argparse.ArgumentParser(
        description="Build H3 SST map + per-AphiaID thermal KDE profiles for on-the-fly suitability."
    )
    parser.add_argument(
        "--product",
        default=os.path.join("build", "h3_7", "data.parquet"),
        help="Occurrence GeoParquet (default: build/h3_7/data.parquet)",
    )
    parser.add_argument(
        "--temperature",
        default=os.environ.get(TEMPERATURE_ENV),
        help=f"Path to WOA/Bio-Oracle NetCDF (or set {TEMPERATURE_ENV}). "
        f"Download: {TEMPERATURE_URL}",
    )
    parser.add_argument("--build-dir", default="build")
    parser.add_argument("--resolution", type=int, default=DEFAULT_RESOLUTION)
    parser.add_argument("--coarsen", type=int, default=DEFAULT_COARSEN)
    parser.add_argument("--time", default=DEFAULT_TIME)
    parser.add_argument("--bandwidth", type=float, default=DEFAULT_BANDWIDTH)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--aphiaids",
        default=None,
        help="Comma-separated AphiaIDs (overrides --limit)",
    )
    args = parser.parse_args(argv)

    if not args.temperature:
        parser.error(
            f"--temperature is required (or set {TEMPERATURE_ENV}). "
            f"Download from {TEMPERATURE_URL}"
        )

    aphiaids = None
    if args.aphiaids:
        aphiaids = [int(x.strip()) for x in args.aphiaids.split(",") if x.strip()]

    builder = ThermalBuilder(
        product_path=args.product,
        temperature_path=args.temperature,
        build_dir=args.build_dir,
        config=ThermalConfig(
            resolution=args.resolution,
            coarsen=args.coarsen,
            time=args.time,
            bandwidth=args.bandwidth,
            workers=args.workers,
        ),
    )
    thetao_path, profiles_path = builder.build(
        force=args.force,
        aphiaids=aphiaids,
        limit=args.limit,
    )
    logger.info("Thermal SST map: %s", thetao_path)
    logger.info("Thermal profiles: %s", profiles_path)


if __name__ == "__main__":
    main()
