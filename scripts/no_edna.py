"""Build a no-eDNA speciesgrids product: H3 grid, density, and thermal profiles.

GBIF input is the occurrence cube that excludes datasets in category ``eDNA``
(download https://www.gbif.org/occurrence/download/0024142-260928105237408).

OBIS input is the occurrence parquet directory. Dataset ids to drop are those
whose ``tags`` include ``EnvironmentalDna`` on https://api.obis.org/dataset.
The ``tags`` query parameter does not return every tagged dataset, so the
script pages the catalog and filters the tag list on each record.

Outputs go to ``build_no_edna/`` and do not replace ``build/``.

    python scripts/no_edna.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pyarrow as pa
import pyarrow.parquet as pq

from speciesgrids import DatasetBuilder
from speciesgrids.density import DensityBuilder, DensityConfig
from speciesgrids.grids import H3Grid
from speciesgrids.progress import configure_logging
from speciesgrids.thermal import TEMPERATURE_ENV, ThermalBuilder, ThermalConfig
EDNA_TAG = "https://rs.obis.org/obis/vocabulary/DatasetType/EnvironmentalDna"
GBIF_CUBE_DOWNLOAD = "0024142-260928105237408"

DEFAULT_OBIS = "/Volumes/acasis/occurrence"
DEFAULT_GBIF = f"/Volumes/acasis/gbif/{GBIF_CUBE_DOWNLOAD}_noedna.csv"
DEFAULT_WORMS = "/Volumes/acasis/worms/worms_draft_20260915.db"
DEFAULT_TEMPERATURE = "/Volumes/acasis/temperature/climatologydecadedepthsurf.nc"
DEFAULT_BUILD = ROOT / "build_no_edna"


def edna_dataset_ids() -> list[str]:
    """Page https://api.obis.org/dataset and keep EnvironmentalDna ids."""
    ids: list[str] = []
    skip = 0
    size = 1000
    total: int | None = None
    while total is None or skip < total:
        query = urllib.parse.urlencode({"size": size, "skip": skip})
        request = urllib.request.Request(
            f"https://api.obis.org/dataset?{query}",
            headers={"User-Agent": "speciesgrids"},
        )
        with urllib.request.urlopen(request, timeout=180) as response:
            payload = json.load(response)
        total = int(payload["total"])
        results = payload["results"]
        if not results:
            break
        for item in results:
            if EDNA_TAG not in (item.get("tags") or []):
                continue
            dataset_id = str(item["id"]).lower()
            if not _is_uuid(dataset_id):
                raise SystemExit(f"OBIS dataset id is not a UUID: {dataset_id}")
            ids.append(dataset_id)
        skip += len(results)
    ids = sorted(set(ids))
    if not ids:
        raise SystemExit("OBIS API returned no EnvironmentalDna datasets")
    return ids


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
    except ValueError:
        return False
    return True


def write_exclusion_list(ids: list[str], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table({"dataset_id": pa.array(ids, type=pa.string())}),
        path,
        compression="zstd",
    )
    return path


def obis_predicate(exclusion_parquet: Path) -> str:
    # NULL dataset_id is kept. NOT IN on NULL would drop the row.
    quoted = str(exclusion_parquet).replace("'", "''")
    return (
        "(dataset_id IS NULL OR dataset_id NOT IN "
        f"(SELECT dataset_id FROM read_parquet('{quoted}')))"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--obis", default=DEFAULT_OBIS, help="OBIS occurrence parquet directory")
    parser.add_argument("--gbif", default=DEFAULT_GBIF, help="No-eDNA GBIF occurrence cube TSV")
    parser.add_argument("--worms", default=DEFAULT_WORMS, help="WoRMS SQLite database")
    parser.add_argument(
        "--temperature",
        default=os.environ.get(TEMPERATURE_ENV, DEFAULT_TEMPERATURE),
        help=f"Bio-Oracle WOA thetao NetCDF (or set {TEMPERATURE_ENV})",
    )
    parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD)
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--force", action="store_true", help="Recompute cached grid, density, and thermal artefacts")
    parser.add_argument("--grid-only", action="store_true", help="Stop after the H3 occurrence grid")
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()

    gbif = Path(args.gbif)
    temperature = Path(args.temperature) if args.temperature else None
    if not gbif.is_file():
        raise SystemExit(
            f"GBIF cube not found: {gbif}\n"
            "Unzip the no-eDNA cube from "
            f"https://www.gbif.org/occurrence/download/{GBIF_CUBE_DOWNLOAD}"
        )
    if not args.grid_only:
        if temperature is None or not temperature.is_file():
            raise SystemExit(
                f"--temperature is required (or set {TEMPERATURE_ENV}) "
                "before density and thermal run."
            )

    build_dir = args.build_dir
    exclusion = write_exclusion_list(
        edna_dataset_ids(),
        build_dir / "work" / "edna_dataset_ids.parquet",
    )

    builder = DatasetBuilder(
        sources={"obis": args.obis, "gbif": str(gbif)},
        grid=H3Grid(7),
        build_dir=str(build_dir),
        worms_sqlite_path=args.worms,
        predicates=[obis_predicate(exclusion)],
    )
    builder.build(prepare=True, aggregate=True, merge=True, force=args.force)
    if args.grid_only:
        return

    product = build_dir / "h3_7" / "data.parquet"
    workers = {} if args.workers is None else {"workers": args.workers}
    DensityBuilder(
        str(product),
        build_dir=str(build_dir),
        config=DensityConfig(**workers),
    ).build(force=args.force)
    ThermalBuilder(
        str(product),
        str(temperature),
        build_dir=str(build_dir),
        config=ThermalConfig(**workers),
    ).build(force=args.force)


if __name__ == "__main__":
    main()
