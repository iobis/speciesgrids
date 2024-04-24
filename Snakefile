from scripts.lib import get_quadkeys


DATASETS = ["obis", "gbif"]
QUADKEY_LEVEL = 5
H3_RESOLUTION = 7


def dataset_subsets(dataset):
    results = glob_wildcards("data/" + dataset + "/{subset}.parquet")
    return results.subset


for dataset in DATASETS:

    subsets = dataset_subsets(dataset)

    rule:
        input:
            expand("data/{dataset}/{subset}.parquet", dataset=[dataset], subset=subsets)
        output:
            expand("output/{dataset}/{subset}/{block}.parquet", dataset=[dataset], subset=subsets, block=get_quadkeys(QUADKEY_LEVEL))
        shell:
            "python scripts/index_blocks.py {input} {QUADKEY_LEVEL} {H3_RESOLUTION}"
