import h3pandas  # noqa: F401
import pyarrow.dataset as ds


parquet_dataset = ds.dataset("../build/h3_7/data.parquet", format="parquet")
scanner = parquet_dataset.to_batches(batch_size=10000)


for batch in scanner:
    df = batch.to_pandas()
    df = df.set_index("cell").h3.h3_to_parent(6)
    print(df.head())
