import pyarrow.dataset as ds
import h3pandas  # noqa: F401


parquet_dataset = ds.dataset("h3_7", format="parquet")
scanner = parquet_dataset.to_batches(batch_size=10000)


for batch in scanner:
    df = batch.to_pandas()
    df = df.set_index("h3_07").h3.h3_to_parent(6)
    print(df.head())
