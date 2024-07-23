import pyarrow.parquet as pq
import os

batch_size = 1000000
parquet_path = "data/obis_20220710.parquet"
output_dir = "data/obis_20220710"

parquet_file = pq.ParquetFile(parquet_path)

i = 0
for batch in parquet_file.iter_batches(batch_size=batch_size):
    writer = pq.ParquetWriter(os.path.join(output_dir, f"{i}.parquet"), batch.schema)
    writer.write_batch(batch)
    print(f"Written batch {i}")
    writer.close()
    i = i + 1
