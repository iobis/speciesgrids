import pyarrow.parquet as pq
import os
import pyarrow as pa
import pyarrow.compute as pc

batch_size = 1000000
parquet_path = "data/obis_20240723.parquet"
output_dir = "data/obis_20240723"

parquet_file = pq.ParquetFile(parquet_path)

i = 0
for batch in parquet_file.iter_batches(batch_size=batch_size):

    float_bathymetry = pc.cast(batch.column("bathymetry"), pa.float64())
    arrays = []
    fields = []
    for column_name in batch.schema.names:
        if column_name == "bathymetry":
            arrays.append(float_bathymetry)
            fields.append(pa.field(column_name, pa.float64()))
        else:
            arrays.append(batch.column(column_name))
            fields.append(batch.schema.field(column_name))
    new_batch = pa.RecordBatch.from_arrays(arrays, pa.schema(fields))

    writer = pq.ParquetWriter(os.path.join(output_dir, f"{i}.parquet"), new_batch.schema)
    writer.write_batch(new_batch)
    print(f"Written batch {i}")
    writer.close()
    i = i + 1
