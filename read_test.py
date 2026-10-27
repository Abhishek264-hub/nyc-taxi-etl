import glob
import pyarrow.parquet as pq

for file in sorted(glob.glob("yellow_tripdata_2026-*.parquet")):
    pf = pq.ParquetFile(file)
    print(file, pf.metadata.num_rows, "rows")

print(pf.schema_arrow)
