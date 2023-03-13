import pyarrow.parquet as pq

table = pq.read_table(
    '/Users/yadhkhalfallah/Desktop/Jina/parquet-crud/test.parquet',
    use_threads=True,
    memory_map=True,
)

df = table.to_pandas()

print(df.head())
