from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from docarray import DocumentArray
from PIL import Image

docs = DocumentArray.pull(f'team-now/best-artworks', show_progress=True)
print(docs.summary())
docs


def read_image(file_path):
    with open(file_path, 'rb') as f:
        image = Image.open(f)
        binary_image = image.tobytes()
    return binary_image


image_paths = [
    '/Users/yadhkhalfallah/Desktop/dogs/f1/243868719.jpeg',
    '/Users/yadhkhalfallah/Desktop/dogs/f1/cute-dog-headshot.jpeg',
]
image_data = [read_image(path) for path in image_paths]

schema = pa.schema([('image', pa.binary()), ('filename', pa.string())])

record_batches = []

batch_size = 1000
num_workers = 8


def process_batch(batch):
    df = pd.DataFrame({'image': batch, 'filename': image_paths[: len(batch)]})
    return pa.RecordBatch.from_pandas(df)


with ThreadPoolExecutor(max_workers=num_workers) as executor:
    futures = []
    for i in range(0, len(image_data), batch_size):
        batch = image_data[i : i + batch_size]
        futures.append(executor.submit(process_batch, batch))
    results = [future.result() for future in as_completed(futures)]


for result in results:
    record_batches.append(result)

table = pa.Table.from_batches(record_batches, schema=schema)

pq.write_table(table, '/Users/yadhkhalfallah/Desktop/Jina/parquet-crud/test.parquet')
