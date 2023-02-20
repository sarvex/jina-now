from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing
from math import floor
import hubble
import psutil
import requests
from docarray import DocumentArray, Document, dataclass
import os
from docarray.typing import Image
import glob
import boto3

from hubble.client.endpoints import EndpointsV2
from hubble import Client as HubbleClient


def check_docarray_exists(dataset_name):
    response = requests.post(
        'https://api.hubble.jina.ai/v2/rpc/artifact.getDetail',
        cookies={
            'st': hubble.get_token(),
        },
        json={
            'name': dataset_name,
        },
    )
    return response.json()['code'] == 200


def download_da(dir_name, part_prefix):
    import requests as py_requests

    headers = {}
    auth_token = hubble.get_token()

    if auth_token:
        headers['Authorization'] = f'token {auth_token}'

    url = (
        HubbleClient()._base_url
        + EndpointsV2.download_artifact
        + f'?name=jem-fu/{part_prefix}'
    )
    response = py_requests.get(url, headers=headers)

    if response.ok:
        url = response.json()['data']['download']
    else:
        response.raise_for_status()

    local_filename = dir_name + f'/{part_prefix}.bin'
    # NOTE the stream=True parameter below
    try:
        print(local_filename, 'being downloaded')
        with py_requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    # If you have chunk encoded response uncomment if
                    # and set chunk_size parameter to None.
                    # if chunk:
                    f.write(chunk)
            docs = DocumentArray.load_binary(
                local_filename, protocol='protobuf', compress='gzip'
            )
        print(local_filename, 'being processed')
        docs = process_docs(docs)
        os.remove(local_filename)
        docs.save_binary(local_filename)
        print(local_filename, 'done')
    except:
        print(local_filename, 'downloading failed')
        # remove the failed file if it exists
        try:
            os.remove(local_filename)
            print(local_filename, 'removed')
        except OSError:
            pass


def get_metadata(doc):
    """
    create a proper multimodal document to get how metadata looks
    which will be used to create other documents manually
    """

    @dataclass
    class MockDock:
        image: Image

    multimodal_doc = Document(MockDock(image=doc.uri))
    return multimodal_doc._metadata, multimodal_doc.chunks[0]._metadata


def process_docs(docs):
    """
    Transform regular documents into multimodal format
    """
    root_metadata, chunk_metadata = get_metadata(docs[0])
    processed_docs = DocumentArray()
    for doc in docs:
        new_doc = Document(chunks=Document(uri=doc.uri))
        new_doc._metadata = root_metadata
        new_doc.chunks[0]._metadata = chunk_metadata
        new_doc.chunks[0].modality = 'image'
        processed_docs.append(new_doc)
    return processed_docs


def download_laion400m(dir_name, size=100):
    """
    Download and process specified amount of subsets of laion400m data.
    Processing involves transforming a regular document into a multimodal one.
    We only store uris, otherwise data is too big.
    Not all subsets are available, that's why we check it first.

    :param dir_name: name of the directory where processed subsets will be stored.
    :param size: number of subsets (each subset has 1m examples) to be downloaded.
    """
    print('starting pulling the data')
    num_workers = (
        min(floor(psutil.virtual_memory().total / 1e9 / 2), multiprocessing.cpu_count())
        - 3
    )
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        collected = 0
        futures = []
        for i in range(400):
            if os.path.exists(os.path.join(dir_name, f'laion400m_part_{i}.bin')):
                collected += 1
                continue
            elif not check_docarray_exists(f'jem-fu/laion400m_part_{i}'):
                continue

            if collected == size:
                break

            futures.append(
                executor.submit(download_da, dir_name, f'laion400m_part_{i}')
            )
            collected += 1
        for future in as_completed(futures):
            future.result()


def split(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def aggregate_data(dir_name, size):
    """
    Aggregate downloaded data into one and store it as a binary file.
    :param dir_name: name of the directory where processed subsets will be stored.
    :param size: size of the chunks (in millions) that will be used to split data.
    """
    file_name = os.path.join(dir_name, f'laion_{size}m.bin')
    if os.path.exists(file_name):
        return file_name

    print('starting to aggregate data into one binary file')
    for i, sublist in enumerate(split(glob.glob(os.path.join(dir_name, '*')), size)):
        print('aggregating', sublist)
        docs = DocumentArray()
        for file in sublist:
            docs_subset = DocumentArray.load_binary(file)
            docs.extend(docs_subset)
        print('saving', sublist)
        docs.save_binary(file_name, protocol='protobuf', compress='lz4')
        print('pushing', sublist)
        push_to_s3(file_name, f'{size}ms/laion_{size}m_{i}.bin')
        print('removing', sublist)
        os.remove(file_name)


def push_to_s3(local_file_name, s3_file_name):
    """
    Push aggregated data into the s3 bucket.
    :param local_file_name: name of the file we want to upload
    :param s3_file_name: under this name will be the file uploaded
    """
    print('pushing the file to the s3 bucket')
    session = boto3.session.Session(
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name='eu-west-1',
    )
    bucket = session.resource('s3').Bucket(
        os.environ.get('S3_CUSTOM_MM_DATA_PATH').split('/')[2]
    )
    bucket.upload_file(local_file_name, os.path.join('laion_data/', s3_file_name))


if __name__ == "__main__":
    """
    Pull original laion400m data uploaded by Jie;
    Process each subset to match our multimodal docarray structure;
    Aggregate subsets into chunk(s) of given size;
    Push the aggregated data into the s3 bucket;

    run: python prepare_laion_data.py

    Script is written in a way that in case of an interruption not everything is lost.
    You can continue by running the script again, or remove local 'laion_data' dir and
    run the script again to startover.
    """
    dir_name = 'laion_data'
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    # download 100m data
    download_laion400m(dir_name=dir_name, size=100)
    # aggregate them as 10m chunks and push to s3
    aggregate_data(dir_name=dir_name, size=10)

    # # remove local data files in the end
    # shutil.rmtree(dir_name)
