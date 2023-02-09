from docarray import DocumentArray, Document, dataclass
import os
from docarray.typing import Image
import glob
import boto3
import shutil


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
    downloaded = 0
    for i in range(400):
        if os.path.exists(os.path.join(dir_name, f'laion_{i}.bin')):
            downloaded += 1
            continue
        if downloaded == size:
            return
        try:
            docs = DocumentArray.pull(f'jem-fu/laion400m_part_{i}', show_progress=True)
            docs = process_docs(docs)
            docs.save_binary(os.path.join(dir_name, f'laion_{i}.bin'))
            downloaded += 1
            print(f'part {i} successfully downloaded')
        except:
            print(f'could not find part {i}')
    print('data has been pulled')


def aggregate_data(dir_name):
    """
    Aggregate downloaded data into one and store it as a binary file.
    """
    file_name = os.path.join(dir_name, 'laion_100m.bin')
    if os.path.exists(file_name):
        return file_name

    print('starting to aggregate data into one binary file')
    docs = DocumentArray()
    for file in glob.glob(os.path.join(dir_name, '*')):
        docs.extend(DocumentArray.load_binary(file))
    docs.save_binary(file_name, protocol='protobuf', compress='lz4')
    return file_name


def push_to_s3(file_name):
    """
    Push aggregated data into the s3 bucket.
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
    bucket.upload_file(file_name, 'laion_data/laion_100m.bin')


if __name__ == "__main__":
    """
    Pull original laion400m data uploaded by Jie;
    Process each subset to match our multimodal docarray structure;
    Aggregate each subset into one;
    Push the aggregated data into the s3 bucket;

    run: python prepare_laion_data.py

    Script is written in a way that in case of an interruption not everything is lost.
    You can continue by running the script again, or remove local 'laion_data' dir and
    run the script again to startover.
    """
    dir_name = 'laion_data'
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    download_laion400m(dir_name=dir_name)
    file_name = aggregate_data(dir_name=dir_name)
    push_to_s3(file_name=file_name)

    # # remove local data files in the end
    # shutil.rmtree(dir_name)
