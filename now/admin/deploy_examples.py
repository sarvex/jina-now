import os
from argparse import Namespace
from concurrent.futures import ProcessPoolExecutor

from now.cli import cli
from now.constants import Apps, DemoDatasets

os.environ['JCLOUD_LOGLEVEL'] = 'DEBUG'
app = Apps.TEXT_TO_IMAGE

params = [
    (Apps.TEXT_TO_IMAGE, DemoDatasets.BEST_ARTWORKS),
    (Apps.TEXT_TO_IMAGE, DemoDatasets.DEEP_FASHION),
    (Apps.IMAGE_TO_TEXT, DemoDatasets.RAP_LYRICS),
    (Apps.IMAGE_TO_IMAGE, DemoDatasets.TLL),
    (Apps.TEXT_TO_TEXT, DemoDatasets.ROCK_LYRICS),
    (Apps.MUSIC_TO_MUSIC, DemoDatasets.MUSIC_GENRES_MIX),
]


def deploy(app, data):
    kwargs = {
        'now': 'start',
        'app': app,
        'data': data,
        # 'custom_dataset_type': DatasetTypes.S3_BUCKET,
        # 'dataset_path': os.environ.get('S3_IMAGE_TEST_DATA_PATH'),
        # 'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID'),
        # 'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY'),
        # 'aws_region_name': 'eu-west-1',
        'deployment_type': 'remote',
        'proceed': True,
        'secured': False,
    }
    kwargs = Namespace(**kwargs)
    response = cli(args=kwargs)
    return response


if __name__ == '__main__':
    parallel = False
    results = []
    if parallel:
        with ProcessPoolExecutor() as thread_executor:
            futures = []
            for app, data in params:
                f = thread_executor.submit(deploy, app, data)
                futures.append(f)
            for f in futures:
                results.append(f.result())
    else:
        for app, data in params:
            result = deploy(app, data)
            results.append(result)
            print(result)
    print(results)
