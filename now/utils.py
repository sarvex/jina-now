from __future__ import annotations, print_function, unicode_literals

import base64
import json
import os
import pkgutil
import shutil
import signal
import stat
import sys
import tempfile
from collections import namedtuple
from collections.abc import MutableMapping
from concurrent.futures import ThreadPoolExecutor
from os.path import expanduser as user
from typing import Dict, List, Optional, Union

import boto3
import hubble
import numpy as np
import yaml
from docarray import Document, DocumentArray
from PIL import Image, ImageDraw, ImageFont
from pyfiglet import Figlet
from rich.console import Console

from now.thirdparty.PyInquirer.prompt import prompt

colors = [
    "navy",
    "turquoise",
    "darkorange",
    "cornflowerblue",
    "teal",
    'maroon',
    'purple',
    'green',
    'lime',
    'navy',
    'teal',
]


def get_device():
    # return only cpu as we want all our processed to run on cpu
    return "cpu"  # "cuda" if torch.cuda.is_available() else "cpu"


def save_before_after_image(path):
    figs = [
        ['pretrained.png', 'finetuned.png'],
        # ['pretrained_pr.png', 'finetuned_pr.png'],
        ['pretrained_m.png', 'finetuned_m.png'],
    ]
    [im1, im2, im3, im4] = [Image.open(img) for imgs in figs for img in imgs]

    big_w = im1.width if im1.width > im3.width else im3.width

    im1 = im1.resize(
        (big_w, int(im1.height * big_w / im1.width)), resample=Image.BICUBIC
    )
    im2 = im2.resize(
        (big_w, int(im2.height * big_w / im2.width)), resample=Image.BICUBIC
    )
    im3 = im3.resize(
        (big_w, int(im3.height * big_w / im3.width)), resample=Image.BICUBIC
    )
    im4 = im4.resize(
        (big_w, int(im4.height * big_w / im4.width)), resample=Image.BICUBIC
    )
    # im5 = im5.resize(
    #     (big_w, int(im5.height * big_w / im5.width)), resample=Image.BICUBIC
    # )
    # im6 = im6.resize(
    #     (big_w, int(im6.height * big_w / im6.width)), resample=Image.BICUBIC
    # )

    width = max(im1.width + im2.width, im3.width + im4.width) + 200
    height = (
        max(
            im1.height + im3.height,
            im2.height + im4.height,
        )
        + 300
    )

    dst = Image.new('RGB', (width, height), color=(255, 255, 255))

    dst.paste(im1, (10, 150))
    dst.paste(im2, (im1.width + 150, 150))
    dst.paste(im3, (10, im1.height + 250))
    dst.paste(im4, (im1.width + 120, im1.height + 250))
    draw = ImageDraw.Draw(dst)
    font = ImageFont.truetype(
        'now/fonts/arial.ttf', 50
    )  # this font does not work in docker currently
    # font = ImageFont.load_default()
    draw.line(
        (
            im1.width + 80,
            0,
            im1.width + 80,
            im1.height + im2.height + im3.height + 250,
        ),
        width=10,
        fill='black',
    )  # vertical line
    draw.line(
        (0, im1.height + 200, im1.width + im2.width + 250, im1.height + 200),
        width=10,
        fill='black',
    )  # horizontal line
    draw.line(
        (
            0,
            im1.height + im2.height,
            im1.width + im2.width + 250,
            im1.height + im2.height,
        ),
        width=10,
        fill='black',
    )  # horizontal line
    draw.text(((im1.width - 150) // 2, 0), "Pre-trained Results", 0, font=font)
    draw.text(
        (im1.width + 50 + (im2.width // 2), 0),
        "Finetuned Results",
        0,
        font=font,
    )
    font = ImageFont.truetype('now/fonts/arial.ttf', 30)
    draw.text((20, 100), "Query", 0, font=font)
    draw.text((im1.width + 150, 100), "Query", 0, font=font)
    draw.text((im1.width // 2, 100), "Top-k", 0, font=font)
    draw.text((im1.width + 100 + (im2.width // 2), 100), "Top-k", 0, font=font)
    dst.show(title=path.split('.')[0])
    dst.save(path)
    for imgs in figs:
        for img in imgs:
            if os.path.isfile(img):
                os.remove(img)


def visual_result(
    data,
    querys,
    output: str = None,
    canvas_size: int = 1500,
    channel_axis: int = -1,
    img_per_row=11,
    label='finetuner_label',
    unique=False,
):
    img_size = int(canvas_size / img_per_row)
    img_h, img_w = img_size, img_size

    if data == 'geolocation-geoguessr':
        img_h, img_w = img_size, 2 * img_size

    sprite_img = np.zeros([img_h * len(querys), img_w * img_per_row, 3], dtype='uint8')

    for img_id, q in enumerate(querys):
        _d = Document(q, copy=True)
        if _d.content_type != 'tensor':
            _d.convert_blob_to_image_tensor()
            channel_axis = -1

        _d.set_image_tensor_channel_axis(channel_axis, -1).set_image_tensor_shape(
            shape=(img_h, img_w)
        )

        row_id = img_id
        col_id = 0
        sprite_img[
            (row_id * img_h) : ((row_id + 1) * img_h),
            (col_id * img_w) : ((col_id + 1) * img_w),
        ] = _d.tensor

        sprite_img[
            (row_id * img_h) : ((row_id + 1) * img_h),
            ((col_id + 1) * img_w) : ((col_id + 2) * img_w),
        ] = np.ones_like(_d.tensor.shape)

        for j, d in enumerate(q.matches, start=1):
            _d = Document(d, copy=True)
            if _d.content_type != 'tensor':
                _d.convert_blob_to_image_tensor()
                channel_axis = -1

            _d.set_image_tensor_channel_axis(channel_axis, -1).set_image_tensor_shape(
                shape=(img_h - 10, img_w - 10)
            )

            match_img = np.ones([img_h, img_w, 3], dtype='uint8')
            match_img[5:-5, 5:-5] = _d.tensor  # center the match results image
            if not unique:
                # apply green if it is same class else red
                if q.tags[label] == _d.tags[label]:
                    match_img[0:5, ...] = (0, 255, 0)
                    match_img[-5:-1, ...] = (0, 255, 0)
                    match_img[:, 0:5, ...] = (0, 255, 0)
                    match_img[:, -5:-1, ...] = (0, 255, 0)
                else:
                    match_img[0:5, ...] = (255, 0, 0)
                    match_img[-5:-1, ...] = (255, 0, 0)
                    match_img[:, 0:5, ...] = (255, 0, 0)
                    match_img[:, -5:-1, ...] = (255, 0, 0)
            else:
                match_img[0:5, ...] = (0, 0, 0)
                match_img[-5:-1, ...] = (0, 0, 0)
                match_img[:, 0:5, ...] = (0, 0, 0)
                match_img[:, -5:-1, ...] = (0, 0, 0)

            # paste it on the main canvas
            col_id = j + 1
            sprite_img[
                (row_id * img_h) : ((row_id + 1) * img_h),
                (col_id * img_w) : ((col_id + 1) * img_w),
            ] = match_img

    from PIL import Image

    img = Image.fromarray(sprite_img)
    if output:
        with open(output, 'wb') as fp:
            img.save(fp)


def plot_metrics(metrics_dict, title):
    dst = Image.new(
        'RGB',
        (500, 300),
        color=(255, 255, 255),
    )
    draw = ImageDraw.Draw(dst)
    font = ImageFont.truetype(
        'now/fonts/arial.ttf', 20
    )  # this font does not work in docker currently
    # font = ImageFont.load_default()
    for idx, (key, val) in enumerate(metrics_dict.items()):
        draw.text((50, idx * 30), f'{key}', 0, font=font)
        draw.text((250, idx * 30), f":  {val:.3f}", 0, font=font)

    dst.save(title)


def custom_spinner():
    SPINNERS_DATA = pkgutil.get_data(__name__, "custom-spinners.json").decode("utf-8")

    def _hook(dct):
        return namedtuple("Spinner", dct.keys())(*dct.values())

    return json.loads(SPINNERS_DATA, object_hook=_hook)


def download_file(path, r_raw):
    with path.open("wb") as f:
        shutil.copyfileobj(r_raw, f)


def download(url, filename):
    import functools
    import pathlib

    import requests
    from tqdm.auto import tqdm

    r = requests.get(url, stream=True, allow_redirects=True)
    if r.status_code != 200:
        r.raise_for_status()  # Will only raise for 4xx codes, so...
        raise RuntimeError(f"Request to {url} returned status code {r.status_code}")
    file_size = int(r.headers.get('Content-Length', 0))

    path = pathlib.Path(filename).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    desc = "(Unknown total file size)" if file_size == 0 else ""
    r.raw.read = functools.partial(
        r.raw.read, decode_content=True
    )  # Decompress if needed

    if any(map(lambda x: x in os.environ, ['NOW_CI_RUN', 'NOW_EXAMPLES'])):
        download_file(path, r.raw)
    else:
        with tqdm.wrapattr(r.raw, "read", total=file_size, desc=desc) as r_raw:
            download_file(path, r_raw)

    return path


def get_rich_console():
    """
    Function to get jina rich default console.
    :return: rich console
    """
    return Console(
        force_terminal=True, force_interactive=True
    )  # It forces render in any terminal, especially in PyCharm


def copytree(src, dst, symlinks=False, ignore=None):
    if not os.path.exists(dst):
        os.makedirs(dst)
        shutil.copystat(src, dst)
    lst = os.listdir(src)
    if ignore:
        excl = ignore(src, lst)
        lst = [x for x in lst if x not in excl]
    for item in lst:
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if symlinks and os.path.islink(s):
            if os.path.lexists(d):
                os.remove(d)
            os.symlink(os.readlink(s), d)
            try:
                st = os.lstat(s)
                mode = stat.S_IMODE(st.st_mode)
                os.lchmod(d, mode)
            except Exception:
                pass  # lchmod not available
        elif os.path.isdir(s):
            copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


def my_handler(signum, frame, spinner):
    with spinner.hidden():
        sys.stdout.write("Program terminated!\n")
    spinner.stop()
    exit(0)


def flow_definition(dirpath) -> Dict:
    with open(dirpath) as f:
        return yaml.safe_load(f.read())


class BetterEnum:
    def __iter__(self):
        return [getattr(self, x) for x in dir(self) if ('__' not in x)].__iter__()


def to_camel_case(text):
    """Convert text to camel case in great coding style"""
    s = text.replace("_", " ")
    s = s.split()
    return ''.join(i.capitalize() for i in s)


sigmap = {signal.SIGINT: my_handler, signal.SIGTERM: my_handler}


def write_env_file(env_file, config):
    config_string = '\n'.join([f'{key}={value}' for key, value in config.items()])
    with open(env_file, 'w+') as fp:
        fp.write(config_string)


@hubble.login_required
def jina_auth_login():
    pass


def get_info_hubble(user_input):
    with open(user('~/.jina/config.json')) as fp:
        config_val = json.load(fp)
        user_token = config_val['auth_token']
    client = hubble.Client(token=user_token, max_retries=None, jsonify=True)
    response = client.get_user_info()
    user_input.admin_emails = (
        [response['data']['email']] if 'email' in response['data'] else []
    )
    if not user_input.admin_emails:
        print(
            'Your hubble account is not verified. Please verify your account to deploy your flow as admin.'
        )
    user_input.jwt = {'token': user_token}
    return response['data'], user_token


def print_headline():
    f = Figlet(font='slant')
    print('Welcome to:')
    print(f.renderText('Jina NOW'))
    print('Get your search case up and running - end to end.\n')
    print(
        'You can choose between image and text search. \nJina NOW trains a model, pushes it to Jina Hub '
        'and deploys a Flow and a playground app in the cloud or locally. \nCheck out one of our demos or bring '
        'your own data.\n'
    )
    print('If you want learn more about our framework please visit docs.jina.ai')
    print(
        '💡 Make sure you give enough memory to your Docker daemon. '
        '5GB - 8GB should be okay.'
    )
    print()


def maybe_prompt_user(questions, attribute, **kwargs):
    """
    Checks the `kwargs` for the `attribute` name. If present, the value is returned directly.
    If not, the user is prompted via the cmd-line using the `questions` argument.

    :param questions: A dictionary that is passed to `PyInquirer.prompt`
        See docs: https://github.com/CITGuru/PyInquirer#documentation
    :param attribute: Name of the value to get. Make sure this matches the name in `kwargs`

    :return: A single value of either from `kwargs` or the user cli input.
    """
    if kwargs and kwargs.get(attribute) is not None:
        return kwargs[attribute]
    else:
        answer = prompt(questions)
        return answer[attribute]


def _prompt_value(
    name: str,
    prompt_message: str,
    prompt_type: str = 'input',
    choices: Optional[List[Union[Dict, str]]] = None,
    **kwargs: Dict,
):
    qs = {'name': name, 'type': prompt_type, 'message': prompt_message}

    if choices is not None:
        qs['choices'] = choices
        qs['type'] = 'list'
    return maybe_prompt_user(qs, name, **kwargs)


def _maybe_download_from_s3(
    docs: DocumentArray, tmpdir: tempfile.TemporaryDirectory, user_input, max_workers
):
    """Downloads file to local temporary dictionary, saves S3 URI to `tags['uri']` and modifies `uri` attribute of
    document to local path in-place.

    :param doc: document containing URI pointing to the location on S3 bucket
    :param tmpdir: temporary directory in which files will be saved
    :param user_input: User iput which contain aws credentials
    :param max_workers: number of threads to create in the threadpool executor to make execution faster
    """

    def download(bucket, uri):
        path_s3 = '/'.join(uri.split('/')[3:])
        path_local = os.path.join(
            str(tmpdir),
            base64.b64encode(bytes(path_s3, "utf-8")).decode("utf-8"),
        )
        bucket.download_file(
            path_s3,
            path_local,
        )
        return path_local

    def convert_fn(d: Document) -> Document:
        """Downloads files and tags from S3 bucket and updates the content uri and the tags uri to the local path"""
        d.tags['uri'] = d.uri
        session = boto3.session.Session(
            aws_access_key_id=user_input.aws_access_key_id,
            aws_secret_access_key=user_input.aws_secret_access_key,
            region_name=user_input.aws_region_name,
        )
        bucket = session.resource('s3').Bucket(d.uri.split('/')[2])
        d.uri = download(bucket=bucket, uri=d.uri)
        if 'tag_uri' in d.tags:
            d.tags['tag_uri'] = download(bucket, d.tags['tag_uri'])
            with open(d.tags['tag_uri'], 'r') as fp:
                tags = json.load(fp)
                tags = flatten_dict(tags)
                d.tags.update(tags)
            del d.tags['tag_uri']
        return d

    docs_to_download = []
    for d in docs:
        if d.uri.startswith('s3://'):
            docs_to_download.append(d)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for d in docs_to_download:
            f = executor.submit(convert_fn, d)
            futures.append(f)
        for f in futures:
            f.result()


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def _get_context_names(contexts, active_context=None):
    names = [c for c in contexts] if contexts is not None else []
    if active_context is not None:
        names.remove(active_context)
        names = [active_context] + names
    return names
