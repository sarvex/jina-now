import os
import shutil


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
