FROM jinaai/jina:3.13.2-dev34-py38-standard


RUN apt-get update && apt-get install --no-install-recommends -y git pip nginx && rm -rf /var/lib/apt/lists/*

## install requirements for the executor
COPY requirements.txt .
RUN pip install --compile -r requirements.txt

# fix a working jina commit (from feb 2nd 2023); after 3.13.3 is released, we can bump version
RUN pip install -U git+https://github.com/jina-ai/jina@57670feedbbd4940938875c884b6ef32e1748146

# install latest code changes of the now repo without the requirements installed already
RUN pip install git+https://github.com/jina-ai/now@JINA_NOW_COMMIT_SHA --no-dependencies

# setup the workspace
COPY . /workdir/
WORKDIR /workdir

# run nginx.conf
COPY nginx.conf nginx.conf

ENTRYPOINT ["jina", "gateway", "--uses", "config.yml"]
