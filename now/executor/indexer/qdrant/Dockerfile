FROM qdrant/qdrant:v0.10.1

RUN apt-get update && apt-get install -y python3 python3-pip git

## install requirements for the executor
COPY requirements.txt .
RUN pip install --compile -r requirements.txt

# install latest code changes of the now repo without the requirements installed already
RUN pip install git+https://github.com/jina-ai/now@JINA_NOW_COMMIT_SHA --no-dependencies

# setup the workspace
COPY . /workdir/
WORKDIR /workdir

ENTRYPOINT ["jina", "executor", "--uses", "config.yml"]
