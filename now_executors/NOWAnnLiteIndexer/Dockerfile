FROM jinaai/jina:3-py38-perf

RUN apt-get update && apt-get install --no-install-recommends -y gcc g++ git \
    && rm -rf /var/lib/apt/lists/*

COPY ./requirements.txt /requirements.txt
RUN pip install -r requirements.txt --no-cache-dir

COPY . /workspace
WORKDIR /workspace

ENTRYPOINT ["jina", "executor", "--uses", "config.yml"]
