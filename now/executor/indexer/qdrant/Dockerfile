FROM qdrant/qdrant:v0.7.0

RUN apt-get update && apt-get install -y python3 python3-pip git

RUN pip install "docarray[qdrant]"

COPY . /workdir/
WORKDIR /workdir
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["jina", "executor", "--uses", "config.yml"]
