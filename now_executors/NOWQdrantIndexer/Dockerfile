FROM qdrant/qdrant:v0.7.0

RUN apt-get update && apt-get install python3 python3-pip -y

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . /workdir/
WORKDIR /workdir

ENTRYPOINT ["jina", "executor", "--uses", "config.yml"]
