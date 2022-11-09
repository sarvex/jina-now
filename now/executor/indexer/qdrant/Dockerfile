FROM qdrant/qdrant:v0.7.0
ARG JINA_NOW_COMMIT_SHA  # you could give this a default value as well
ENV JINA_NOW_COMMIT_SHA=JINA_NOW_COMMIT_SHA

RUN apt-get update && apt-get install python3 python3-pip git -y

RUN pip install "docarray[qdrant]"

COPY . /workdir/
WORKDIR /workdir
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install jina==3.11.0

ENTRYPOINT ["jina", "executor", "--uses", "config.yml"]
