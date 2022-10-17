FROM docker.elastic.co/elasticsearch/elasticsearch:8.4.2
USER root
RUN apt-get update && apt-get install --no-install-recommends -y gcc g++ git python3 python3-pip


COPY . /workspace
WORKDIR /workspace
RUN pip install -r requirements.txt --no-cache-dir


ENTRYPOINT ["jina", "executor", "--uses", "config.yml"]
