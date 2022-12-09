ARG JINA_VERSION=3.12.0

FROM jinaai/jina:${JINA_VERSION}

RUN apt-get update && apt-get install -y git &&\
    apt-get -y install build-essential libgomp1 libgl1-mesa-glx libglib2.0-0 libsm6 libxext6 libxrender-dev && \
    rm -rf /var/cache/apt/*

# install special requirements for this executor
COPY requirements-special.txt .
RUN pip install --compile -r requirements-special.txt

# install all requirements from the now repo (the file is put here during ci/cd)
# the advantage is that all of the requirements are then cached and only have to be reinstalled if they change
COPY requirements-now.txt .
RUN pip install --compile -r requirements-now.txt

# setup the workspace
COPY . /workdir/
WORKDIR /workdir

# install requirements before copying the workspace
COPY requirements.txt .
RUN pip install --compile -r requirements.txt

ENTRYPOINT ["jina", "executor", "--uses", "config.yml"]