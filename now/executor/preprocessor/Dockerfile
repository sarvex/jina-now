FROM jinaai/jina:3.12.0

RUN apt-get update && apt-get install -y git &&\
    apt-get -y install build-essential libgomp1 libgl1-mesa-glx libglib2.0-0 libsm6 libxext6 libxrender-dev && \
    rm -rf /var/cache/apt/*

# install requirements for the executor
COPY requirements-special.txt .
RUN pip install --compile -r requirements.txt

####### This is only for optimization purpose #######
# install all requirements from the now repo (the file is put here during ci/cd)
# the advantage is that all of the requirements are then cached and only have to be reinstalled if they change
COPY requirements-now.txt .
RUN pip install --compile -r requirements-now.txt

####### cache is invalidated here #######
# at the end the current jina now version has to be installed.
# this always happens, because the JINA_NOW_COMMIT_SHA is always different
RUN pip install git+https://github.com/jina-ai/now@JINA_NOW_COMMIT_SHA

# setup the workspace
COPY . /workdir/
WORKDIR /workdir

ENTRYPOINT ["jina", "executor", "--uses", "config.yml"]