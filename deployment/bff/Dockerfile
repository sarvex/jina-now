FROM python:3.9

WORKDIR /code

COPY requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# install latest code changes of the now repo without the requirements installed already
RUN pip install git+https://github.com/jina-ai/now@JINA_NOW_COMMIT_SHA

COPY app /code/deployment/bff/app

EXPOSE 8080

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "deployment.bff.app.app:application", "--host", "0.0.0.0", "--port", "8080", "--loop", "uvloop"]
