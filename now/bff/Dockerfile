FROM python:3.9
#FROM jinaai/jina:3.2.8-py37-standard

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app

EXPOSE 8080

CMD ["uvicorn", "app.app:application", "--host", "0.0.0.0", "--port", "8080", "--loop", "uvloop"]
