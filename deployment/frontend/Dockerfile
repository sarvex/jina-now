FROM python:3.9

RUN apt-get update
RUN apt-get -y install

COPY requirements.txt /requirements.txt
RUN pip3 install -r requirements.txt
COPY . /

EXPOSE 8051

ENTRYPOINT ["streamlit", "run", "frontend.py", "--server.port", "8501", "<node_url>", "31080", "<output_modality>", "<data>"]
#ENTRYPOINT ["streamlit", "run", "frontend.py", "--server.port", "8501", "localhost", "31080", "image", "best-artworks"]
