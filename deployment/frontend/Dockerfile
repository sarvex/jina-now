FROM jinaai/jina:3.2.8-py37-standard

RUN apt-get update
RUN apt-get -y install

COPY requirements.txt /requirements.txt
RUN pip3 install -r requirements.txt
COPY . /

EXPOSE 8051

ENTRYPOINT ["streamlit", "run", "frontend.py", "--server.port", "8501", "<node_url>", "31080", "<data>"]
# when using below change FROM above as well
#ENTRYPOINT ["streamlit", "run", "frontend.py", "--server.port", "80", "0.0.0.0", "8080", "<data>"]



# might need to add nginx