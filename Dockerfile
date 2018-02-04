FROM conda/miniconda3:latest
MAINTAINER Kaiqiang Wang

ADD . /app
WORKDIR /app

RUN pip install -r /app/requirements.txt

ENTRYPOINT ["/app/run.sh"]
