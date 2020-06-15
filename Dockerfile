FROM python:3.8-slim

RUN mkdir -p /usr/src/es

WORKDIR /usr/src/es

COPY requirements.txt .

RUN pip install -r requirements.txt

ADD . .

ENTRYPOINT [ "python3" ]