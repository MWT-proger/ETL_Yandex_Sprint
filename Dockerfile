FROM python:3.9.7-buster
WORKDIR /usr/src/etl

COPY ./requirements.txt .
RUN pip install -r requirements.txt

RUN isort .

COPY . .
