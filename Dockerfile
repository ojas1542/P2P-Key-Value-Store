# syntax=docker/dockerfile:1

FROM python:3.11-slim

RUN apt-get update && apt-get install -y iptables iputils-ping

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY kvs kvs

CMD [ "python3", "-m", "kvs" ]
