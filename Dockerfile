FROM python:3.7.4-alpine3.10
MAINTAINER Flywheel <support@flywheel.io>

RUN apk add --no-cache bash git

WORKDIR /flywheel/v0
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

ENTRYPOINT ["/flywheel/v0/run.py"]
