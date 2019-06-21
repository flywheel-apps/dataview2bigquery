FROM python:3-alpine
MAINTAINER Flywheel <support@flywheel.io>

RUN apk add --no-cache bash git \
    && rm -rf /var/cache/apk/*

WORKDIR /flywheel/v0
COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

ENTRYPOINT ["/flywheel/v0/run.py"]