# Docker image for grasshopper-loader
# To build, run docker build --rm --tag=hmda/<image-name>:<tag-name> .
# To test, run ./docker-test.sh <image-name>:<tag-name> <cli args>
# To run, run ./docker-run <image-name>:<tag-name> <cli args>

FROM geodata/gdal:1.11.2
MAINTAINER Wyatt Pearsall <wyatt.pearsall@cfpb.gov>
USER root

RUN apt-get update && apt-get install -y curl git && \
    curl -sL https://deb.nodesource.com/setup_5.x | sudo bash - && \
    apt-get install -y nodejs &&\
    npm update -g npm &&\
    mkdir -p /usr/src/app

WORKDIR /usr/src/app
COPY . /usr/src/app

RUN useradd notroot && chown -R notroot /usr/src/app && chmod u+rwx /usr/src/app
RUN npm install

USER notroot

CMD /bin/bash

