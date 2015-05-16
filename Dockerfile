# Docker image for grasshopper-loader
# To build, run docker build --rm --tag=hmda/grasshopper-loader .
# A container can be started by running docker run -ti hmda/grasshopper-parser /bin/bash
# Then run the loader from within the container

FROM wpears/gdal:v1
MAINTAINER Wyatt Pearsall <wyatt.pearsall@cfpb.gov>

RUN apt-get update && apt-get install -y \
  nodejs \
  npm

RUN ln -s /usr/bin/nodejs /usr/bin/node

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY . /usr/src/app

RUN npm install

CMD /bin/bash

