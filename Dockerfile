# Docker image for grasshopper-loader
# To build, run docker build --rm --tag=hmda/grasshopper-loader .
# A container can be started by running docker run -ti hmda/grasshopper-parser /bin/bash
# Then run the loader from within the container

FROM geodata/gdal:1.11.2
MAINTAINER Wyatt Pearsall <wyatt.pearsall@cfpb.gov>
USER root
RUN apt-get update && apt-get install -y curl
RUN curl -sL https://deb.nodesource.com/setup_0.12 | sudo bash -

RUN apt-get install -y nodejs

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY . /usr/src/app

RUN npm install


RUN useradd tester
RUN chown -R tester /usr/src/app
RUN chmod u+rwx /usr/src/app
USER tester

CMD /bin/bash

