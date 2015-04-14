#!/usr/bin/env bash
sudo apt-get update -q
sudo apt-get -y install python-software-properties 
sudo apt-add-repository ppa:chris-lea/node.js -y
sudo apt-get update -q
sudo apt-get -y install nodejs g++ build-essential git curl

curl http://download.osgeo.org/gdal/1.11.2/gdal-1.11.2.tar.gz | tar xfz 
cd gdal-1.11.2
./configure && make && sudo make install
sudo ldconfig
cd ..

curl http://download.osgeo.org/proj/proj-4.9.1.tar.gz | tar xfz 
cd proj-4.9.1
./configure && make && sudo make install
sudo ldconfig
cd ..

