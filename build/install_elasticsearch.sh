#!/usr/bin/env bash
sudo apt-get -y install openjdk-6-jre
wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-0.90.7.tar.gz
tar -xf elasticsearch-0.90.7.tar.gz
sudo mkdir /etc/elasticsearch
echo network.bind_host: localhost | sudo tee -a /etc/elasticsearch/elasticsearch.yml
echo script.disable_dynamic: true | sudo tee -a /etc/elasticsearch/elasticsearch.yml


