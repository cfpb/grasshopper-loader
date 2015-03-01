#!/usr/bin/env node

'use strict';

var program = require('commander');
var validateShp = require('./lib/validateShp');
var shp = require('./lib/shp');
var errorText = ''

program
  .version('1.0.0')
  .option('-s, --shapefile <s>', 'Shapefile')
  .option('-h, --host <h>', 'ElasticSearch host')
  .option('-p ,--port <p>', 'ElasticSearch port', parseInt)
  .parse(process.argv);


if(!program.shapefile){
  errorText += 'Must provide a shapefile.\n';
}

if(!program.host){
  errorText += 'Must provide an elasticsearch host.\n';
}

if(!program.port || program.port < -1 || program.port > 65536){
  errorText += 'Must provide a port number between 0 and 65535.\n';
}


if (errorText) return usage(errorText);


validateShp(program.shapefile,function(err, shapefile){
  if (err) return console.log(err);
  shp.read(shapefile, program.host, program.port);
});




function usage(error) {
  console.log(error);
  console.log('usage: ./shp2es -s <shapefile> -h <ElasticSearch host> -p <ElasticSearch port>');
}
