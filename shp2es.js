#!/usr/bin/env node

'use strict';

var program = require('commander');
var shp = require('./lib/shp');
var errorText = ''

program
  .version('1.0.0')
	.option('-s, --shapefile <s>', 'Shapefile')
	.option('-h, --host <h>', 'ElasticSearch host')
	.option('-p ,--port <p>', 'ElasticSearch port', parseInt)
  .parse(process.argv);


if(!program.shapefile || !program.shapefile.match(/\.shp$/i)){
  errorText += 'Invalid shapefile.\n';
}

if(!program.host){
  errorText += 'Must provide an elasticsearch host.\n';
}

if(!program.port || program.port < -1 || program.port > 65536){
  errorText += 'Must provide a port number between 0 and 65535.\n';
}


if (errorText) return usage(errorText);

return shp.read(program.shapefile, program.host, program.port);


function usage(error) {
  console.log(error);
  console.log('usage: ./shp2es -s <shapefile> -h <ElasticSearch host> -p <ElasticSearch port>');
}
