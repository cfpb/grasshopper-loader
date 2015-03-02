#!/usr/bin/env node

'use strict';

var program = require('commander');
var checkUsage = require('./lib/checkUsage');
var validateShp = require('./lib/validateShp');
var shp = require('./lib/shp');

program
  .version('1.0.0')
  .option('-s, --shapefile <s>', 'Shapefile')
  .option('-h, --host <h>', 'ElasticSearch host')
  .option('-p ,--port <p>', 'ElasticSearch port', parseInt)
  .parse(process.argv);


if(!checkUsage(program)) return;


validateShp(program.shapefile,function(err, shapefile){
  if (err) return console.log(err);
  shp.read(shapefile, program.host, program.port);
});





