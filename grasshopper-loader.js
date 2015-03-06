#!/usr/bin/env node

'use strict';

var fs = require('fs');
var path = require('path');

var program = require('commander');
var unzip = require('unzip');

var checkUsage = require('./lib/checkUsage');
var esLoader = require('./lib/esLoader');
var ogr = require('./lib/ogr');
var splitOGRJSON = require('./lib/splitOGRJSON');
var transformer;


program
  .version('1.0.0')
  .option('-s, --shapefile <s>', 'Shapefile')
  .option('-h, --host <h>', 'ElasticSearch host')
  .option('-p, --port <p>', 'ElasticSearch port', parseInt)
  .option('-t, --transformer <t>', 'Data transformer')
  .parse(process.argv);


if(program.transformer){
  transformer = require('./transformers/' + program.transformer);
}else{
  transformer = require('./transformers/default');
}


if(!checkUsage(program)) return;


esLoader.connect(program.host, program.port);


var shapefile = program.shapefile;
var ext = path.extname(shapefile);
var basename = path.basename(shapefile, ext);
var dirname = path.dirname(shapefile);


//fast dir check... requires passing ext with shapefile
if(!ext){
  dirname = shapefile;
}


if(ext.toLowerCase() === '.zip'){
  dirname = path.join(dirname, basename);

  fs.mkdir(dirname, function(err){
    if(err) throw err;
    var unzipped = unzip.Extract({path: dirname});
    unzipped.on('close', processShapefile);

    fs.createReadStream(shapefile).pipe(unzipped)
  });
}else{
  processShapefile(); 
}


function processShapefile(){
  var shp = path.join(dirname, basename + '.shp');
  console.log("Streaming %s to elasticsearch.",shp);
  ogr(shp).pipe(splitOGRJSON()).pipe(transformer()).pipe(esLoader.load()).on('error',function(err){
    console.log("Error loading data",err); 
  });
}

