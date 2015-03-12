#!/usr/bin/env node

'use strict';

var fs = require('fs');
var path = require('path');

var program = require('commander');
var lump = require('lump-stream');
var unzip = require('unzip');

var checkUsage = require('./lib/checkUsage');
var esLoader = require('./lib/esLoader');
var ogr = require('./lib/ogr');
var splitOGRJSON = require('./lib/splitOGRJSON');
var makeBulkSeparator = require('./lib/makeBulkSeparator');

var index = 'address';
var type = 'point';


program
  .version('1.0.0')
  .option('-s, --shapefile <shp>', 'Shapefile as a zip, .shp, or directory')
//  .option('-r --recursive', 'Recursively locate and operate on shapefiles in a directory')
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost', 'localhost')
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200', Number, 9200)
  .option('-t, --transformer <transformer>', 'Data transformer. Defaults to transformer/default', 'transformer/default')
  .parse(process.argv);

if(!checkUsage(program)) return;

var transformer = require(program.transformer);

esLoader.connect(program.host, program.port, index, type);


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
  ogr(shp)
    .pipe(splitOGRJSON())
    .pipe(transformer(makeBulkSeparator(), '\n'))
    .pipe(lump(Math.pow(2,20)))
    .pipe(esLoader.load())
    .on('error',function(err){
      console.log("Error piping data",err); 
    });
}

