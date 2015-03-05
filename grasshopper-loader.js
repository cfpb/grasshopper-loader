#!/usr/bin/env node

'use strict';

var fs = require('fs');
var path = require('path');

var program = require('commander');
var unzip = require('unzip');

var checkUsage = require('./lib/checkUsage');
var project = require('./lib/project');
//var esLoader = require('./lib/esLoader');

program
  .version('1.0.0')
  .option('-s, --shapefile <s>', 'Shapefile')
  .option('-h, --host <h>', 'ElasticSearch host')
  .option('-p ,--port <p>', 'ElasticSearch port', parseInt)
  .parse(process.argv);


if(!checkUsage(program)) return;

var shapefile = program.shapefile;
var ext = path.extname(shapefile);
var basename = path.basename(shapefile, ext);
var dirname = path.dirname(shapefile);

//fast dir check... requires ext passing
if(!ext){
  dirname = shapefile;
}


if(ext.toLowerCase() === '.zip'){
  dirname = path.join(dirname, basename);

  fs.mkdir(dirname, function(err){
    if(err) throw err;
    fs.createReadStream(shapefile).pipe(unzip.Extract({path: dirname}))
      .on('close', processShapefile);
  });
}else{
  processShapefile(); 
}


function processShapefile(){
  var shp = path.join(dirname, basename + '.shp');
  var prj = path.join(dirname, basename + '.prj');

  fs.readFile(prj, function(err, data){
    if (err) throw err;
    project(shp, data.toString());
  });
}
//  esLoader.read(shapefile, program.host, program.port);





