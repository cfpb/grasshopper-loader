#!/usr/bin/env node

'use strict';

var fs = require('fs');
var path = require('path');

var program = require('commander');
var lump = require('lump-stream');
var unzip = require('unzip');

var checkUsage = require('./lib/checkUsage');
var esLoader = require('./lib/esLoader');
var ogrChild = require('./lib/ogrChild');
var splitOGRJSON = require('./lib/splitOGRJSON');
var makeBulkSeparator = require('./lib/makeBulkSeparator');

var index = 'address';
var type = 'point';


program
  .version('1.0.0')
  .option('-d, --data <data>', 'Point data as a .zip, .shp, .gdb, or directory')
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost', 'localhost')
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200', Number, 9200)
  .option('-t, --transformer <transformer>', 'Data transformer. Defaults to ./transformers/default', './transformers/default')
  .parse(process.argv);

var usage = checkUsage(program);
console.log(usage.messages.join(''));
if(usage.err) return;

var transformer = require(path.resolve(program.transformer));

esLoader.connect(program.host, program.port, index, type);


var geodata = program.data;
var ext = path.extname(geodata);
var basename = path.basename(geodata, ext);
var dirname = path.dirname(geodata);


//fast dir check... requires passing ext with data
if(!ext){
  dirname = geodata;
}


if(ext.toLowerCase() === '.zip'){
  dirname = path.join(dirname, basename);

  fs.mkdir(dirname, function(err){
    if(err) throw err;
    var unzipped = unzip.Extract({path: dirname});
    unzipped.on('close', processData);

    fs.createReadStream(geodata).pipe(unzipped)
  });
}else{
  processData(); 
}


function processData(err, file, cb){
  if(err) throw err;
  var datafile = path.join(dirname, basename + ext);
  console.log("Streaming %s to elasticsearch.",datafile);
  var child = ogrChild(datafile);

  child.stdout 
    .pipe(splitOGRJSON())
    .pipe(transformer(makeBulkSeparator(), '\n'))
    .pipe(lump(Math.pow(2,20)))
    .pipe(esLoader.load())
    .on('error',function(err){
      console.log("Error piping data",err); 
    })
    .on('end', function(){
      if(cb) cb();
    });
}

