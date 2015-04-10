#!/usr/bin/env node

'use strict';

var fs = require('fs');
var path = require('path');

var program = require('commander');
var lump = require('lump-stream');

var checkUsage = require('./lib/checkUsage');
var esLoader = require('./lib/esLoader');
var getGeoFiles = require('./lib/getGeoFiles');
var resolveTransformer = require('./lib/resolveTransformer');
var requireTransformer = require('./lib/requireTransformer');
var ogrChild = require('./lib/ogrChild');
var splitOGRJSON = require('./lib/splitOGRJSON');
var makeBulkSeparator = require('./lib/makeBulkSeparator');
var verify = require('./lib/verify');

var transformer;


program
  .version('1.0.0')
  .option('-d, --data <data>', 'Point data as a .zip, .shp, .gdb, or directory')
  .option('-t, --transformer <transformer>', 'Data transformer. Defaults to ./transformers/[[file basename]].js')
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost', 'localhost')
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200', Number, 9200)
  .option('--index <index>', 'Elasticsearch index. Defaults to address', 'address')
  .option('--type <type>', 'Elasticsearch type within the provided or default index. Defaults to point', 'point')
  .parse(process.argv);

var usage = checkUsage(program);
console.log(usage.messages.join(''));
if(usage.err) return;


var client = esLoader.connect(program.host, program.port);


getGeoFiles(program.data, processData);


function processData(err, file, cb){
  if(err){
    if(cb) return cb(err);
    throw err;
  }

  console.log("Streaming %s to elasticsearch.", file);


  var transFile = resolveTransformer(program.transformer, file);

  try{
    transformer = requireTransformer(transFile, file);
  }catch(err){
    console.log('\nCouldn\'t find transformer: %s.\nProvide one with the -t option.', transFile);
    if(cb)return cb(err);
    throw err;
  }


  var child = ogrChild(file);
  var loader = esLoader.load(client, program.index, program.type);

  child.stdout 
    .pipe(splitOGRJSON())
    .pipe(transformer(makeBulkSeparator(), '\n'))
    .pipe(lump(Math.pow(2,20)))
    .pipe(loader)
    .on('error',function(err){
      console.log("Error piping data",err); 
    });

    loader.on('finish', function(){
      client.close();
      var count = this.count;

      verify(file, count, function(errObj){
        if(errObj){
          if(cb) return cb(errObj.error);
          throw errObj.error;
        }
        console.log("All %d records loaded.", count);
        if(cb) cb();
      });

    });
}

