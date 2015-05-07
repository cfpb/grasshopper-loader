#!/usr/bin/env node

'use strict';

var fs = require('fs');
var path = require('path');

var program = require('commander');
var isUrl = require('is-url');
var lump = require('lump-stream');

var checkUsage = require('./lib/checkUsage');
var esLoader = require('./lib/esLoader');
var getS3Files = require('./lib/getS3Files');
var getGeoUrl = require('./lib/getGeoUrl');
var getGeoFiles = require('./lib/getGeoFiles');
var resolveTransformer = require('./lib/resolveTransformer');
var requireTransformer = require('./lib/requireTransformer');
var ogrChild = require('./lib/ogrChild');
var splitOGRJSON = require('./lib/splitOGRJSON');
var makeBulkSeparator = require('./lib/makeBulkSeparator');
var verify = require('./lib/verify');
var Counter = require('./lib/counter');

//Favor source GDAL installations for ogr transformations
process.env.PATH = '/usr/local/bin:' + process.env.PATH

program
  .version('0.0.1')
  .option('-b, --bucket <bucket>', 'An S3 bucket where data resides. If no -d option is passed, will attempt to load all data in the bucket.')
  .option('-d, --data <data>', 'Point data as a .zip, .shp, .gdb, or directory. Provide a local or an S3 key. Zip an GeoJson data can also be accessed via url. Required if no bucket is passed.')
  .option('-t, --transformer <transformer>', 'Data transformer. Defaults to ./transformers/[[file basename]].js')
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost', 'localhost')
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200', Number, 9200)
  .option('--index <index>', 'Elasticsearch index. Defaults to address', 'address')
  .option('--type <type>', 'Elasticsearch type within the provided or default index. Defaults to point', 'point')
  .option('--profile <profile>', 'The aws credentials profile in ~/.aws/credentials. Will also respect AWS keys as environment variables.', 'default')
  .parse(process.argv);

var usage = checkUsage(program);
console.log(usage.messages.join(''));
if(usage.err) return;


var client = esLoader.connect(program.host, program.port);
var counter = new Counter();

if(program.bucket) getS3Files(program, counter, processData)
else if(isUrl(program.data)) getGeoUrl(program.data, counter, processData);
else getGeoFiles(program.data, counter, processData)


function processData(err, fileName, stream, cb){
 
  if(typeof stream === 'function'){
    cb = stream;
    stream = null;
  }

  if(err){
    if(cb) return cb(err);
    throw err;
  }
  
  var transformer;
  try{
    transformer = getTransformer(fileName, cb)
  }catch(err){
    console.log("transformer error", err, cb);
    if(cb) return cb(err);
    throw err;
  }
  
  console.log("Streaming %s to elasticsearch.", fileName);

  return pipeline(fileName, stream, transformer, cb);
}


function getTransformer(fileName, cb){
  var transFile = resolveTransformer(program.transformer, fileName);
  return requireTransformer(transFile, fileName);
}


function pipeline(fileName, stream, transformer, cb){
  var child = ogrChild(fileName, stream);
  var loader = esLoader.load(client, program.index, program.type);

  var verifyResults = verify(fileName, stream);

  child.stdout 
    .pipe(splitOGRJSON())
    .pipe(transformer(fileName, makeBulkSeparator(), '\n'))
    .pipe(lump(Math.pow(2,20)))
    .pipe(loader)

    loader.on('finish', function(){
      console.log('Finished streaming %s', fileName);

      if(counter.decr() === 0){
        client.close();
      }

      var count = this.count;

      verifyResults(count, function(errObj){
        if(errObj){
          if(cb) return cb(errObj.error);
          throw errObj.error;
        }
        console.log("All %d records from %s loaded.", count, fileName);
        if(cb) cb();
      });

    });
}

