#!/usr/bin/env node

'use strict';

var fs = require('fs');
var path = require('path');

var program = require('commander');
var isUrl = require('isUrl');
var lump = require('lump-stream');

var checkUsage = require('./lib/checkUsage');
var esLoader = require('./lib/esLoader');
var getS3Files = require('./lib/getS3Files');
var getGeoFiles = require('./lib/getGeoFiles');
var resolveTransformer = require('./lib/resolveTransformer');
var requireTransformer = require('./lib/requireTransformer');
var ogrChild = require('./lib/ogrChild');
var splitOGRJSON = require('./lib/splitOGRJSON');
var makeBulkSeparator = require('./lib/makeBulkSeparator');
var verify = require('./lib/verify');


program
  .version('1.0.0')
  .option('-b, --bucket <bucket>', 'An S3 bucket where data resides. If no -d option is passed, will attempt to load all data in the bucket.')
  .option('-d, --data <data>', 'Point data as a .zip, .shp, .gdb, or directory. Provide a local or remote path.')
  .option('-t, --transformer <transformer>', 'Data transformer. Defaults to ./transformers/[[file basename]].js')
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost', 'localhost')
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200', Number, 9200)
  .option('--aws-profile <profile>', 'The aws credentials profile in ~/.aws/credentials, if not default')
  .option('--index <index>', 'Elasticsearch index. Defaults to address', 'address')
  .option('--type <type>', 'Elasticsearch type within the provided or default index. Defaults to point', 'point')
  .parse(process.argv);

var usage = checkUsage(program);
console.log(usage.messages.join(''));
if(usage.err) return;


var client = esLoader.connect(program.host, program.port);


if(program.bucket) getS3Files(program, processStream)
else if (isUrl(program.data)) getGeoFiles(program.data, processStream)
else getGeoFiles(program.data, processLocal)





function processLocal(err, file, cb){
  if(err){
    if(cb) return cb(err);
    throw err;
  }

  var transformer = getTransformer(file, cb);

  console.log("Streaming %s to elasticsearch.", file);

  
  
}

function processStream(err, file, stream, cb){

}


function getTransformer(file, cb){
  var transFile = resolveTransformer(program.transformer, file);

  try{
    transformer = requireTransformer(transFile, file);
  }catch(err){
    console.log('\nCouldn\'t find transformer: %s.\nProvide one with the -t option.', transFile);
    if(cb)return cb(err);
    throw err;
  }
}

function pipeline(file, transformer){
  var child = ogrChild(file);
  var loader = esLoader.load(client, program.index, program.type);

  child.stdout 
    .pipe(splitOGRJSON())
    .pipe(transformer(makeBulkSeparator(), '\n'))
    .pipe(lump(Math.pow(2,20)))
    .pipe(loader)

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

