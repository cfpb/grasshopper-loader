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

var esVar = process.env.ELASTICSEARCH_PORT;
var esHost;
var esPort;

if(esVar){
  esVar = esVar.split('//')[1].split(':');
  esHost = esVar[0];
  esPort = +esVar[1];
}

program
  .version('0.0.1')
  .option('-b, --bucket <bucket>', 'An S3 bucket where data resides. If no -d option is passed, will attempt to load all data in the bucket.')
  .option('-d, --data <data>', 'Point data as a .zip, .shp, .gdb, or directory. Provide a local or an S3 key. Zip an GeoJson data can also be accessed via url. Required if no bucket is passed.')
  .option('-t, --transformer <transformer>', 'Data transformer. Defaults to ./transformers/[[file basename]].js.')
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost unless linked to a Docker container aliased to Elasticsearch', esHost || 'localhost')
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200 unless linked to a Docker container aliased to Elasticsearch.', Number, esPort || 9200)
  .option('-l, --log <log>', 'ElasticSearch log level. Defaults to debug.', 'debug')
  .option('--index <index>', 'Elasticsearch index. Defaults to address.', 'address')
  .option('--type <type>', 'Elasticsearch type within the provided or default index. Defaults to point.', 'point')
  .option('--profile <profile>', 'The aws credentials profile in ~/.aws/credentials. Will also respect AWS keys as environment variables.', 'default')
  .option('--source-srs <sourceSrs>', 'Source Spatial Reference System, passed to ogr2ogr as -s_srs. Auto-detects by default.')
  .option('--preformatted', 'Input has been preformatted to GeoJson and transformed to WGS84 by ogr2ogr. Results in the loader skipping ogr2ogr and immediately splitting the input into records.')
  .parse(process.argv);

var usage = checkUsage(program, process.env);
console.log(usage.messages.join(''));
if(usage.err) return;


var client = esLoader.connect(program.host, program.port, program.log);
var counter = new Counter();

if(program.bucket) getS3Files(program, counter, process.env, processData)
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
  var loader = esLoader.load(client, program.index, program.type);
  var source;

  if(program.preformatted){
    if(stream) source = stream;
    else source = fs.createReadStream(fileName);
  }else{
    source = ogrChild(fileName, stream, program.sourceSrs).stdout;
  }

  var verifyResults = verify(fileName, stream);

  source
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

