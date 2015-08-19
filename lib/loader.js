#!/usr/bin/env node

'use strict';

var fs = require('fs-extra');
var path = require('path');
var crypto = require('crypto');
var isUrl = require('is-url');
var pump = require('pump');
var lump = require('lump-stream');
var csvParse = require('csv-parse');
var OgrJsonStream = require('ogr-json-stream');

var checkUsage = require('./checkUsage');
var esLoader = require('./esLoader');
var getS3Files = require('./getS3Files');
var getGeoUrl = require('./getGeoUrl');
var getGeoFiles = require('./getGeoFiles');
var unzipGeoStream = require('./unzipGeoStream');
var resolveTransformer = require('./resolveTransformer');
var requireTransformer = require('./requireTransformer');
var ogrChild = require('./ogrChild');
var makeBulkSeparator = require('./makeBulkSeparator');
var verify = require('./verify');
var Counter = require('./counter');


function run(program, passedCallback){

  if(!passedCallback){
    passedCallback = function(err){
      if(err){
        if(typeof err === "object" && !(err instanceof Error)){
          err = new Error(JSON.stringify(err));
        }
        throw err;
      }
      console.log("Loading complete.");
    }
  }

  var usage = checkUsage(program, process.env);
  if(usage.err) return passedCallback(new Error(usage.messages.join('')));
  console.log(usage.messages.join(''));

  var scratchSpace = fs.mkdirsSync('./scratch/' + crypto.pseudoRandomBytes(8).toString('hex'));
  unzipGeoStream.setScratchSpace(scratchSpace);

  var loaderCallback = function(err){
    fs.remove(scratchSpace, function(e){
      return passedCallback(err||e);
    });
  };


  //Cleanup if bad file
  if(!program.bucket && program.data){
    try{
      fs.statSync(program.data);
    }catch(err){
      return loaderCallback(err);
    }
  }

  try{
    var client = esLoader.connect(program.host, program.port, program.log);
  }catch(err){
    loaderCallback(err);
  }

  var counter = new Counter();

  if(program.bucket) getS3Files(program, counter, process.env, processData)
  else if(isUrl(program.data)) getGeoUrl(program.data, counter, processData);
  else getGeoFiles(program.data, counter, processData)


  function processData(err, fileName, stream){

    if(err) return loaderCallback(err);

    var transformer;
    try{
      transformer = getTransformer(fileName)
    }catch(err){
      console.log("transformer error", err);
      return loaderCallback(err);
    }

    console.log("Streaming %s to elasticsearch.", fileName);

    return pipeline(fileName, stream, transformer);
  }


  function getTransformer(fileName){
    var transFile = resolveTransformer(program.transformer, fileName);
    return requireTransformer(transFile, fileName);
  }


  function pipeline(fileName, stream, transformer){
    var basename = path.basename(fileName, path.extname(fileName));
    var loader;

    try{
      loader = esLoader.load(client, basename, program.alias, program.type);
    }catch(e){
      return loaderCallback(e);
    }

    var source;
    var parser;

    if(program.preformatted){
      if(stream) source = stream;
      else source = fs.createReadStream(fileName);
    }else{
      source = ogrChild(fileName, stream, program.sourceSrs).stdout;
    }

    if(path.extname(fileName) === '.csv') parser = csvParse({columns: true});
    else parser = OgrJsonStream();

    var verifyResults = verify(fileName, stream, scratchSpace, loaderCallback);

    pump(
      source,
      parser,
      transformer(fileName, makeBulkSeparator(), '\n'),
      lump(Math.pow(2, 20)),
      loader,

      function(err){
        var error;
        if(err){
          console.log('Streaming of %s interrupted by error,', fileName);
          return loaderCallback(err);
        }

        loader.on('error', function(err){
          error = 1;
          return loaderCallback(err)
        });

        loader.on('alias', function(){
          if(error) return;
          console.log('Finished streaming %s', fileName);

          if(counter.decr() === 0){
            client.close();
          }

          var count = loader.count;

          verifyResults(count, function(errObj){
            if(errObj) return loaderCallback(errObj.error);
            console.log("All %d records from %s loaded.", count, fileName);
            return loaderCallback(null);
          });
        })
      }
    )
  }
}

module.exports = run;
