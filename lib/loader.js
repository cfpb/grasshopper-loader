#!/usr/bin/env node

'use strict';

var fs = require('fs-extra');
var crypto = require('crypto');
var isUrl = require('is-url');

var checkUsage = require('./checkUsage');
var getS3Files = require('./getS3Files');
var getGeoUrl = require('./getGeoUrl');
var getGeoFiles = require('./getGeoFiles');
var esLoader = require('./esLoader');
var unzipGeoStream = require('./unzipGeoStream');
var Counter = require('./counter');
var loaderPipeline = require('./loader-pipeline');


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

  try{
    var fieldFile = fs.readJSONSync(program.fields);
  }catch(err){
    return loaderCallback(err);
  }


  try{
    var client = esLoader.connect(program.host, program.port, program.log);
  }catch(e){
    return loaderCallback(e);
  }

  //Cleanup if bad file
  if(!program.bucket && program.data){
    try{
      fs.statSync(program.data);
    }catch(err){
      return loaderCallback(err);
    }
  }


  var counter = new Counter();

  if(program.bucket) getS3Files(program, counter, process.env, processData)
  else if(isUrl(program.data)) getGeoUrl(program.data, counter, processData);
  else getGeoFiles(program.data, counter, processData)


  function processData(err, stream){

    if(err) return loaderCallback(err);

    console.log("Streaming %s to elasticsearch.", program.name);

    var loader = loaderPipeline(program, stream, client, loaderCallback);

    loader.on('error', function(err){
      loader.removeAllListeners();
      return loaderCallback(err)
    });

    loader.on('alias', function(){

      if(counter.decr() === 0){
        client.close();
      }

      var count = loader.count;

      var expectedCount;

      for(var i=0; i<fieldFile.length; i++){
        if(fieldFile[i].name === program.name){
          expectedCount = fieldFile[i].count;
        }
      }

      if(expectedCount){
        if(count === expectedCount){
          console.log("All %d records from %s loaded.", count, program.name);
          return loaderCallback(null);
        }else{
          return loaderCallback(new Error('Didn\'t load all features. Expected: ' + expectedCount + '. Actual: ' + count));
        }
      }else{
        console.log('No count in metadata to verify.');
      }
    });
  }
}

module.exports = run;
