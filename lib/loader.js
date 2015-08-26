'use strict';

var winston = require('winston');
var loaderPipeline = require('./loader-pipeline');


function run(options, stream, record, loaderCallback){

  if(!options.logger) options.logger = winston;
  var logger = options.logger;


  if(!loaderCallback){
    loaderCallback = function(err){
      if(err){
        if(typeof err === "object" && !(err instanceof Error)){
          err = new Error(JSON.stringify(err));
        }
        throw err;
      }
      logger.info("Loading complete.");
    }
  }


  logger.info("Streaming %s to elasticsearch.", record.name);


  var loader = loaderPipeline(options, stream, record, loaderCallback);

  loader.on('error', function(err){
    loader.removeAllListeners();
    return loaderCallback(err)
  });


  loader.on('alias', function(){

    var count = loader.count;
    var expectedCount = record.count;

    if(expectedCount){
      if(count === expectedCount){
        logger.info("All %d records from %s loaded.", count, record.name);
        return loaderCallback(null);
      }else{
        return loaderCallback(new Error('Didn\'t load all features. Expected: ' + expectedCount + '. Actual: ' + count));
      }
    }else{
      logger.warn('No count in metadata to verify.');
      return loaderCallback(null);
    }
  });

}

module.exports = run;
