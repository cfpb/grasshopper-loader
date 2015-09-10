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


  logger.info("Streaming %s to elasticsearch on %s:%d.", record.name, options.host, options.port);


  loaderPipeline(options, stream, record, function(err, loader){
    if(err) loaderCallback(err);

    loader.on('error', function(err){
      loader.removeAllListeners();
      return loaderCallback(err)
    });


    loader.on('loaded', function(){
      var count = loader.count;
      var expectedCount = record.count;

      if(expectedCount){
        if(count === expectedCount){
          logger.info("All %d expected records from %s loaded.", count, record.name);
          return loaderCallback(null);
        }else{
          return loaderCallback(new Error('Didn\'t load all features. Expected: ' + expectedCount + '. Actual: ' + count));
        }
      }
    });
  });

}

module.exports = run;
