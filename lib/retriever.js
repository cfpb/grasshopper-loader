var fs = require('fs-extra');
var async = require('async');
var util = require('util');
var crypto = require('crypto');
var url = require('url');
var winston = require('winston');
var request = require('request');
var resolveOverrides = require('./resolveOverrides');
var ftp = require('./ftpWrapper');
var handleZip = require('./handleZip');
var handleCsv = require('./handleCsv');
var retrieverPipeline = require('./retriever-pipeline');
var fieldFilter = require('./fieldFilter');
var assureRecordCount = require('./assureRecordCount');
var loader = require('./loader');

var zipReg = /.zip$/i;
var csvReg = /(?:txt|csv)$/i;
var restrictedReg = /\.\.|\//;


function retrieve(options, callback){

  var output = {
    errors: [],
    overridden: [],
    processed: [],
    loaded: [],
    startTime: Date.now(),
    endTime: null
  };

  var overrides;


  var scratchSpace = 'scratch/' + crypto.pseudoRandomBytes(10).toString('hex');
  fs.mkdirsSync(scratchSpace);


  var logger = options.logger || winston;
  fieldFilter.setLogger(logger);


  function wrappedCb(err){
    if(err) output.errors.push(err);
    ftp.closeClients();

    try{
      fs.removeSync(scratchSpace);
    }catch(e){
      output.errors.push(e);
    }

    output.endTime = Date.now();
    if(output.errors.length && !callback) throw output.errors.join('\n');
    if(callback) callback(output);
    else if(options.client) options.client.close();
  }


  try{
    fs.statSync(options.file)
  }catch(e){
    return wrappedCb(new Error('Must provide a valid metadata file with the -f option.'));
  }

  if(!parseInt(options.concurrency, 10)) return wrappedCb(new Error('Must provide valid concurrency value. Found: ' + options.concurrency));

  var stringMatch = typeof options.match === 'string';
  var regMatch = typeof options.match === 'object';


  try{
    var data = fs.readJSONSync(options.file);
    var recordCount = data.length;
  }catch(err){
    return wrappedCb(err);
  }


  function recordCallback(err, record){
    if(record._processed) return;
    else record._processed = 1;
    output.processed.push(record.name);

    if(err){
      logger.error(err);
      output.errors.push(err);
    }

    logger.info('Finished processing %s. Total processed count: %d. Record count: %d', record.name, output.processed.length, recordCount);

    if(output.processed.length === recordCount){
      return wrappedCb(null);
    }
  }


  function worker(record, callback){

    //Don't allow to traverse to other folders via data.json
    if(restrictedReg.test(record.name)){
      return callback(new Error(util.format('Invalid record name %s. Must not contain ".." or "/".', record.name)), record);
    }

    if(stringMatch && options.match.indexOf(record.name) === -1 ||
      regMatch && !options.match.test(record.name)
    ){
      if(recordCount === output.processed.length){
          return wrappedCb(null);
      }
      return callback(null, record);
    }

    logger.info('Processing %s', record.name);

    var override = overrides.resolve(record.name);

    if(override){
      logger.info('Override found at %s%s', options.bucket ? options.bucket + '/' : '', options.directory);
      record._override = override;
      processRequest(overrides.get(record.name), record, callback);
    }else{
      var urlObj = url.parse(record.url);
      if(urlObj.protocol === 'ftp:'){
        ftp.connect(urlObj, function(err){
          if(err) return callback(err, record);
          ftp.request(urlObj, function(err, stream){
            if(err) return callback(err, record);
            return processRequest(stream, record, callback);
          });
        },
        function(err){
          logger.error(err);
        });
      }else{
        try{
          processRequest(request(record.url), record, callback);
        }catch(e){
          return callback(e, record);
        }
      }
    }
  }


  function processRequest(stream, record, callback){
    var handleError = makeErrorHandler(record, callback);
    var handleStream = makeStreamHandler(record, handleError, callback);

    stream.on('error', handleError.bind(stream));

    //Need fallback logic. Probably on stream error.
    //Such machinery could also be tuned to allow X retries

    if(record._override){
      output.overridden.push(record.name);
    }

    if(record.isZipped || zipReg.test(record._override) || zipReg.test(record.url)){
      logger.info('Unzipping file stream of %s from %s', record.name, record._override || record.url);
      handleZip(stream, record, scratchSpace, handleStream, handleError);
    }else{
      if(csvReg.test(record.file)){
        logger.info('Extracting geodata from csv for %s', record.name);
        handleCsv(stream, record, scratchSpace, handleStream, handleError);
      }else{
        handleStream(null, stream);
      }
    }
  }


  function makeErrorHandler(record, callback){
    return function(stream, err){
      if(record._processed) return;

      if(this && stream && !err){
        err = stream;
        stream = this;
      }

      if(stream){
        if(stream.unpipe) stream.unpipe();
        if(stream.destroy) stream.destroy();
        if(stream.kill) stream.kill();
      }

      if(record._retrieverOutput){
        fs.removeSync(record._retrieverOutput);
      }

      callback(err, record);
    }
  }


  function makeStreamHandler(record, errback, callback){
    return function(file, stream){
      assureRecordCount(record, file, function(err){
        if(err) return errback(record, callback, err);

        if(record.count) logger.info('Detected %d records in %s', record.count, record.name);

        var pipeline = retrieverPipeline(record, file, stream);

        loader(options, pipeline, record, function(err){
          if(record._processed) return;
          if(err) return errback.call(pipeline, record, err);
          output.loaded.push(record.name);

          callback(null, record);
        });
      });
    };
  }


  resolveOverrides(options, function(err, resolved){

    if(err) logger.error('Error assigning data overrides, continuing without them.');

    overrides = resolved;

    var queue = async.queue(worker, options.concurrency);

    data.forEach(function(record){
      queue.push(record, recordCallback);
    });

  });
}

module.exports = retrieve;
