var fs = require('fs-extra');
var util = require('util');
var crypto = require('crypto');
var url = require('url');
var winston = require('winston');
var request = require('request');
var resolveOverrides = require('./resolveOverrides');
var ftp = require('./ftpWrapper');
var checkHash = require('./checkHash');
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
    fresh: [],
    stale: [],
    overridden: [],
    processed: [],
    loaded: [],
    startTime: Date.now(),
    endTime: null
  };


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


  resolveOverrides(options, function(err, overrides){

    if(err) logger.error('Error assigning data overrides, continuing without them.');

    data.forEach(function(record){

      //Don't allow to traverse to other folders via data.json
      if(restrictedReg.test(record.name)){
        return recordCallback(new Error(util.format('Invalid record name %s. Must not contain ".." or "/".', record.name)), record);
      }


      //If the record is filtered, remove it from the count
      if(stringMatch && options.match.indexOf(record.name) === -1 ||
        regMatch && !options.match.test(record.name)
      ){
        if(--recordCount === output.processed.length){
            return wrappedCb(null);
        }
        return;
      }

      logger.info('Processing %s', record.name);

      var override = overrides.resolve(record.name);

      if(override){
        logger.info('Override found at %s%s', options.bucket ? options.bucket + '/' : '', options.directory);
        record._override = override;
        processRequest(overrides.get(record.name), record);
      }else{
        var urlObj = url.parse(record.url);
        if(urlObj.protocol === 'ftp:'){
          ftp.connect(urlObj, function(err){
            if(err) return recordCallback(err, record);
            ftp.request(urlObj, function(err, stream){
              if(err) return recordCallback(err, record);
              return processRequest(stream, record);
            });
          },
          function(err){
            logger.error(err);
          });
        }else{
          processRequest(request(record.url), record);
        }
      }
    });


    function processRequest(stream, record){
      stream.on('error', handleStreamError.bind(stream, record));

      //Need fallback logic. Probably on stream error.
      //Such machinery could also be tuned to allow X retries

      if(record._override){
        output.overridden.push(record.name);
      }else{
        //Ensure data has not changed
        checkHash(stream, record.hash, function(hashIsEqual, remoteHash){
          if(hashIsEqual){
            logger.info('Remote file for %s verified.', record.name);
            output.fresh.push(record.name);

            if(options.monitor) return recordCallback(null, record);
            return;
          }
          output.stale.push(record.name);

          var staleErr = new Error('The hash from ' + record.name + ' did not match the downloaded file\'s hash.\nRecord hash: ' + record.hash +'\nRemote hash: ' + remoteHash +'\n')

          //ftp stream is auto-closed before error propagates
          if(url.parse(record.url).protocol === 'ftp:'){
            handleStreamError.call(stream, record, staleErr);
          }else{
            stream.emit('error', staleErr);
          }
        });
      }


      if(options.monitor) return;


      if(zipReg.test(record.url)){
        logger.info('Unzipping file stream of %s from %s', record.name, record._override || record.url);
        handleZip(stream, record, scratchSpace, handleStream, handleStreamError);
      }else{
        if(csvReg.test(record.file)){
          logger.info('Extracting geodata from csv for %s', record.name);
          handleCsv(stream, record, scratchSpace, handleStream, handleStreamError);
        }else{
          handleStream(record, null, stream);
        }
      }
    }



    function handleStreamError(record, err){
      if(record._processed) return;

      if(this){
        if(this.unpipe) this.unpipe();
        if(this.destroy) this.destroy();
        if(this.kill) this.kill();
      }

      if(record._retrieverOutput){
        fs.removeSync(record._retrieverOutput);
      }

      recordCallback(err, record);
    }

    function handleStream(record, file, stream){
      assureRecordCount(record, file, function(err){
        if(err) return handleStreamError(record, err);

        if(record.count) logger.info('Detected %d records in %s', record.count, record.name);

        var pipeline = retrieverPipeline(record, file, stream);

        loader(options, pipeline, record, function(err){
          if(record._processed) return;
          if(err) return handleStreamError.call(pipeline, record, err);
          output.loaded.push(record.name);

          recordCallback(null, record);
        });
      });
    }
  });
}

module.exports = retrieve;
