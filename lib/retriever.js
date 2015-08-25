var fs = require('fs-extra');
var util = require('util');
var path = require('path');
var crypto = require('crypto');
var url = require('url');
var winston = require('winston');
var pump = require('pump');
var request = require('request');
var zlib = require('zlib');
var yauzl = require('yauzl');
var csvToVrt = require('csv-to-vrt');
var UploadStream = require('./UploadStream');
var connectToFtp = require('./connectToFtp');
var checkHash = require('./checkHash');
var spawnOgr = require('./spawnOgr');
var fieldFilter = require('./fieldFilter');

var zipReg = /.zip$/i;
var csvReg = /(?:txt|csv)$/i;
var restrictedReg = /\.\.|\//;


function retrieve(options, callback){

  var output = {
    errors: [],
    fresh: [],
    stale: [],
    processed: [],
    retrieved: [],
    startTime: Date.now(),
    endTime: null,
    location: ''
  };

  var scratchSpace = 'scratch/' + crypto.pseudoRandomBytes(10).toString('hex');
  fs.mkdirsSync(scratchSpace);


  var logger = options.logger || winston;
  fieldFilter.setLogger(logger);


  function wrappedCb(err){
    if(err) output.errors.push(err);

    try{
      fs.removeSync(scratchSpace);
    }catch(e){
      output.errors.push(e);
    }

    output.endTime = Date.now();

    if(output.errors.length && !callback) throw output.errors.join('\n');

    if(callback) callback(output);
  }


  if(!options.file) return wrappedCb(new Error('Must provide a metadata file with the -f option.'));


  var stringMatch = typeof options.match === 'string';
  var regMatch = typeof options.match === 'object';


  try{
    var data = fs.readJSONSync(options.file);
    var recordCount = data.length;
  }catch(err){
    return wrappedCb(err);
  }


  function recordCallback(err, record){
    output.processed.push(record.name);

    if(err){
      logger.error(err);
      output.errors.push(err);
    }else{
      output.retrieved.push(record.name);
    }
    if(output.processed.length === recordCount) wrappedCb(null);
  }


  if(options.backupBucket) var uploadStream = new UploadStream(options.backupBucket, options.profile);


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
      return recordCount;
    }


    var urlObj = url.parse(record.url);

    if(urlObj.protocol === 'ftp:'){
      connectToFtp(urlObj, record, processRequest, recordCallback);
    }else{
      processRequest(request(record.url), record);
    }
  });


  function processRequest(stream, record){
    checkHash(stream, record.hash, function(hashIsEqual, remoteHash){
      if(hashIsEqual){
        logger.info('Remote file for %s verified.', record.name);
        record._retrieverVerified = 1;
        output.fresh.push(record.name);

        //if no output or stream has already completed
        if(monitoringMode || record._retrieverProcessed) return recordCallback(null, record);
        return;
      }
      output.stale.push(record.name);
      stream.emit('error', new Error('The hash from ' + record.name + ' did not match the downloaded file\'s hash.\nRecord hash: ' + record.hash +'\nRemote hash: ' + remoteHash +'\n'));
    });

    stream.on('error', handleStreamError.bind(stream, record));

    if(monitoringMode) return;

    if(zipReg.test(record.url)){
      var zipdir = path.join(scratchSpace, record.name);
      var zipfile = zipdir + '.zip';

      pump(stream, fs.createWriteStream(zipfile), function(err){
        if(err) return stream.emit('error', err);

        yauzl.open(zipfile, function(err, zip){
          if(err) return recordCallback(err, record);
          var entriesFinished = 0;
          var count = 0;

          zip.on('end', function(){
            entriesFinished = 1;
          });

          zip.on('entry', function(entry){
            if(/\/$/.test(entry.fileName)) return;

            zip.openReadStream(entry, function(err, readStream) {
              if(err) return handleStreamError.call(this, record, err);
              count++;
              var output = fs.createOutputStream(path.join(zipdir, entry.fileName));

              pump(readStream, output, function(err){
                if(err) return recordCallback(err, record);
                count--;
                if(entriesFinished && !count){
                  var unzipped = path.join(zipdir, record.file);

                  if(csvReg.test(record.file)){

                    csvToVrt(unzipped, record.sourceSrs, function(err, vrt){
                      if(err) return recordCallback(err, record);
                      handleStream(spawnOgr(record, vrt), record);
                    });

                  }else{
                    handleStream(spawnOgr(unzipped), record);
                  }
                }
              });
            });
          });
        });
      });
    }else{
      if(csvReg.test(record.file)){
        var csv = path.join(scratchSpace, record.file);
        var csvStream = fs.createWriteStream(csv);

        pump(stream, csvStream, function(err){
          if(err) return recordCallback(err, record);

          csvToVrt(csv, record.sourceSrs, function(err, vrt){
            if(err) return recordCallback(err, record);
            handleStream(spawnOgr(record, vrt), record);
          });
        });

      }else{
        handleStream(spawnOgr(record, null, stream), record);
      }
    }
  }


  function handleStreamError(record, err){
    if(this.unpipe) this.unpipe();
    if(this.destroy) this.destroy();
    if(this.kill) this.kill();
    if(record._retrieverOutput){
      if(options.backupBucket){
        record._retrieverOutput.abortUpload(function(err){
          if(err) logger.error(err);
        });
      }else{
        fs.removeSync(record._retrieverOutput);
      }
    }
    recordCallback(err, record);
  }


  


  function handleStream(stream, record){

    if(options.backupBucket && !options.backupDirectory){
      options.backupDirectory = '.';
    }

    var endfile = path.join(options.backupDirectory, record.name + '.csv.gz');
    var zipStream = zlib.createGzip();
    var destStream;

    if(options.backupBucket){
      destStream = uploadStream.stream(endfile);
      record._retrieverOutput = destStream;
      output.location = options.backupBucket + '/' + options.backupDirectory;
    }else{
      destStream = fs.createWriteStream(endfile);
      record._retrieverOutput = endfile;
      output.location= options.backupDirectory;
    }

    pump(stream, zipStream, destStream, function(err){
      if(!err) record._retrieverProcessed = 1;
      if(err||record._retrieverVerified) return recordCallback(err, record);
    });
  }

}

module.exports = retrieve;
