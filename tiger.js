#!/usr/bin/env node

'use strict';

var fs = require('fs-extra');
var path = require('path');
var url = require('url');
var crypto = require('crypto');
var pump = require('pump');
var OgrJsonStream = require('ogr-json-stream');
var options = require('commander');
var async = require('async');
var addProps = require('add-props-stream');
var ogrChild = require('./lib/ogrChild');
var ftp = require('./lib/ftpWrapper');
var unzipFile = require('./lib/unzipFile');
var assureRecordCount = require('./lib/assureRecordCount');
var loader = require('./lib/loader');
var esLoader = require('./lib/esLoader');
var createIndex = require('./lib/createIndex');
var getTigerState = require('./lib/getTigerState');
var makeLogger = require('./lib/makeLogger');

//If linked to an elasticsearch Docker container
var esVar = process.env.ELASTICSEARCH_PORT;
var esHost = 'localhost';
var esPort = 9200;

if(esVar){
  esVar = esVar.split('//')[1].split(':');
  esHost = esVar[0];
  esPort = +esVar[1];
}


var scratchSpace = 'scratch/' + crypto.pseudoRandomBytes(10).toString('hex');
fs.mkdirsSync(scratchSpace);


options
  .version('0.0.1')
  .option('-d, --directory <directory>', 'Directory where TIGER files live. Can be a remote FTP directory.', 'ftp://ftp2.census.gov/geo/tiger/TIGER2015/ADDRFEAT/')
  .option('-c, --concurrency <concurrency>', 'How many loading tasks will run at once. Defaults to 4.', 4)
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost', esHost)
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200', Number, esPort)
  .option('-a, --alias <alias>', 'Elasticsearch index alias. Defaults to census', 'census')
  .option('-t, --type <type>', 'Elasticsearch type within the provided or default index. Defaults to addrfeat', 'addrfeat')
  .option('-l, --log <log>', 'ElasticSearch log level. Defaults to error.', 'error')
  .option('-q, --quiet', 'Suppress logging.', false)
  .parse(process.argv);


var logger = makeLogger(options);

options.client = esLoader.connect(options.host, options.port, options.log);


function worker(file, callback){

  //ftp url object
  if(file.href){
    ftp.request(file, function(err, stream){
      if(err) return logger.error(err);

      var zipfile = path.join(scratchSpace, path.basename(file.path));
      pump(stream, fs.createOutputStream(zipfile), function(err){
        if(err) return logger.error(err);

        processFile(zipfile, callback);
      });
    });
  }else{
    processFile(file, callback);
  }
}


function processFile(file, callback){
  var name = path.basename(file, path.extname(file));
  var record = {name: name, file: name + '.shp'}

  function handleStreamError(record, err){
    if(this){
      if(this.unpipe) this.unpipe();
      if(this.destroy) this.destroy();
      if(this.kill) this.kill();
    }

    logger.error('Error extracting data from %s: \n', record.name, err);
    callback(err);
  }


  unzipFile(file, record, scratchSpace, function(unzipped){
    assureRecordCount(record, unzipped, function(err){
      if(err) return handleStreamError(record, err);
      if(record.count) logger.info('Detected %d records in %s', record.count, record.name);

      var child = ogrChild(unzipped);
      var stream = addProps.stringify('properties.STATE', getTigerState(record.file));
      child.stderr.on('data', function(data){
        logger.error('Error:', data.toString());
      });

      pump(child.stdout, OgrJsonStream(), stream);

      loader(options, stream, record, callback);
    });
    },
    handleStreamError
  );
}


var queue = async.queue(worker, options.concurrency);


queue.drain = function(){
  esLoader.applyAlias(options, options.forcedIndex, function(err){
    options.client.close();
    ftp.closeClients();
    if(err) return logger.error('Unable to apply alias to %s', options.forcedIndex, err);
    logger.info('All files processed.');
  });
};


function enqueueFtp(urlObj){

  ftp.connect(urlObj, function(err){
    if(err) return logger.error(err);

    ftp.list(urlObj, function(err, ftpListings){
      if(err) return logger.error(err);

      ftpListings.forEach(function(listing){
        var fileUrl = urlObj.href + listing.name;
        var fileObj = url.parse(fileUrl);
        enqueueFile(fileObj);
      });
    });
  },
  function(err){
    logger.error('Ftp error:', err)
  });
}


function enqueueDirectory(){
  fs.readdir(options.directory, function(err, files){
    if(err) return logger.error(err);
    files.forEach(function(file){
      enqueueFile(path.join(options.directory, file));
    })
  });
}


function enqueueFile(file){
  queue.push(file, function(err){
    if(err) logger.error(err);
    if(file.href) file = file.href;
    logger.info(path.basename(file) + ' finished processing');
  });
}


createIndex(options, 'tiger', function(err, index){
  if(err) return logger.error(err);
  options.forcedIndex = index;

  var urlObj = url.parse(options.directory);

  if(urlObj.hostname){
    enqueueFtp(urlObj);
  }else{
    enqueueDirectory();
  }
});
