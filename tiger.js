#!/usr/bin/env node

'use strict';

var fs = require('fs-extra');
var path = require('path');
var crypto = require('crypto');
var pump = require('pump');
var OgrJsonStream = require('ogr-json-stream');
var winston = require('winston');
var options = require('commander');
var async = require('async');
var ogrChild = require('./lib/ogrChild');
var unzipFile = require('./lib/unzipFile');
var loader = require('./lib/loader');

//If linked to an elasticsearch Docker container
var esVar = process.env.ELASTICSEARCH_PORT;
var esHost = 'localhost';
var esPort = 9200;

if(esVar){
  esVar = esVar.split('//')[1].split(':');
  esHost = esVar[0];
  esPort = +esVar[1];
}

var logger = new winston.Logger({
    transports: [
      new (winston.transports.Console)()
    ]
  });

var scratchSpace = 'scratch/' + crypto.pseudoRandomBytes(10).toString('hex');
fs.mkdirsSync(scratchSpace);


options
  .version('0.0.1')
  .option('-d, --directory <directory>', 'Directory where TIGER files live')
  .option('-c, --concurrency <concurrency>', 'How many loading tasks will run at once. Defaults to 4.', 4)
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost', esHost)
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200', Number, esPort)
  .option('-a, --alias <alias>', 'Elasticsearch index alias. Defaults to census', 'census')
  .option('-t, --type <type>', 'Elasticsearch type within the provided or default index. Defaults to addrfeat', 'addrfeat')
  .option('-l, --log <log>', 'ElasticSearch log level. Defaults to error.', 'error')
  .option('--profile', 'The aws credentials profile in ~/.aws/credentials. Will also respect AWS keys as environment variables.', 'default')
  .option('-q, --quiet', 'Suppress logging.', false)
  .parse(process.argv);

options.logger = logger;

function worker(file, callback){
  var name = path.basename(file, path.extname(file));
  var record = {name: name, file: name + '.shp'}

  unzipFile(file, record, scratchSpace, function(unzipped){
    var child = ogrChild(unzipped);
    var stream = OgrJsonStream.stringify();

    child.stderr.on('data', function(data){
      logger.error('Error:', data.toString());
    });

    pump(child.stdout, stream);

    loader(options, stream, record, callback);
  }, function(record, err){
       if(this){
         if(this.unpipe) this.unpipe();
         if(this.destroy) this.destroy();
         if(this.kill) this.kill();
       }

       logger.error('Error extracting data from %s: \n', record.name, err);
       callback(err);
     }
  );
}



var queue = async.queue(worker, options.concurrency);

queue.drain = function(){
  logger.info('All files processed.');
};

fs.readdir(options.directory, function(err, files){
  if(err) return logger.error(err);
  files.forEach(function(file){
    queue.push(path.join(options.directory, file), function(err){
      if(err)logger.error(err);
      logger.info(file + ' finished processing');
    })
  })
});

