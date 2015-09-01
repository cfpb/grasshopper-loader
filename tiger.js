#!/usr/bin/env node

'use strict';

var fs = require('fs-extra');
var path = require('path');
var pump = require('pump');
var OgrJsonStream = require('ogr-json-stream');
var winston = require('winston');
var options = require('commander');
var async = require('async');
var ogrChild = require('./ogrChild');
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

options
  .version('0.0.1')
  .option('-d, --directory <directory>', 'Directory where TIGER files live')
  .option('-c, --concurrency <concurrency>', 'How many loading tasks will run at once. Defaults to 4.', 4)
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost', esHost)
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200', Number, esPort)
  .option('-a, --alias <alias>', 'Elasticsearch index alias. Defaults to census', 'census')
  .option('-t, --type <type>', 'Elasticsearch type within the provided or default index. Defaults to addrfeat', 'addrfeat')
  .option('--profile', 'The aws credentials profile in ~/.aws/credentials. Will also respect AWS keys as environment variables.', 'default')
  .option('-q, --quiet', 'Suppress logging.', false)
  .parse(process.argv);

options.logger = logger;

function worker(file, callback){
  var child = ogrChild(file);
  var stream = OgrJsonStream();

  pump(child, stream);

  loader(options, stream, {name: path.basename(file, path.extname(file))}, callback);
}


var queue = async.queue(worker, options.concurrency);

queue.drain = function(){
  logger.info('All files processed.');
};

fs.readdir(options.directory, function(err, files){
  if(err) return logger.error(err);
  files.forEach(function(file){
    queue.push(file, function(err){
      if(err)logger.error(err);
      logger.info(file + ' finished processing');
    })
  })
});

