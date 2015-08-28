#!/usr/bin/env node

'use strict';

var options = require('commander');
var winston = require('winston');
var retriever = require('../../../lib/retriever');
var checkUsage = require('../../../lib/checkUsage');

var logger = new winston.Logger({
    transports: [
      new (winston.transports.Console)()
    ]
  });


//Favor source GDAL installations for ogr transformations
process.env.PATH = '/usr/local/bin:' + process.env.PATH

var esVar = process.env.ELASTICSEARCH_PORT;
var esHost;
var esPort;

if(esVar){
  esVar = esVar.split('//')[1].split(':');
  esHost = esVar[0];
  esPort = +esVar[1];
}
options
  .version('0.0.1')
  .option('-f --file <file>', 'The json data file that contains the collected data endpoints and field mappings.')
  .option('-m --match <match>', 'A string or regular expression that the names from the <file> must contain or match')
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost unless linked to a Docker container aliased to Elasticsearch', esHost || 'localhost')
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200 unless linked to a Docker container aliased to Elasticsearch.', Number, esPort || 9200)
  .option('-a, --alias <alias>', 'Elasticsearch index alias. Defaults to address.', 'address')
  .option('-t, --type <type>', 'Elasticsearch type within the provided or default alias. Defaults to point.', 'point')
  .option('-l, --log <log>', 'ElasticSearch log level. Defaults to debug.', 'debug')
  .option('-q --quiet', 'Suppress logging.', false)
  .option('-b, --backup-bucket <backupBucket>', 'An S3 bucket where the data should be backed up.')
  .option('-d, --backup-directory <backupDirectory>', 'A directory where the data should be loaded, either relative to the current folder or the passed S3 bucket.')
  .option('--profile <profile>', 'The aws profile in ~/.aws/credentials. Will also respect environmental variables.', 'default')
  .option('--monitor', 'Run the retriever in monitoring mode which only checks data source freshness and doesn\'t load or backup data.')
  .parse(process.argv);

if(options.quiet){
  logger.remove(winston.transports.Console);
}

options.logger = logger;


var usage = checkUsage(options, process.env);
if(usage.err) throw new Error(usage.messages.join(''));
usage.messages.forEach(function(v){logger.info(v)});


retriever(options);

