#!/usr/bin/env node

'use strict';

var options = require('commander');
var winston = require('winston');
var retriever = require('./lib/retriever');
var checkUsage = require('./lib/checkUsage');

var logger = new winston.Logger({
    transports: [
      new (winston.transports.Console)()
    ]
  });


//Favor source GDAL installations for ogr transformations
process.env.PATH = '/usr/local/bin:' + process.env.PATH

//If linked to an elasticsearch Docker container, default to the appropriate host and port
var esVar = process.env.ELASTICSEARCH_PORT;
var esHost = 'localhost';
var esPort = 9200;

if(esVar){
  esVar = esVar.split('//')[1].split(':');
  esHost = esVar[0];
  esPort = +esVar[1];
}

options
  .version('0.0.1')
  .option('-f, --file <file>', 'The json data file that contains the collected data endpoints and field mappings.')
  .option('-m, --match <match>', 'A string or regular expression that the names from the <file> must contain or match')
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost unless linked to a Docker container aliased to Elasticsearch', esHost)
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200 unless linked to a Docker container aliased to Elasticsearch.', Number, esPort)
  .option('-a, --alias <alias>', 'Elasticsearch index alias. Defaults to address.', 'address')
  .option('-t, --type <type>', 'Elasticsearch type within the provided or default alias. Defaults to point.', 'point')
  .option('-l, --log <log>', 'ElasticSearch log level. Defaults to debug.', 'debug')
  .option('-q, --quiet', 'Suppress logging.', false)
  .option('-b, --backup-bucket <backupBucket>', 'An S3 bucket where the data should be backed up.')
  .option('-d, --backup-directory <backupDirectory>', 'A directory where the data should be loaded, either relative to the current folder or the passed S3 bucket.')
  .option('--profile', 'The aws profile in ~/.aws/credentials. Will also respect environmental variables.', 'default')
  .option('--monitor', 'Run the retriever in monitoring mode which only checks data source freshness and doesn\'t load or backup data.')
  .parse(process.argv);

if(options.quiet){
  logger.remove(winston.transports.Console);
}

options.logger = logger;


var usage = checkUsage(options, process.env);
if(usage.err) throw new Error(usage.messages.join(''));
usage.messages.forEach(function(v){logger.info(v)});


retriever(options, function(output){
  //get streams; load each of them
  //loader(options, stream, callback
  //
  logger.info('%d error%s encountered.',
    output.errors.length,
    output.errors.length === 1 ? '' : 's'
  );

  output.errors.forEach(function(v, i){
    logger.error(v);
    output.errors[i] = v.toString();
  });

  if(!options.bucket && !options.directory){
    logger.info('%d source%s still fresh, %d source%s need updates.',
      output.fresh.length,
      output.fresh.length === 1 ? '' : 's',
      output.stale.length,
      output.stale.length === 1 ? '' : 's'
    );
  }else{
    logger.info('Fetched %d source%s and placed %s in %s.',
      output.retrieved.length,
      output.retrieved.length === 1 ? '' : 's',
      output.retrieved.length === 1 ? 'it' : 'them',
      output.location
    );
  }

  logger.info(JSON.stringify(output));
});

