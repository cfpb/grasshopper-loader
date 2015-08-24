#!/usr/bin/env node

'use strict';

var options = require('commander');
var winston = require('winston');
var retriever = require('./lib/retriever');

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
  .option('--alias <alias>', 'Elasticsearch index alias. Defaults to address.', 'address')
  .option('--type <type>', 'Elasticsearch type within the provided or default alias. Defaults to point.', 'point')
  .option('-l, --log <log>', 'ElasticSearch log level. Defaults to debug.', 'debug')
  .option('-b, --backup-bucket <bucket>', 'An S3 bucket where the data should be backed up.')
  .option('-d, --backup-directory <directory>', 'A directory where the data should be loaded, either relative to the current folder or the passed S3 bucket.')
  .option('-p, --profile <profile>', 'The aws profile in ~/.aws/credentials. Will also respect environmental variables.', 'default')
  .option('-q --quiet', 'Suppress logging.', false)
  .parse(process.argv);

if(options.quiet){
  logger.remove(winston.transports.Console);
}

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

  console.log(JSON.stringify(output));
});

