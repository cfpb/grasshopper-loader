#!/usr/bin/env node

'use strict';

var options = require('commander');
var retriever = require('./lib/retriever');
var makeLogger = require('./lib/makeLogger');
var esLoader = require('./lib/esLoader');


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
  .option('-l, --log <log>', 'ElasticSearch log level. Defaults to error.', 'error')
  .option('-c, --concurrency <concurrency>', 'How many loading tasks will run at once. Defaults to 2.', 2)
  .option('-q, --quiet', 'Suppress logging.', false)
  .option('-b, --bucket <bucket>', 'An S3 bucket where the data resides.')
  .option('-d, --directory <directory>', 'A directory where data sources reside, either relative to the current folder or the passed S3 bucket.')
  .option('-P, --profile <profile>', 'The aws profile in ~/.aws/credentials. Only needed if loading data from a bucket. AWS environment variables will override this value.', 'default')
  .parse(process.argv);


var logger = makeLogger(options);


options.client = esLoader.connect(options.host, options.port, options.log);

if(options.directory && options.directory[options.directory.length - 1] === '/') options.directory = options.directory.slice(0, -1);

retriever(options, function(output){
  options.client.close();

  logger.info('%d error%s encountered.',
    output.errors.length,
    output.errors.length === 1 ? '' : 's'
  );

  output.errors.forEach(function(v, i){
    logger.info(v);
    output.errors[i] = v.toString();
  });

  logger.info('%d source%s loaded, %d source%s overridden from known files.',
    output.loaded.length,
    output.loaded.length === 1 ? '' : 's',
    output.overridden.length,
    output.overridden.length === 1 ? '' : 's'
  );

  logger.info(JSON.stringify(output));
});

