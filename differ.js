#!/usr/bin/env node
'use strict';
var path = require('path');
var elasticsearch = require('elasticsearch');
var aws = require('aws-sdk');
var program = require('commander');
var awsAutoAuth = require('aws-auto-auth');

var esVar = process.env.ELASTICSEARCH_PORT;
var S3 = new aws.S3();
var credentials = awsAutoAuth(aws);
var esHost;
var esPort;
var esObj = {};
var s3List;
var makeRequest;

if(credentials.accessKeyId){
  makeRequest = S3.makeRequest.bind(S3);
}else{
  makeRequest= S3.makeUnauthenticatedRequest.bind(S3);
}

if(esVar){
  esVar = esVar.split('//')[1].split(':');
  esHost = esVar[0];
  esPort = +esVar[1];
}

program
  .version('0.0.1')
  .option('-b, --bucket <bucket>', 'An S3 bucket where data resides.')
  .option('-d, --directory <directory>', 'A directory within S3 or on the filesystem relative to where the differ was run.', '.')
  .option('--alias <alias>', 'Elasticsearch index alias. Defaults to address.', 'address')
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost unless linked to a Docker container aliased to Elasticsearch', esHost || 'localhost')
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200 unless linked to a Docker container aliased to Elasticsearch.', Number, esPort || 9200)
  .parse(process.argv);

if(program.bucket && program.directory === '.') program.directory = '';

var client = new elasticsearch.Client({
  host: program.host + ':' + program.port,
  log: []
});

client.indices.get({index: program.alias}, function(err, data){
  if (err) throw err;
  Object.keys(data).forEach(function(index){
    esObj[index.split('-').slice(0, -2).join('-')] = 1;
  });
  diffLists();
});


makeRequest('listObjects', {'Bucket': program.bucket, 'Prefix': program.directory}, function(err, res){
  if(err) throw err;
  s3List = res.Contents.filter(function(v){
    return v.Key[v.Key.length - 1] !== '/';
  }).map(function(v){
    return {key: v.Key, basename: path.basename(v.Key, path.extname(v.Key)).toLowerCase()};
  });
  diffLists();
});

function diffLists(){
  if(!s3List || !Object.keys(esObj).length) return;
  s3List.forEach(function(v){
    if(!esObj[v.basename]) console.log(v.key);
  });
}
