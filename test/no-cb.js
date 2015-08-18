#!/usr/bin/env node

'use strict';

var program = require('commander');
var retriever = require('../index');

program
  .version('0.0.1')
  .option('-b, --bucket <bucket>', 'An S3 bucket where the data should be loaded.')
  .option('-p, --profile <profile>', 'The aws profile in ~/.aws/credentials. Will also respect environmental variables.', 'default')
  .option('-d, --directory <directory>', 'A directory where the data should be loaded, either relative to the current folder or the passed S3 bucket.', '.')
  .option('-f --file <file>', 'The json data file that contains the collected data endpoints. Defaults to data.json.', 'data.json')
  .option('-m --match <match>', 'A string or regular expression that the names from the <file> must contain or match', '')
  .parse(process.argv)

retriever(program);
