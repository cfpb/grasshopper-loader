'use strict';

var path = require('path');
var spawn = require('child_process').spawn;
var split = require('split2');

var fcRegex = /^Feature Count/;
var countRegex = /\d+$/;

module.exports = function(record, file, cb){
  if(!record) return cb(new Error('Must call assureRecordCount with a record.'));
  if(record.count !== undefined || !file) return cb(null);

  var filePath = path.resolve(file);
  var fileCount;
  var child = spawn('ogrinfo', ['-al', '-so', '-ro', filePath]);
  var splitter = split();

  child.stdout.pipe(splitter).on('data', parseLines);

  child.stdout.on('end', function(){
    if(fileCount === undefined) cb(new Error('Unable to compute feature count on ' + record.name));
  });

  child.stderr.once('data', function(errText){
    if(errText){
      child.kill();
      splitter.removeListener('data', parseLines);
      cb(new Error(errText.toString()));
    }
  });

  function parseLines(line){
    if(line.match(fcRegex)){
      this.removeListener('data', parseLines);
      fileCount = Number(line.match(countRegex)[0]);
      record.count = fileCount;
      cb(null);
    }
  }

};
