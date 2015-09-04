'use strict';

var path = require('path');
var spawn = require('child_process').spawn;
var split = require('split2');

var fcRegex = /^Feature Count/;
var countRegex = /\d+$/;

module.exports = function(record, file, cb){
  if(record.count !== undefined) return cb(null);
  var fileCount;
  var child = spawn('ogrinfo', ['-al', '-so', '-ro', path.resolve(file)]);
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
