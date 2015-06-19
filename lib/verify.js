'use strict';
var fs = require('fs');
var path = require('path');
var spawn = require('child_process').spawn;
var split = require('split');

var fcRegex = /^Feature Count/;
var countRegex = /\d+$/;

function verify(file, stream, scratchSpace, callback){
  var child;
  var tempFile;
  var ended = 0;
  var fileCount;
  var isCsv = path.extname === '.csv';
  if(!scratchSpace) scratchSpace = '.';

  function runOgrInfo(file, stream){
    if(stream){
        child = spawn('ogrinfo', ['-al', '-so', '-ro', '/vsistdin/'])
        stream.pipe(child.stdin);
      }else{
        child = spawn('ogrinfo', ['-al', '-so', '-ro', path.resolve(file)])
      }

      child.stdout.pipe(split()).on('data', function(line){
        if(line.match(fcRegex)){
          fileCount = Number(line.match(countRegex)[0]);
        }
      });

      child.stdout.on('end', function(){
        ended = 1;
      });
  }
  if(isCsv && stream){
    tempFile = path.join(scratchSpace, file);
    stream.pipe(fs.createWriteStream(tempFile))
      .on('finish', function(){
        runOgrInfo(tempFile, null);
      })
      .on('error', function(err){
         callback(err);
      });
  }else{
    runOgrInfo(file, stream);
  }

  return function(count, cb){
    count = Number(count);

    function verifyResults(){
      if(fileCount === undefined) return cb({error: new Error('Couldn\'t access features in: ' + path.resolve(file) + '. Check that it exists.')});

      if(count === fileCount) return cb();

      return cb({error: new Error('Loaded: ' + count + '\nPresent in file: ' + fileCount + '\n'),
                 loaded: count,
                 present: fileCount
                });
    }

    if(ended) return verifyResults();
    child.stdout.on('end', verifyResults);
  }
}


module.exports = verify;
