'use strict';
var path = require('path');
var spawn = require('child_process').spawn;
var split = require('split');

var fcRegex = /^Feature Count/;
var countRegex = /\d+$/;

function verify(file, featureCount, cb){
  featureCount = Number(featureCount);
  var child = spawn('ogrinfo', ['-al', '-so', '-ro', path.resolve(file)]);
  var noMatch = 1; 

  child.stdout.pipe(split()).on('data', function(line){
    if(line.match(fcRegex)){
      noMatch = 0;
      var fileFeatures = Number(line.match(countRegex)[0]);

      if(featureCount === fileFeatures) return cb();
      return cb({error: new Error('Expected: ' + featureCount + '\nActual: ' + fileFeatures+ '\n'),
                 expected: featureCount,
                 actual: fileFeatures
                });
    }
  });

  child.stdout.on('end', function(err){
    if(noMatch){
      return cb({error: new Error('Couldn\'t access features in: ' + path.resolve(file) + '. Check that it exists.')});
    }
  });

}

module.exports = verify;
