var path = require('path');
var spawn = require('child_process').spawn;
var split = require('split');

var fcRegex = /^Feature Count/;
var countRegex = /\d+$/;

function verify(file, featureCount, cb){
  featureCount = Number(featureCount);
  var child = spawn('ogrinfo', ['-al', '-so', '-ro', path.resolve(file)]);
  
  child.stdout.pipe(split()).on('data', function(line){
    if(line.match(fcRegex)){
      var fileFeatures = Number(line.match(countRegex)[0]);

      if(featureCount === fileFeatures) return cb();
      return cb({error: new Error('Expected: ' + featureCount + '\nActual: ' + fileFeatures+ '\n'),
                 expected: featureCount,
                 actual: fileFeatures
                });
    }
  });

  child.stderr.pipe(split()).once('data', function(data){
    cb({error: new Error('Problem accessing: ' + path.resolve(file))});
  });
}

module.exports = verify;
