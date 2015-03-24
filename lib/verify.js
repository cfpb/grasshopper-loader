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
      return cb({expected: featureCount, actual: fileFeatures});
    }
  });
}

module.exports = verify;
