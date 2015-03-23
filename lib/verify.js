var spawn = require('child_process').spawn;
var split = require('split');

var child = spawn('ogrinfo', ['-al', '-so', '-ro', '../test/data/t.json']);

var fcRegex = /^Feature Count/;
var countRegex = /\d+$/;
child.stdout.pipe(split()).on('data', function(line){
  if(line.match(fcRegex)) process.stdout.write(line.match(countRegex)[0])
});
