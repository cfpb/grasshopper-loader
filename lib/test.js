var sp = require('child_process').spawn;
var fs = require('fs');

var ls = sp('ls',['-al'])
ls.stdout.pipe(fs.createWriteStream('out.txt'));
