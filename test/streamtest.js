var ogr = require('../lib/ogr');
var transformer = require('../transforms/arkansas');
var fs = require('fs');

ogr('t.json').pipe(transformer()).pipe(fs.createWriteStream('op.txt'));
