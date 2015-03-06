var ogr = require('../lib/ogr');
var transformer = require('../transforms/arkansas');
var fs = require('fs');
var split = require('split');

ogr('t.json').pipe(split(/,\r?\n/)).pipe(transformer()).pipe(fs.createWriteStream('op.txt'));
