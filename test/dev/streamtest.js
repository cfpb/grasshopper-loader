'use strict';
var ogr = require('../../lib/ogr');
var transformer = require('../../transforms/arkansas');
var fs = require('fs');
var splitOGR = require('../../lib/splitOGRJSON');
ogr('testdata/t.json').pipe(splitOGR()).pipe(transformer()).pipe(process.stdout);
