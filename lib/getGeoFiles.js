'use strict';
var fs = require('fs');
var path = require('path');
var nodedir = require('node-dir');

var unzipGeoStream = require('./unzipGeoStream');


function discover(input, counter, callback, silent, afterProcessingCb){
  var ext = path.extname(input);

  if(ext){
    if(ext === '.zip') return unzipGeoStream(input, fs.createReadStream(input), counter, discover, callback); 

    if(ext === '.gdb' || ext === '.shp' || ext === '.vrt' || ext === '.json'){
      counter.incr();
      return callback(null, input, afterProcessingCb);
    }

    if(ext === '.csv') return callback(new Error('.csv files need to be wrapped in .vrt files to get spatial data loaded. Try https://www.npmjs.com/package/csv-to-vrt.'), null, afterProcessingCb);

    if(silent) return;

    return callback(new Error('File type "' + ext + '" unsupported.'), null, afterProcessingCb);
  } 

  return nodedir.files(input, function(err, files){
    if(err) callback(err, null, afterProcessingCb);

    files.forEach(function(file){
      discover(file, counter, callback, 1, afterProcessingCb);
    });
  });

}

module.exports = discover;
