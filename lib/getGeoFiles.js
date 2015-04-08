'use strict';
var fs = require('fs');
var path = require('path');
var nodedir = require('node-dir');

var unzipGeoStream = require('./unzipGeoStream');


function discover(input, callback, silent, afterProcessingCb){
  var ext = path.extname(input);

  if(ext){
    if(ext === '.zip') return unzipGeoStream(input, fs.createReadStream(input), discover, callback); 

    if(ext === '.gdb'|| ext === '.shp'|| ext === '.json')
      return callback(null, input, afterProcessingCb);

    if(silent) return;

    return callback(new Error('File type "' + ext + '" unsupported.'), null, afterProcessingCb);
  } 

  return nodedir.files(input, function(err, files){
    if(err) callback(err);

    files.forEach(function(file){
      discover(file, callback, 1, afterProcessingCb);
    });
  });

}

module.exports = discover;
