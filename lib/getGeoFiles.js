'use strict';
var fs = require('fs');
var path = require('path');
var nodedir = require('node-dir');

var gunzipGeo = require('./gunzipGeo');
var unzipGeoStream = require('./unzipGeoStream');


function discover(input, counter, callback, silent){
  var ext = path.extname(input);

  if(ext){

    if(ext === '.gz'){
      return gunzipGeo(input, null, counter, callback);
    }

    if(ext === '.zip'){
      return unzipGeoStream(input, fs.createReadStream(input), counter, discover, callback);
    }

    if(ext === '.json'|| ext === '.csv' || ext === '.gdb' || ext === '.shp'){
      counter.incr();
      return callback(null, input);
    }

    if(silent) return;

    return callback(new Error('File type "' + ext + '" unsupported.'));
  }

  return nodedir.files(input, function(err, files){
    if(err) callback(err);

    files.forEach(function(file){
      discover(file, counter, callback, 1);
    });
  });

}

module.exports = discover;
