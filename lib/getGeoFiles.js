'use strict';
var fs = require('fs');
var zlib = require('zlib');
var path = require('path');
var nodedir = require('node-dir');

var unzipGeoStream = require('./unzipGeoStream');


function discover(input, counter, callback, silent, afterProcessingCb){
  var ext = path.extname(input);
  var stream;
  var gunzipStream;

  if(ext){

    if(ext === '.gzip'){
      counter.incr()

      stream = fs.createReadStream(input);
      gunzipStream = zlib.createGunzip();

      stream.on('error', function(err){
        callback(err, null, afterProcessingCb);
      });

      stream.pipe(gunzipStream);

      return callback(null, input, gunzipStream, afterProcessingCb);
    }

    if(ext === '.zip'){
      return unzipGeoStream(input, fs.createReadStream(input), counter, discover, callback);
    }

    if(ext === '.json'|| ext === '.csv' || ext === '.gdb' || ext === '.shp'){
      counter.incr();
      return callback(null, input, afterProcessingCb);
    }

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
