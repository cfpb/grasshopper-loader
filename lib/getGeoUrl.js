'use strict';
var path = require('path');
var request = require('request');

var unzipGeoStream = require('./unzipGeoStream');
var getGeoFiles = require('./getGeoFiles');

function getFileFromUrl(url, counter, callback){
  var stream = request.get(url);
  var basename = path.basename(url);
  var ext = path.extname(url);

  stream.on('error', function(err){
    callback(err);
  });

  if(ext === '.zip'){
    return unzipGeoStream(path.join(__dirname, basename), stream, counter, getGeoFiles, callback); 
  }

  counter.incr();
  return callback(null, basename, stream);

}

module.exports = getFileFromUrl;
