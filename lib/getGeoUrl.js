'use strict';
var url = require('url');
var path = require('path');
var request = require('request');

var unzipGeoStream = require('./unzipGeoStream');
var getGeoFiles = require('./getGeoFiles');
var gunzipGeo = require('./gunzipGeo');

function getFileFromUrl(urlPath, counter, callback){
  var pathname = url.parse(urlPath).pathname;
  var basename = path.basename(pathname);
  var ext = path.extname(pathname);

  var stream = request.get(urlPath);

  stream.on('error', function(err){
    callback(err);
  });

  if(ext === '.zip'){
    return unzipGeoStream(path.join(__dirname, basename), stream, counter, getGeoFiles, callback);
  }

  if(ext === '.gzip'){
    stream = gunzipGeo(basename, stream);
  }

  counter.incr();
  return callback(null, basename, stream);

}

module.exports = getFileFromUrl;
