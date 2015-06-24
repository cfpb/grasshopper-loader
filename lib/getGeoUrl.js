'use strict';
var url = require('url');
var path = require('path');
var hyperquest = require('hyperquest');

var unzipGeoStream = require('./unzipGeoStream');
var getGeoFiles = require('./getGeoFiles');
var gunzipGeo = require('./gunzipGeo');

function getFileFromUrl(urlPath, counter, callback){
  var pathname = url.parse(urlPath).pathname;
  var basename = path.basename(pathname);
  var ext = path.extname(pathname);

  var stream = hyperquest(urlPath);

  stream.on('error', function(err){
    callback(err);
  });

  if(ext === '.zip'){
    return unzipGeoStream(basename, stream, counter, getGeoFiles, callback);
  }

  if(ext === '.gz'){
    return gunzipGeo(basename, stream, counter, callback);
  }

  counter.incr();
  return callback(null, basename, stream);

}

module.exports = getFileFromUrl;
