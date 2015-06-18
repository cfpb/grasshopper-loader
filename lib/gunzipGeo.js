'use strict';

var fs = require('fs');
var zlib = require('zlib');
var path = require('path');


function gunzipGeo(file, stream, counter, callback, errback){
  counter.incr();

  var gunzipStream = zlib.createGunzip();

  if(!stream){
    stream = fs.createReadStream(file);
    stream.on('error', function(err){
      if(errback) return errback(err);
      throw err;
    });
  }

  stream.pipe(gunzipStream);

  return callback(null, path.basename(file, '.gz'), gunzipStream, errback);
}

module.exports = gunzipGeo;
