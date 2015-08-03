'use strict';

var fs = require('fs');
var zlib = require('zlib');
var path = require('path');


function gunzipGeo(file, stream, counter, callback){
  counter.incr();

  var gunzipStream = zlib.createGunzip();

  if(!stream){
    stream = fs.createReadStream(file);
    stream.on('error', callback);
  }

  stream.pipe(gunzipStream);

  return callback(null, path.basename(file, '.gz'), gunzipStream);
}

module.exports = gunzipGeo;
