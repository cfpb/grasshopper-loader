'use strict';

var fs = require('fs');
var zlib = require('zlib');


function gunzipGeo(file, stream, callback){

  var gunzipStream = zlib.createGunzip();

  if(!stream){
    stream = fs.createReadStream(file);
    stream.on('error', function(err){
      if(callback) return callback(err);
      throw err;
    });
  }

  return stream.pipe(gunzipStream);
}

module.exports = gunzipGeo;
