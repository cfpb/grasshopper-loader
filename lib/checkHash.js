'use strict';
var crypto = require('crypto');
var eos = require('end-of-stream');

module.exports = function(stream, hash, cb){
  var shasum = crypto.createHash('sha256');

  stream.on('data', function(data){
    shasum.update(data);
  });

  eos(stream, function(){
    var remoteHash = shasum.digest('hex');
    cb(remoteHash === hash, remoteHash);
  });
};
