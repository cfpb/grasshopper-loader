'use strict';
var through = require('through2');
var bulkSeparator = new Buffer('{"index":{}}\n');
var sepLen = bulkSeparator.length;

module.exports = function(){
  return through(function(chunk, enc, cb){
    return cb(null, Buffer.concat([bulkSeparator, chunk], sepLen + chunk.length));
  });
};
