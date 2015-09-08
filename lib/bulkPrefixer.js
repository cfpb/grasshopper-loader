'use strict';
var through = require('through2');
var bulkSeparator = new Buffer('{"index":{}}\n');
var sepWithPrefix = new Buffer('\n{"index":{}}\n');

//Prefix all the JSON documents with a instructions to index
//during a bulk load request
//see https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-bulk
module.exports = function(){
  return through(function(chunk, enc, cb){
    var chunkLen = chunk.length;
    var sep;

    if(chunk[chunkLen - 1] === '0a') sep = bulkSeparator;
    else sep = sepWithPrefix;

    return cb(null, Buffer.concat([sep, chunk], sep.length + chunk.length));
  });
};
