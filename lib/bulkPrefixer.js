'use strict';
var through = require('through2');
var bulkSeparator = new Buffer('{"index":{}}\n');

//Prefix all the JSON documents with a instructions to index
//during a bulk load request
//see https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference.html#api-bulk
module.exports = function(){
  return through(function(chunk, enc, cb){
    var newline = '';

    if(chunk[chunk.length - 1] !== 10) newline = '\n';

    newline = new Buffer(newline);

    return cb(null, Buffer.concat([bulkSeparator, chunk, newline], bulkSeparator.length + chunk.length + newline.length));
  });
};
