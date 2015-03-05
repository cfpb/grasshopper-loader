var elasticsearch = require('elasticsearch');
var util = require('util');
var Writable = require('stream').Writable;

var client;
var index = 'address';
var type = 'point';

function writeStream(){
  Writable.call(this);
}

writeStream.prototype._write = function(chunk, enc, cb){
  client.create({
    index: index,
    type: type,
    body: chunk
  });
  cb();
}

util.inherits(writeStream, Writable);

function connect(host, port, newType){
  client = new elasticsearch.Client({
		  host: host + ":" + port,
			log: 'debug'
	});
  if(newType) type = newType;
}

function load(){
  return new writeStream();
}

module.exports = {
  connect: connect,
  load: load  
}
