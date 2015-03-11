var elasticsearch = require('elasticsearch');
var util = require('util');
var Writable = require('stream').Writable;

var client;
var metaObj = {
  _index: 'address',
  _type: 'point'
};

function writeStream(){
  Writable.call(this);
}

util.inherits(writeStream, Writable);

writeStream.prototype._write = function(chunk, enc, cb){
  client.create({
    index: index,
    type: type,
    body: chunk.toString()
  });
  cb();
}

function connect(host, port, newType){
  client = new elasticsearch.Client({
		  host: host + ":" + port,
			log: 'debug'
	});
  if(newType) metaObj._type = newType;
}

function load(){
  return new writeStream();
}

module.exports = {
  connect: connect,
  load: load  
}
