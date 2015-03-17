var elasticsearch = require('elasticsearch');
var util = require('util');
var Writable = require('readable-stream/writable');

var client;
var index;
var type;


function writeStream(){
  Writable.call(this);
}


util.inherits(writeStream, Writable);


writeStream.prototype._write = function(chunk, enc, cb){

  client.bulk({
    index: index,
    type: type,
    body: chunk.toString()
  });

  cb();
}


function connect(host, port, newIndex, newType){
  client = new elasticsearch.Client({
		  host: host + ":" + port,
			log: 'debug'
	});
  index = newIndex;
  type = newType;
}


function load(){
  return new writeStream();
}


module.exports = {
  connect: connect,
  load: load  
}
