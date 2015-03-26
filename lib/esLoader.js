var elasticsearch = require('elasticsearch');
var util = require('util');
var Writable = require('readable-stream/writable');


function writeStream(client, index, type){
  Writable.call(this);
  this.client = client;
  this.index = index;
  this.type = type;
  this.errors = [];
  this.count = 0;
}


util.inherits(writeStream, Writable);


writeStream.prototype._write = function(chunk, enc, cb){

  this.client.bulk({
    index: this.index,
    type: this.type,
    body: chunk.toString()
  }, trackCount.bind(this, cb));

}


//Bound to instance in constructor
function trackCount(cb, err, res){
    if(err) this.errors.push(err);
    if(res) this.count += res.items.length;
    cb();
}


function connect(host, port, log){
  if(!host || !port && port !== 0) throw new Error('Cient connect requires host and port');
  return new elasticsearch.Client({
		  host: host + ":" + port,
			log: log || 'debug'
	});
}


function load(client, index, type){
  return new writeStream(client, index, type);
}


module.exports = {
  connect: connect,
  load: load  
}
