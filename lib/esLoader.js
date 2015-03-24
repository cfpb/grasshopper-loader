var elasticsearch = require('elasticsearch');
var util = require('util');
var Writable = require('readable-stream/writable');

var client;
var index;
var type;


function writeStream(){
  Writable.call(this);
  this.errors = [];
  this.count = 0;
}


util.inherits(writeStream, Writable);


writeStream.prototype._write = function(chunk, enc, cb){

  client.bulk({
    index: index,
    type: type,
    body: chunk.toString()
  }, trackCount.bind(this, cb));

}


//Bound to instance in constructor
function trackCount(cb, err, res){
    if(err) this.errors.push(err);
    this.count += res.items.length;
    cb();
}


function connect(host, port, newIndex, newType){
  if(!host || !port && port !== 0) throw new Error('Cient connect requires host and port');
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
