'use strict';
var elasticsearch = require('elasticsearch');
var util = require('util');
var Writable = require('readable-stream/writable');


function writeStream(client, index, type){
  Writable.call(this);
  this.client = client;
  this.canonicalIndex = index;
  this.internalIndex = Math.round(Math.random()*1e15).toString() + Math.round(Math.random()*1e15).toString();
  this.type = type;
  this.errors = [];
  this.count = 0;
}


util.inherits(writeStream, Writable);


writeStream.prototype._write = function(chunk, enc, cb){

  this.client.bulk({
    index: this.internalIndex,
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


function load(client, index, type, cb){
  var stream = new writeStream(client, index, type);

  stream.on('finish', function(){
    var self = this;
    wipe(client, index, type, function(err){
      if(err) cb(err);
      client.indices.putAlias({
        index:self.internalIndex,
        name: self.canonicalIndex
      }, cb.bind(self));
    });
  });

  return stream;
}


function wipe(client, index, type, cb){

  client.indices.existsAlias({name:index, type:type}, function(err, exists){
    if(err) return cb(err);
    if(exists){
      client.indices.getAlias({name:index, type:type}, function(err, indexes){
        if(err) return cb(err);
        var indexArr = Object.keys(indexes);
        console.log(indexArr, "deleting these");
        client.indices.delete({index:indexArr}, cb);
      })
    }else{
      console.log("NO %s/%s",index,type);
      cb(null);
    }
  });
}


module.exports = {
  connect: connect,
  load: load,
  wipe: wipe
}
