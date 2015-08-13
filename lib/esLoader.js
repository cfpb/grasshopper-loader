'use strict';
var crypto = require('crypto');
var elasticsearch = require('elasticsearch');
var util = require('util');
var Writable = require('readable-stream/writable');


function writeStream(client, basename, alias, type){
  Writable.call(this);
  this.client = client;
  this.index = basename.toLowerCase() + '-' + Date.now() + '-' + crypto.pseudoRandomBytes(6).toString('hex');
  this.alias = alias;
  this.type = type;
  this.oldIndex = null;
  this.errors = [];
  this.count = 0;
}


util.inherits(writeStream, Writable);


writeStream.prototype._write = function(chunk, enc, cb){

  this.client.bulk({
    index: this.index,
    type: this.type,
    body: chunk.toString()
  }, trackCounter(this, cb));

}


function trackCounter(thisArg, cb){
  return function(err, res){
    if(err) thisArg.errors.push(err);
    if(res) thisArg.count += res.items.length;
    cb();
  }
}


function connect(host, port, log){
  if(!host || !port && port !== 0) throw new Error('Cient connect requires host and port');
  return new elasticsearch.Client({
    host: host + ":" + port,
		log: log || 'debug'
	});
}


function load(client, basename, alias, type, cb){

  if(!client || !basename || !alias || !type){
    var err = new Error('Invalid arguments. Need client, basename, alias, type, and callback');
    if(cb) return cb(err);
    throw err;
  }

  basename = basename.toLowerCase();

  client.indices.get({index: alias}, function(err, res){

    if(err){
      if(err.message.slice(0, 21) === 'IndexMissingException'){
        res = [];
      }else{
        return cb(err);
      }
    }

    var stream = new writeStream(client, basename, alias, type);

    stream.on('finish', function(){
      var self = this;
      client.indices.putAlias({
        index: this.index,
        name: this.alias
      }, function(err){
        if(err) stream.emit('error', err);
        if(self.oldIndex){
          client.indices.delete({index: self.oldIndex}, function(err){
            if(err && err.message.slice(0, 21) !== 'IndexMissingException'){
              stream.emit('error', err)
            }else{
              stream.emit('alias');
            }
          });
        }else{
          stream.emit('alias');
        }
      });
    });


    var internalIndexArr = Object.keys(res);

    for(var i=0; i<internalIndexArr.length; i++){
      var index = internalIndexArr[i];
      if(index.split('-').slice(0, -2).join('-') === basename){
        stream.oldIndex = index;
        break;
      }
    }

    cb(null, stream);

  });
}


module.exports = {
  connect: connect,
  load: load
}
