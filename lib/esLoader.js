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


function load(client, basename, alias, type){

  if(!client || !basename || !alias || !type){
    throw new Error('Invalid arguments. Need client, basename, alias, type, and callback');
  }

  basename = basename.toLowerCase();

  var stream = new writeStream(client, basename, alias, type);

  stream.on('finish', function(){
    var self = this;

    client.indices.get({index: alias}, function(err, res){

      if(err){
        if(err.message.slice(0, 21) === 'IndexMissingException' && err.message.slice(0, 11) !== 'Bad Request'){
          res = [];
        }else{
          return stream.emit('error', err);
        }
      }

      var internalIndexArr = Object.keys(res);
      var oldIndices = [];

      for(var i=0; i<internalIndexArr.length; i++){
        var index = internalIndexArr[i];
        if(index.split('-').slice(0, -2).join('-') === basename){
          oldIndices.push(index);
        }
      }

      client.indices.putAlias({
        index: self.index,
        name: self.alias
      }, function(err){
        if(err) return stream.emit('error', err);

        if(oldIndices.length){
          client.indices.delete({index: oldIndices.join(',')}, function(err){
            if(err
            && err.message.slice(0, 21) !== 'IndexMissingException'
            && err.message.slice(0, 11) !== 'Bad Request'
            ){
              stream.emit('error', err)
            }else{
              client.close();
              stream.emit('alias');
            }
          });
        }else{
          client.close();
          stream.emit('alias');
        }
      });
    });
  });

  return stream;
}


module.exports = {
  connect: connect,
  load: load
}
