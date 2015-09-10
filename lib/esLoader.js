'use strict';
var elasticsearch = require('elasticsearch');
var util = require('util');
var Writable = require('readable-stream/writable');
var createIndex = require('./createIndex');


function writeStream(client, index, alias, type){
  Writable.call(this);
  this.client = client;
  this.index = index;
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
    host: host + ':' + port,
		log: log || 'error',
    requestTimeout: 120000,
    pingTimeout: 30000,
    maxSockets: 64
	});
}


function load(options, client, basename, callback){

  var alias = options.alias;
  var type = options.type;

  if(!client || !basename || !alias || !type){
    return callback(new Error('Invalid arguments. Need client, basename, alias, type, and callback'));
  }


  createIndex(options, client, basename, function(err, index){
    if(err) return callback(err);

    var stream = new writeStream(client, index, alias, type);

    stream.on('finish', function(){
      var self = this;
      client.indices.get({index: alias}, function(err, res){
        if(err){
          if(err.message.indexOf('IndexMissingException') > -1){
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
              if(err && err.message.indexOf('IndexMissingException') === -1){
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

    callback(null, stream);
  });
}


module.exports = {
  connect: connect,
  load: load
}
