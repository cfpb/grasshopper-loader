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


function isIndexException(err){
  var message = err.message;
  if(message.indexOf('IndexMissingException') > -1 ||
     message.indexOf('index_not_found_exception') > -1){
    return true;
  }else{
    return false;
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


function load(options, basename, callback){

  var client = options.client;
  var alias = options.alias;
  var type = options.type;

  if(!client || !basename || !alias || !type){
    return callback(new Error('Invalid arguments. Need client, basename, alias, type, and callback'));
  }


  createIndex(options, basename, function(err, index){
    if(err) return callback(err);

    var stream = new writeStream(client, index, alias, type);

    stream.on('finish', function(){
      applyAlias(options, index, function(err){
        if(err) return stream.emit('error', err);
        return stream.emit('loaded');
      })
    });

    return callback(null, stream);
  });
}


function loadIntoIndex(options, callback){

  var client = options.client;
  var alias = options.alias;
  var index = options.forcedIndex;
  var type = options.type;

  if(!client || !index || !alias || !type){
    return callback(new Error('Invalid arguments. Need client, forcedIndex, alias, type, and callback'));
  }

  var stream = new writeStream(client, index, alias, type);

  stream.on('finish', function(){
    this.emit('loaded');
  });

  return callback(null, stream);
}


function applyAlias(options, index, callback){
  var client = options.client;
  var alias = options.alias;
  var basename = getBasename(index);

  client.indices.get({index: alias}, function(err, res){
    if(err){
      if(isIndexException(err)){
        res = {};
      }else{
        return callback(err);
      }
    }

    var internalIndexArr = Object.keys(res);
    var oldIndices = [];

    for(var i=0; i<internalIndexArr.length; i++){
      var internalIndex = internalIndexArr[i];
      if(getBasename(internalIndex) === basename){
        oldIndices.push(internalIndex);
      }
    }

    client.indices.putAlias({
      index: index,
      name: alias
    }, function(err){
      if(err) return callback(err);

      if(oldIndices.length){
        client.indices.delete({index: oldIndices.join(',')}, function(err){
          if(err && !isIndexException(err)){
            return callback(err);
          }else{
           callback(null);
          }
        });
      }else{
        callback(null);
      }
    });
  });
}


function getBasename(index){
  return index.split('-').slice(0, -2).join('-');
}


module.exports = {
  connect: connect,
  load: load,
  loadIntoIndex: loadIntoIndex,
  applyAlias: applyAlias
}
