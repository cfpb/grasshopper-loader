'use strict';
var util = require('util');
var Transform = require('readable-stream/transform');

util.inherits(parseOGRJSON, Transform);

var mainSplit = / },?\n/;
var featureSplit = /"features": \[\n/;
var reform = ' }';

function parseOGRJSON(){
  Transform.call(this, {readableObjectMode: true});
  this.leftovers = '';
  this.beforeFeatures = 1;
}

parseOGRJSON.prototype._transform = function(chunk, enc, cb){
  var split;
  chunk = this.leftovers + chunk;
  if(this.beforeFeatures){
    split = chunk.split(featureSplit);
    //Not yet to features
    if(split.length === 1){
      this.leftovers += chunk;
    }else{
      this.beforeFeatures = 0;
      this.leftovers = split[1];
    }
  }else{
    split = chunk.split(mainSplit);
    var len = split.length - 1;
    var json;
    for(var i=0; i < len; i++){
      try{
        json = JSON.parse(split[i] + reform);
      }catch(e){
        return cb(e);
      }
      this.push(json);
    }
    this.leftovers = split[i];
  }
  cb();
};

parseOGRJSON.prototype._flush = function(cb){
  var split = this.leftovers.split(mainSplit);
  var len = split.length - 1;
  var json;
  for(var i=0; i < len; i++){
    try{
      JSON.parse(split[i] + reform);
    }catch(e){
      return cb(e)
    }
    this.push(json);
  }
  cb();
}

module.exports = function(){
  return new parseOGRJSON();
};
