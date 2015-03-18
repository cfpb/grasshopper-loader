var util = require('util');
var Transform = require('readable-stream/transform');

util.inherits(splitOGRJSON, Transform);

var featureSplit = /"features": \[\n/;
var mainSplit = /} },?\n/;
var reform = '} }';
var beforeFeatures = 1;

function splitOGRJSON(){
  Transform.call(this, {readableObjectMode: true});
  this.leftovers = '';
}

splitOGRJSON.prototype._transform = function(chunk, enc, cb){
  var split;
  chunk = this.leftovers + chunk;

  if(beforeFeatures){
    split = chunk.split(featureSplit);
    //Not yet to features
    if(split.length === 1){
      this.leftovers += chunk;
    }else{
      beforeFeatures = 0;
      this.leftovers = split[1];
    }
  }else{
    split = chunk.split(mainSplit);
    var len = split.length - 1;
    for(var i=0; i < len; i++){
      this.push(split[i] + reform);
    }
    this.leftovers = split[i];
  }
  cb();
};

splitOGRJSON.prototype._flush = function(cb){
  var split = this.leftovers.split(mainSplit);
  var len = split.length - 1;
  for(var i=0; i < len; i++){
    this.push(split[i] + reform);
  }
  cb();
}

module.exports = function(){
  return new splitOGRJSON();
};