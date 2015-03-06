var util = require('util');
var Transform = require('stream').Transform;

util.inherits(splitOGRJSON, Transform);

var featureSplit = /"features": \[\n/;
var mainSplit = /\n,/;
var beforeFeatures = 1;

function splitOGRJSON(){
  Transform.call(this, {readableObjectMode: true});
  this.leftovers = '';
}

splitOGRJSON.prototype._transform = function(chunk, enc, cb){
  chunk = this.leftovers + chunk;

  if(beforeFeatures){
    var split = chunk.split(featureSplit);
    //Not yet to features
    if(split.length === 1){
      this.leftovers += chunk;
    }else{
      beforeFeatures = 0;
      this.leftovers = split[1];
    }
  }else{
    parseFeatures(chunk);
  }
  cb()


};
module.exports = function(){

};
