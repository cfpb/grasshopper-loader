'use strict';
var Transform = require('readable-stream/transform');
var inherits = require('inherits');

var loadDate = Date.now();

module.exports = function(){
  
  //Chunk is a GeoJSON feature
  function transform(chunk, enc, cb){
    var json = JSON.parse(chunk);
    var props = json.properties;
    var prefix = this.prefix || '';
    var suffix = this.suffix || '';

    Object.keys(props).forEach(function(v){
      if(props[v] === null){
        props[v] = "";
      }
    });
    
    props.load_date = loadDate;

    //Elaticsearch bulk wants newline separated values
    this.push(prefix + JSON.stringify(json) + suffix);
    cb();
  }

  function Transformer(pre, suf){
    if(!(this instanceof Transformer)) return new Transformer(pre, suf);
    Transform.call(this);
    this.prefix = pre;
    this.suffix = suf;
  }

  inherits(Transformer, Transform);
  Transformer.prototype._transform = transform;

  return Transformer;
};
