'use strict';
var Transform = require('readable-stream/transform');
var inherits = require('inherits');
var us = require('us');

var fipsToAbbr = us.mapping('fips', 'abbr');
var fipsReg = /tl_\d{4}_(\d{2})/;
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
    props.STATE = this.stateAbbr;

    //Elaticsearch bulk wants newline separated values
    this.push(prefix + JSON.stringify(json) + suffix);
    cb();
  }

  function Transformer(file, pre, suf){
    if(!file) throw new Error('Must provide filename to transform tiger data.')
    if(!(this instanceof Transformer)) return new Transformer(file, pre, suf);
    Transform.call(this);
    this.prefix = pre;
    this.suffix = suf;
    this.stateAbbr = fipsToAbbr[file.match(fipsReg)[1]];
    if(!this.stateAbbr) console.log('Unable to intuit state from filename.');
  }

  inherits(Transformer, Transform);
  Transformer.prototype._transform = transform;

  return Transformer;
};
