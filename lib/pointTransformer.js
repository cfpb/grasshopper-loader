'use strict';
var Transform = require('readable-stream/transform');
var inherits = require('inherits');
var formatAddress = require('../lib/formatAddress');

var loadDate = Date.now();

module.exports = function(addr, city, state, zip){
  if(!addr) throw new Error('Must provide address at minimum.');
  var args = [addr, city, state, zip];

  //Chunk is a GeoJSON feature
  function transform(chunk, enc, cb){
    chunk = JSON.parse(chunk);
    var props = chunk.properties;
    var payload; 
    var prefix = this.prefix || '';
    var suffix = this.suffix || '';

    try{ 
      
      var vals = args.map(function(arg){
        if(typeof arg === 'function') return arg(props);
        return props[arg];
      });

      payload = {
        type: "Feature",
        properties: {
          address: formatAddress(
                       vals[0],
                       vals[1],
                       vals[2],
                       vals[3]
                       ),
          alt_address: "",
          load_date: loadDate
          //Source<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        },
        geometry: {
          type: "Point",
          coordinates: chunk.geometry.coordinates
        }
      };

    }catch(e){
      //possibly log the error
      return cb();
    }
    //Elaticsearch bulk wants newline separated values
    this.push(prefix + JSON.stringify(payload) + suffix);
    cb();
  }

  function Transformer(file, pre, suf){
    if(!(this instanceof Transformer)) return new Transformer(file, pre, suf);
    Transform.call(this);
    this.prefix = pre;
    this.suffix = suf;
  }

  inherits(Transformer, Transform);
  Transformer.prototype._transform = transform;

  return Transformer;
};
