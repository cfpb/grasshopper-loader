'use strict';
var Transform = require('readable-stream/transform');
var inherits = require('inherits');
var formatAddress = require('../lib/formatAddress');

var loadDate = Date.now();

var lats = [
  'y',
  'latitude',
  'lat',
  'lat_dd',
  'lat_y'
  ];

var lons = [
  'x',
  'longitude',
  'long',
  'lon',
  'lon_dd',
  'long_dd',
  'lon_x',
  'long_x'
];


function getGeometry(props, geo){
  if(geo !== null) return geo;

  if(this.coordinates.x){
    return {
      "type":"Point",
      "coordinates":[+props[this.coordinates.x], +props[this.coordinates.y]]
    }
  }

  var lowerCase = {};
  Object.keys(props).forEach(function(v){
    lowerCase[v.toLowerCase()] = {key: v, val: props[v]};
  });

  var xMatch = matchFromArr(lowerCase, lons);
  var yMatch = matchFromArr(lowerCase, lats);
  
  this.coordinates.x = xMatch.key;
  this.coordinates.y = yMatch.key

  return {
    "type":"Point",
    "coordinates":[+xMatch.val, +yMatch.val]
  }

}


function matchFromArr(props, arr){
  for(var i=0; i<arr.length; i++){
    var match = props[arr[i]];
    if(match) return match;
  }
  throw new Error('File resulted in null geometry will no obvious lat/lon fields detected.');
}


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
        geometry: this._getGeometry(chunk.properties, chunk.geometry)
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
    this.coordinates = {x:'', y:''}
  }

  inherits(Transformer, Transform);
  Transformer.prototype._transform = transform;
  Transformer.prototype._getGeometry = getGeometry;

  return Transformer;
};
