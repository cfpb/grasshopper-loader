'use strict';
var Transform = require('readable-stream/transform');
var inherits = require('inherits');
var formatAddress = require('./formatAddress');
var resolveFields = require('./resolveFields');
var path = require('path');
var centroid = require('turf-centroid');

var loadDate = Date.now();

//whitelisted geo names for csvs
var lats = ['y', 'latitude', 'lat', 'lat_dd', 'lat_y'];
var lons = ['x', 'longitude', 'long', 'lon', 'lon_dd', 'long_dd', 'lon_x', 'long_x'];


//csv streams contain just the properties object
function getProps(obj){
  if(this.isCSV) return obj;
  return obj.properties;
}


//Return geometry even if not present in parsed chunk
//Search through properties for a possible match, memoizing it if found
//Non-point geometries return their centroid
function getGeometry(props, geo){
  if(this.isCSV) geo = null;

  if(geo){
    if(geo.type === "Point") return geo;
    return centroid(geo).geometry;
  }

  if(this.coordinates.x){
    return {
      "type": "Point",
      "coordinates": [+props[this.coordinates.x], +props[this.coordinates.y]]
    }
  }

  var lowerCase = {};
  Object.keys(props).forEach(function(v){
    lowerCase[v.toLowerCase()] = {key: v, val: props[v]};
  });

  var xMatch = matchGeoFields(lowerCase, lons);
  var yMatch = matchGeoFields(lowerCase, lats);

  this.coordinates.x = xMatch.key;
  this.coordinates.y = yMatch.key;

  return {
    "type": "Point",
    "coordinates": [+xMatch.val, +yMatch.val]
  }

}


//Naively attempt to match x/y coords from whitelisted arrays
function matchGeoFields(props, arr){
  for(var i=0; i<arr.length; i++){
    var match = props[arr[i]];
    if(match) return match;
  }
  throw new Error('File resulted in null geometry with no obvious lat/lon fields detected.');
}


//Individual transformers call the exported function
//Each call saves the passed field names/functions in a closure
//To be used by the created Transform stream's _transform method
module.exports = function(fields){

  //Chunk is a GeoJSON feature
  function transform(chunk, enc, cb){
    var props = this._getProps(chunk);
    var payload;
    var prefix = this.prefix || '';
    var suffix = this.suffix || '';

    try{
      var vals = resolveFields(props, fields);
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
        },
        geometry: this._getGeometry(props, chunk.geometry)
      };

    }catch(e){
      console.log("Couldn't extract the desired properties from %s.", this.fileName, e);
      return cb();
    }

    //Elaticsearch bulk wants newline separated values
    this.push(prefix + JSON.stringify(payload) + suffix);
    cb();
  }


  function Transformer(file, pre, suf){
    if(!(this instanceof Transformer)) return new Transformer(file, pre, suf);

    Transform.call(this, {writableObjectMode: true});

    this.isCSV = path.extname(file) === '.csv';
    this.prefix = pre;
    this.suffix = suf;
    this.fileName = file;
    this.coordinates = {x: '', y: ''};
    this.propertyNames = [];
  }


  inherits(Transformer, Transform);
  Transformer.prototype._transform = transform;
  Transformer.prototype._getGeometry = getGeometry;
  Transformer.prototype._getProps = getProps;

  return Transformer;
};
