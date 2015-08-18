'use strict';
var through = require('through2');

var objectMode = {objectMode: true};

module.exports = function(fields){

  //Chunk is a GeoJSON feature
  return through(objectMode, function(chunk, enc, cb){

    var payload = {
      type: "Feature",
      properties: {},
      geometry: chunk.geometry
    }

    var props = chunk.properties;

    for(var i=0; i<fields.length; i++){
      var field = fields[i];
      var val;

      if(field.type === 'dynamic'){
        try{
          val = new Function('props', field.value)(props); //eslint-disable-line
        }catch(e){
          return cb(null);
        }
      }else{
        val = props[field.value];
      }

      if(!val) return cb(null);

      payload.properties[field.name] = val;
    }

    return cb(null, payload);
  });
};
