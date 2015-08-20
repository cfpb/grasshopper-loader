'use strict';
var through = require('through2');
var winston = require('winston');
var resolveFields = require('./resolveFields');

var objectMode = {objectMode: true};

module.exports = function(fields, logger){
  if(!logger) logger = winston;

  //Chunk is a GeoJSON feature
  return through(objectMode, function(chunk, enc, cb){

    var payload = {
      type: "Feature",
      properties: {},
      geometry: chunk.geometry
    }

    var props = chunk.properties;

    try{
      var vals = resolveFields(props, fields);
    }catch(e){
      logger.error(e);
      return cb(null);
    }

    for(var i=0; i<vals.length; i++){
      if(!vals[i]){
        logger.error('No value found for ' + fields[i].name);
        return cb(null);
      }
      payload.properties[fields[i].name] = vals[i];
    }

    return cb(null, payload);
  });
};
