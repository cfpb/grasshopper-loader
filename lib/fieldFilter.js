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

    var keys = Object.keys(vals);
    for(var i=0; i<keys.length; i++){
      if(!vals[keys[i]]){
        logger.error('No value found for ' + keys[i]);
        return cb(null);
      }
      payload.properties[keys[i]] = vals[keys[i]];
    }

    return cb(null, payload);
  });
};
