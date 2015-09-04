'use strict';
var through = require('through2');
var winston = require('winston');
var resolveFields = require('./resolveFields');
var formatAddress = require('./formatAddress');

var objectMode = {objectMode: true};
var logger = winston;


function fieldFilter(record){
  var fields = record.fields;
  var name = record.name;
  //Chunk is a GeoJSON feature
  return through(objectMode, function(chunk, enc, cb){

    var props = chunk.properties;

    try{
      var vals = resolveFields(props, fields);
      var payload = {
        type: "Feature",
        properties: {
          address:
            formatAddress(
              vals.Address,
              vals.City,
              vals.State,
              vals.Zip,
              name
            ),
          alt_address: ""
        },
        geometry: chunk.geometry
      }
    }catch(e){
      logger.error(e);
      return cb(null);
    }

    return cb(null, payload);
  });
}

fieldFilter.setLogger = function(newLogger){
  logger = newLogger;
}

module.exports = fieldFilter;
