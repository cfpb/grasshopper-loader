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
    }catch(e){
      logger.error(e);
      return cb(null);
    }

    var address = formatAddress(vals.Address, vals.City, vals.State, vals.Zip);

    if(address === null){
      logger.info('No valid address for %s.\n\nOriginal fields: %s\n\nResolved fields: %s\n', name, JSON.stringify(props), JSON.stringify(vals));
      return cb(null);
    }

    var payload = {
      type: "Feature",
      properties: {
        address: address,
        alt_address: ""
      },
      geometry: chunk.geometry
    }

    return cb(null, payload);
  });
}

fieldFilter.setLogger = function(newLogger){
  logger = newLogger;
}

module.exports = fieldFilter;
