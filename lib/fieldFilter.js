/*
 * Creates a transform stream with a record in its closure that
 * pulls from streamed data desired properties (as listed in the record).
 */

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

    var number = vals.Number;
    var street = vals.Street;
    var city = vals.City;
    var state = vals.State;
    var zip = vals.Zip;

    if(number === undefined || street === undefined || city === undefined || state === undefined || zip === undefined){
      return cb(new Error('The field mappings provided in the metadata file are not correct. Perhaps the schema has changed.'));
    }

    var address = formatAddress(number, street, city, state, zip);

    if(address === null || chunk.geometry === null){
      logger.info('No valid street name or number for %s.\n\nOriginal fields: %s\n\nResolved fields: %s\n', name, JSON.stringify(props), JSON.stringify(vals));
      return cb(null);
    }

    var payload = {
      type: "Feature",
      properties: {
        address: address,
        number: number + '',
        street: street,
        city: city,
        state: state,
        zip: zip + ''
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
