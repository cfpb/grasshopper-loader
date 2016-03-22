'use strict';
var spawn = require('child_process').spawn;
var pump = require('pump');

module.exports = function(file, stream, record){
  var child;
  if(!record) record = {};
  var args = ['-f', 'GeoJson', '-t_srs', 'WGS84', '/vsistdout/'];

  if(record.spatialReference){
    args.splice(4, 0, '-s_srs', record.spatialReference);
  }

  if(stream){
    args.push('/vsistdin/');
    if(record.layerName) args.push(record.layerName);
    child = spawn('ogr2ogr', args);
    pump(stream, child.stdin);
  }else{
    args.push(file);
    if(record.layerName) args.push(record.layerName);
    child = spawn('ogr2ogr', args);
  }

  return child;
};
