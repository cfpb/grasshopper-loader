'use strict';
var spawn = require('child_process').spawn;
var pump = require('pump');

module.exports = function(file, stream, s_srs){
  var child;
  var args = ['-f', 'GeoJson', '-t_srs', 'WGS84', '/vsistdout/'];

  if(s_srs){
    args.splice(4, 0, '-s_srs', s_srs);
  }

  if(stream){
    args.push('/vsistdin/');
    child = spawn('ogr2ogr', args);
    pump(stream, child.stdin);
  }else{
    args.push(file);
    child = spawn('ogr2ogr', args);
  }

  return child;
};
