'use strict';
var spawn = require('child_process').spawn;

module.exports = function(file, stream){
  var child;

  if(stream){
    child = spawn('ogr2ogr', ['-f', 'GeoJson', '-t_srs', 'WGS84', '/vsistdout/', '/vsistdin/']);
    stream.pipe(child.stdin);
  }else{
    child = spawn('ogr2ogr', ['-f', 'GeoJson', '-t_srs', 'WGS84', '/vsistdout/', file]);
  }

  return child;
};
