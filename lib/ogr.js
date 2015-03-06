var fs = require('fs');
var spawn = require('child_process').spawn;

module.exports = function(shp){

  var ogr = spawn('ogr2ogr',
      ['-f', 'GeoJson', '-t_srs', 'WGS84', '/vsistdout/', shp]
      );

  ogr.stderr.pipe(process.stdout);

  return ogr.stdout;
};
