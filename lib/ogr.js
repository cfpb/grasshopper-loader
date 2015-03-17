var spawn = require('child_process').spawn;

module.exports = function(file){

  var ogr = spawn('ogr2ogr',
      ['-f', 'GeoJson', '-t_srs', 'WGS84', '/vsistdout/', file]
      );
  ogr.stderr.pipe(process.stdout);

  return ogr.stdout;
};
