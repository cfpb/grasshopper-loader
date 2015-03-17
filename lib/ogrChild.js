var spawn = require('child_process').spawn;

module.exports = function(file){

  return spawn('ogr2ogr',
    ['-f', 'GeoJson', '-t_srs', 'WGS84', '/vsistdout/', file]
  );
};
