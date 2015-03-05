var spawn = require('child_process').spawn;

module.exports = function(shp){
  var ogr = spawn('ogr2ogr',
      ['-f', 'GeoJson', '-t_srs', 'WGS84', '/vsistdout/', shp]
      );
  ogr.on('error',function(err){
    console.log(err, 'Error in OGR transformation with %s shapefile', shp);
  }); 

  return ogr.stdout;
};
