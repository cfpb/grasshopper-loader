var ogr2ogr = require('ogr2ogr');
var fs = require('fs');

ogr2ogr('../test/test.shp')
  .project('WGS84', '../test/test.prj')
  .stream()
  .pipe(process.stdout);
