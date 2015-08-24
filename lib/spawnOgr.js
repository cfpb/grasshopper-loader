var pump = require('pump');
var OgrJsonStream = require('ogr-json-stream');
var centroidStream = require('centroid-stream');
var fieldFilter = require('./fieldFilter');
var ogrChild = require('./ogrChild');

function spawnOgr(record, file, stream){

    var jsonChild = ogrChild(file, stream, record.spatial_reference);
    var centroids = centroidStream.stringify();

    jsonChild.stderr.on('data', function(errorText){
      jsonChild.stdout.emit('error', new Error(errorText));
    });

    pump(jsonChild.stdout, OgrJsonStream(), fieldFilter(record.fields), centroids, function(err){
      if(err) centroids.emit('error', err);
    });



    return centroids;
  }

module.exports = spawnOgr;
