'use strict';

var fs = require('fs');
var path = require('path');
var unzip = require('unzip');
var rmrf = require('rimraf');

var getGeoFiles = require('./getGeoFiles');

function unzipGeoStream(zip, stream, callback){
  var dirname = path.join(path.dirname(zip), path.basename(zip, '.zip'));
  fs.mkdir(dirname, function(err){
    if(err) callback(err);
    var unzipped = unzip.Extract({path: dirname});

    //Process unzipped files silently. After loading is complete, delete them.
    unzipped.on('close', getGeoFiles.bind(null, dirname, callback, 1, function(err, cb){
     rmrf(dirname,function(e){
       if(err)callback(err);
       if(e)callback(e);
       if(cb)cb();
     }); 
    }));

    stream.pipe(unzipped)
  }); 
}

module.exports = unzipGeoStream;
