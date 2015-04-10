'use strict';

var path = require('path');
var mkdirp = require('mkdirp');
var unzip = require('unzip');
var rmrf = require('rimraf');


function unzipGeoStream(zip, stream, counter, geoCb, finalCb){
  var dirname = path.join(path.dirname(zip), Math.round(Math.random()*1e15).toString(), path.basename(zip, '.zip'));

  mkdirp(dirname, function(err){
    if(err) finalCb(err);
    var unzipped = unzip.Extract({path: dirname});
    
    //Process unzipped files silently. After loading is complete, delete them.
    unzipped.on('close', geoCb.bind(null, dirname, counter, finalCb, 1, function(err, cb){
      rmrf(path.dirname(dirname),function(e){
        if(err)finalCb(err);
        if(e)finalCb(e);
        if(cb)cb();
      }); 
    }));

    stream.pipe(unzipped)
  }); 
}

module.exports = unzipGeoStream;
