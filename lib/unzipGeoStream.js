'use strict';

var fs = require('fs');
var path = require('path');
var unzip = require('unzip');
var rmrf = require('rimraf');


function unzipGeoStream(zip, stream, geoCb, finalCb){
  var dirname = path.join(path.dirname(zip), Math.round(Math.random()*1e15) + path.basename(zip, '.zip'));

  fs.mkdir(dirname, function(err){
    if(err) finalCb(err);
    var unzipped = unzip.Extract({path: dirname});
    
    //Process unzipped files silently. After loading is complete, delete them.
    unzipped.on('close', geoCb.bind(null, dirname, finalCb, 1, function(err, cb){
      rmrf(dirname,function(e){
        if(err)finalCb(err);
        if(e)finalCb(e);
        if(cb)cb();
      }); 
    }));

    stream.pipe(unzipped)
  }); 
}

module.exports = unzipGeoStream;
