'use strict';

var fs = require('fs-extra');
var path = require('path');
var unzip = require('unzip');

var scratchSpace = '.';

function unzipGeoStream(zip, stream, counter, geoCb, finalCb){
  var dirname = path.join(scratchSpace, path.basename(zip, '.zip'));

  fs.mkdirs(dirname, function(err){
    if(err) finalCb(err);
    var unzipped = unzip.Extract({path: dirname});

    //Process unzipped files silently.
    unzipped.on('close', geoCb.bind(null, dirname, counter, finalCb, 1));

    stream.pipe(unzipped)
    stream.on('error', function(err){
      stream.unpipe();
      finalCb(err);
    });
  });
}

unzipGeoStream.setScratchSpace = function(scratch){
  scratchSpace= scratch;
}

module.exports = unzipGeoStream;
