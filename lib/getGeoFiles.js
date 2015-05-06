'use strict';
var fs = require('fs');
var path = require('path');
var nodedir = require('node-dir');
var csvToVrt = require('csv-to-vrt');

var unzipGeoStream = require('./unzipGeoStream');


function discover(input, counter, callback, silent, afterProcessingCb){
  var ext = path.extname(input);

  if(ext){
    if(ext === '.zip') return unzipGeoStream(input, fs.createReadStream(input), counter, discover, callback); 

    if(ext === '.gdb' || ext === '.shp' || ext === '.json' || ext === '.vrt'){
      counter.incr();
      return callback(null, input, afterProcessingCb);
    }

    if(ext === '.csv'){
      //Assuming loaded csv's will be in NAD83
      return csvToVrt(input, 'NAD83', function(err, vrt){
        if(err) callback(err, null, afterProcessingCb);
        return callback(null, vrt, function(){
          var afterArgs = arguments;
          fs.unlink(vrt, function(err){
            if(err) return callback(err, null, afterProcessingCb);
            if(afterProcessingCb) afterProcessingCb.apply(null, afterArgs); 
          })
        }) 
      })
    }

    if(silent) return;

    return callback(new Error('File type "' + ext + '" unsupported.'), null, afterProcessingCb);
  } 

  return nodedir.files(input, function(err, files){
    if(err) callback(err, null, afterProcessingCb);

    files.forEach(function(file){
      discover(file, counter, callback, 1, afterProcessingCb);
    });
  });

}

module.exports = discover;
