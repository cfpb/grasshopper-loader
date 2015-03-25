var fs = require('fs');
var path = require('path');
var unzip = require('unzip');
var nodedir = require('node-dir');
var rmrf = require('rimraf');
var callback;


function extractGeo(zip, callback){
  var dirname = path.join(path.dirname(zip), path.basename(zip, '.zip'));
  fs.mkdir(dirname, function(err){
    if(err) callback(err);
    var unzipped = unzip.Extract({path: dirname});

    //Process unzipped files. Silently. After loading is complete, delete them.
    unzipped.on('close', discover.bind(null, dirname, callback, 1, function(err){
     rmrf(dirname,function(e){
       if(err)callback(err);
       if(e)callback(e);
     }); 
    }));

    fs.createReadStream(zip).pipe(unzipped)
  }); 
}


function discover(input, callback, silent, afterProcessingCb){
  var ext = path.extname(input);

  if(ext){
    if(ext === '.zip') return extractGeo(input, callback); 

    if(ext === '.gdb'|| ext === '.shp'|| ext === '.json')
      return callback(null, input, afterProcessingCb);

    if(silent) return;

    return callback(new Error('File type "' + ext + '" unsupported.'), null, afterProcessingCb);
  } 

  return nodedir.files(input, function(err, files){
    if(err) callback(err);
    files.forEach(function(file){
      discover(file, callback, 1, afterProcessingCb);
    });
  });

}

module.exports = discover;
