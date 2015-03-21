var fs = require('fs');
var path = require('path');
var nodedir = require('node-dir');
var rmrf = require('rimraf');
var callback;


function init(cb){
  callback = cb;
}


function extractGeo(zip){
  var dirname = path.join(path.dirname(zip), path.basename(zip, '.zip'));

  fs.mkdir(dirname, function(err){
    if(err) callback(err);
    var unzipped = unzip.Extract({path: dirname});

    //Process unzipped files. Silently. After loading is complete, delete them.
    unzipped.on('close', discover.bind(null, dirname, 1, function(){
     rmrf(dirname,function(e){if(e)callback(e)}); 
    }));

    fs.createReadStream(zip).pipe(unzipped)
  }); 
}


function discover(input, silent, afterProcessingCb){
  var ext = path.extname(input);

  if(ext){
    if(ext === '.zip') return extractGeo(input); 

    if(ext === '.gdb'|| ext === '.shp'|| ext === '.json')
      return callback(null, input, afterProcessingCb);
   
    if(silent) return;
    
    return callback(new Error('File type "' + ext + '" unsupported.'));
  } 
  
  nodedir.readFiles(input, walkDiscover);
  
}


function walkDiscover(err, file, next){
  if(err) callback(err);
  discover(file);
  next();
}


module.exports = {
  init: init,
  discover: discover
}
