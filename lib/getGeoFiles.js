var fs = require('fs');
var path = require('path');
var nodedir = require('node-dir');
var rmrf = require('rimraf');
var callback;

function extractGeo(zip){
  var dirname = path.join(path.dirname(zip), path.basename(zip, '.zip'));

  fs.mkdir(dirname, function(err){
    if(err) callback(err);
    var unzipped = unzip.Extract({path: dirname});

    //Process unzipped files. After loading is complete, delete them.
    unzipped.on('close', process.bind(null, dirname, function(){
     rmrf(dirname,function(e){if(e)callback(e)}); 
    }));

    fs.createReadStream(zip).pipe(unzipped)
  }); 
}

function process(input, cb){
  var ext = path.extname(input);

  if(ext){
    if (ext === '.zip'){
      return extractGeo(input); 
    } 
  } 




  
}

module.exports = {
  init: init,
  process: process
}


