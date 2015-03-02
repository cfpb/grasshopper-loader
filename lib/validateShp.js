var fs = require('fs');
var path = require('path');

var async = require('async');

var filesExist = require('./filesExist');
var dirMatch = require('./dirMatch');

module.exports = function(input, cb){
  var ext = path.extname(input);

  if(ext){
    filesExist(completeShp(input), cb);  
  }else{
    fs.stat(input, function(err, stats){
      if(err){
        console.log("Stat failed");
        cb(err); 
      }
      
      if(stats.isDirectory()){
        dirMatch(input, /\.shp$/, function(err, shapefiles){
          async.forEach(shapefiles.map(completeShp), filesExist, cb);
        });
      }


    });
  }

}

var suffixes = ['.shp', '.shx', '.dbf', '.prj'];

function completeShp(file){
  var ext = path.extname(file); 
  var base = path.basename(file, ext);
  var dir = path.dirname(file);

  return suffixes.map(function(suffix){
    return path.join(dir, base + suffix);
  }); 
}
