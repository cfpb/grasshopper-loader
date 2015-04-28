'use strict';
var aws = require('aws-sdk');
var path = require('path');
var unzipGeoStream = require('./unzipGeoStream');
var getGeoFiles = require('./getGeoFiles');

var folderReg = /\/$/; 

function S3Request(program, counter, callback){
  if(!(this instanceof S3Request)) return new S3Request(program, counter, callback);
  aws.config.credentials = new aws.SharedIniFileCredentials({profile: program.profile||'default'});

  this.S3 = new aws.S3();
  this.bucket = program.bucket;
  this.counter = counter;
  this.callback = callback;

  var key = program.data;

  if(!key){
    this.getAllFiles();
  }else{ 
    //A key with no extension is interpreted as a folder.
    //Used so the foldername doesn't need to be suffixed with / on the command line
    if(!key.match(folderReg) && !path.extname(key)){
      key += '/';
    }

    if(key.match(folderReg)){
      this.getAllFiles(key);
    }else{
      this.getFile(key);
    }
  }
}


S3Request.prototype.getFile = function(key){
  var self = this;
  var req = self.S3.getObject({'Bucket': self.bucket, 'Key': key});
  var stream = req.createReadStream();

  stream.on('error',function(err){
    self.callback(err);
  });
  
  if(path.extname(key) === '.zip'){
    return unzipGeoStream(path.join(__dirname, key), stream, self.counter, getGeoFiles, self.callback);   
  } 
   
  self.counter.incr();

  return self.callback(null, key, stream); 
}


S3Request.prototype.getAllFiles = function(prefix){
  var self = this;
  self.S3.listObjects({'Bucket': self.bucket, 'Prefix': prefix||''}, function(err, res){
    if(err) return self.callback(err);
    res.Contents.forEach(function(v){
      if(!folderReg.match(v.Key)){
        self.getFile(v.Key);
      }
    }); 
 });
}

module.exports = S3Request;
