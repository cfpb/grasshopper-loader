'use strict';
var aws = require('aws-sdk');
var path = require('path');
var unzipGeoStream = require('./unzipGeoStream');
var getGeoFiles = require('./getGeoFiles');

var folderReg = /\/$/; 

function getS3(program, counter, callback){
  var credentials = new aws.SharedIniFileCredentials({profile: program.profile||'default'});
  aws.config.credentials = credentials; 
  var S3 = new aws.S3();
  var key = program.data;

  if(!key){
    return getAllFiles(S3, counter, program.bucket, '', callback);
  } 
  
  //A key with no extension is interpreted as a folder.
  //Used so the foldername doesn't need to be suffixed with / on the command line
  if(!key.match(folderReg) && !path.extname(key)){
    key += '/';
  }

  if(key.match(folderReg)){
    return getAllFiles(S3, counter, program.bucket, key, callback);
  }

  return getFile(S3, counter, program.bucket, key, callback);
}


function getFile(S3, counter, bucket, key, callback){
  var req = S3.getObject({'Bucket': bucket, 'Key': key});
  var stream = req.createReadStream();

  stream.on('error',function(err){
    callback(err);
  });
  
  if(path.extname(key) === '.zip'){
    return unzipGeoStream(path.join(__dirname, key), stream, counter, getGeoFiles, callback);   
  } 
   
  counter.incr(); 

  return callback(null, key, stream); 
}


function getAllFiles(S3, counter, bucket, filter, callback){
 S3.listObjects({'Bucket': bucket, 'Prefix': filter}, function(err, res){
   if(err) return callback(err);
   res.Contents.forEach(function(v){
     if(!folderReg.match(v.Key)){
       getFile(S3, counter, bucket, v.Key, callback);
     }
   }); 
 });
}

module.exports = getS3;
