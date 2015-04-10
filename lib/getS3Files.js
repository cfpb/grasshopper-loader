'use strict';
var aws = require('aws-sdk');
var path = require('path');
var unzipGeoStream = require('./unzipGeoStream');
var getGeoFiles = require('./getGeoFiles');

function getS3(program, counter, callback){
  var credentials = new aws.SharedIniFileCredentials({profile: program.profile||'default'});
  aws.config.credentials = credentials; 
  var S3 = new aws.S3();

  if(!program.data){
    return getAllFiles(S3, counter, program.bucket, callback);
  } 
  return getFile(S3, counter, program.bucket, program.data, callback);
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


function getAllFiles(S3, counter, bucket, callback){
 //list contents, for each key getFile 
 S3.listObjects({'Bucket': bucket}, function(err, res){
   if(err) return callback(err);
   res.Contents.forEach(function(v){
     getFile(S3, counter, bucket, v.Key, callback);
   }); 
 });
}

module.exports = getS3;
