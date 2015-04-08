'use strict';
var aws = require('aws-sdk');
var path = require('path');
var unzipGeoStream = require('./unzipGeoStream');
var getGeoFiles = require('./getGeoFiles');

function getS3(program, callback){
  var credentials = new aws.SharedIniFileCredentials({profile: program.profile||'default'});
  aws.config.credentials = credentials; 
  var S3 = new aws.S3();

  if(!program.data){
    return getAllFiles(S3, program, callback);
  } 
  return getFile(S3, program, callback);
}


function getFile(S3, program, callback){
  var key = program.data;
  var req = S3.getObject({'Bucket':program.bucket, 'Key': key});
  var stream = req.createReadStream();

  stream.on('error',function(err){
    callback(err);
  });
  
  if(path.extname(key) === '.zip'){
    return unzipGeoStream(path.join(__dirname, key), stream, getGeoFiles, callback);   
  } 

  return callback(null, key, stream); 
}


function getAllFiles(S3, program, callback){

}

module.exports = getS3;
