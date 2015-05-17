'use strict';
var aws = require('aws-sdk');
var path = require('path');
var unzipGeoStream = require('./unzipGeoStream');
var getGeoFiles = require('./getGeoFiles');

var folderReg = /\/$/; 
var makeRequest;

function getS3(program, counter, env, callback){
  var credentials;

  if(env.AWS_ACCESS_KEY_ID){
    credentials = new aws.EnvironmentCredentials('AWS');
  }else{
    credentials = new aws.SharedIniFileCredentials({profile: program.profile});
  }

  aws.config.credentials = credentials; 

  var S3 = new aws.S3();
  
  //If no environment variables are passed
  //And no ~/.aws/credentials file is present
  //Fall back to unauthenticated request
  if(credentials.accessKeyId){
    makeRequest = S3.makeRequest.bind(S3);
  }else{
    makeRequest= S3.makeUnauthenticatedRequest.bind(S3);
  }

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
  var req = makeRequest('getObject', {'Bucket': bucket, 'Key': key});
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
  makeRequest('listObjects', {'Bucket': bucket, 'Prefix': filter}, function(err, res){
   if(err) return callback(err);
   res.Contents.forEach(function(v){
     if(!v.Key.match(folderReg)){
       getFile(S3, counter, bucket, v.Key, callback);
     }
   }); 
 });
}

module.exports = getS3;
