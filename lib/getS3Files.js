var aws = require('aws-sdk');

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
  var req = S3.getObject({'Bucket':program.bucket, 'Key': program.data});
  var stream = req.createReadStream();

  stream.on('error',function(err){
    callback(err);
  });
  
  return stream; 

}


function getAllFiles(S3, program, callback){

}

module.exports = getS3;
