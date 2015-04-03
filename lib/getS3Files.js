var aws = require('aws-sdk');



function getS3(program, callback){
  var credentials = new aws.SharedIniFileCredentials({profile: program.profile||'default'});
  aws.config.credentials = credentials; 


}

module.exports = getS3;
