'use strict';

module.exports = function(options, env){
  var messages = [];
  var err = 0;

  if(options.port === null ||
     options.port === undefined ||
     isNaN(options.port) ||
     options.port < -1 ||
     options.port > 65536){
    messages.push('Must provide a port number between 0 and 65535.\n');
    err = 1;
  }


  //Explain defaults when usage is otherwise correct
  if(!err){

    if(!options.backupBucket && options.profile !== 'default'){
      messages.push('AWS profile is unnecessary without a provided bucket.');
    }

    if(options.backupBucket){
      if(env && env.AWS_ACCESS_KEY_ID){
        messages.push('Preferring AWS environment variables to ~/.aws/credentials.');
      }else{
        messages.push('Using ' + options.profile || 'default' + ' aws profile from ~/.aws/credentials.');
      }
    }

    if(options.monitor){
      messages.push('Running in monitoring mode. Remote files will be checked for freshness but not loaded or backed up.');
    }

    messages.push('Connecting to ' + options.host + ' on port ' + options.port + '.')
    messages.push('Loading data into ' + options.alias + '/' + options.type + '.');


  }


  return {
    messages: messages,
    err: err
  };
}
