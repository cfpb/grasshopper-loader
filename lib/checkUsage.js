'use strict';

var isUrl = require('is-url');
var url = require('url');
var path = require('path');

module.exports = function(program){
  var messages = [];
  var err = 0;

  if(!program.data&&!program.bucket){
    messages.push('Must provide data as a shapefile, geodatabase, GeoJson or zip.\nYou can also provide an S3 bucket.\n');
    err = 1;
  }

  if(isNaN(program.port) || program.port < -1 || program.port > 65536){
    messages.push('Must provide a port number between 0 and 65535.\n');
    err = 1;
  }

  if(isUrl(program.data)){
    var ext = path.extname(url.parse(program.data).pathname);
    if(ext !== '.zip' && ext !== '.json'){
      messages.push('Files accessed via URL must be either zipped or GeoJson.\n');
      err = 1;
    }
  }
  
  //Explain defaults when usage is otherwise correct
  if(!err){
    if(program.host === 'localhost'){
      messages.push('Connecting to localhost.\n')
    }

    if(program.port === 9200){
      messages.push('Using port 9200.\n');
    }
    
    if(program.bucket && !program.data){
      if(program.transformer){
        messages.push('Streaming in contents of ' + program.bucket +'. '+ program.transformer + ' will be used for each dataset.\n');
      }else{
        messages.push('Streaming in contents of ' + program.bucket +'. Transformers will be resolved automatically from key names if possible.\n');
      }
    }

    if(!program.bucket && program.profile){
      messages.push('AWS profile is unnecessary without a provided bucket.\n');
    }

    if(program.bucket && !program.profile){
      messages.push('Using default aws profile from ~/.aws/credentials.\n');
    }

    messages.push('Loading data into ' + program.index + '/' + program.type + '.\n');


  }


  return {
    messages:messages,
    err: err
  };
}
