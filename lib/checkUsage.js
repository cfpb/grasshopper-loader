'use strict';

var isUrl = require('is-url');
var url = require('url');
var path = require('path');

module.exports = function(program, env){
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

  if(program.preformatted && program.sourceSrs && program.sourceSrs !== 'WGS84'){
    messages.push('Source coordinate system is ignored for preformatted data. Transform to WGS84 with ogr2ogr before loading.');
    err = 1;
  }

  if(path.extname(program.data) === '.csv' && !program.preformatted){
    messages.push('Only accepts preformatted .csv data. Run through ogr2ogr specifying your transform and passing -lco GEOMETRY=AS_XY to get spatial data in a form accessible by the loader.');
    err = 1;
  }

  //Explain defaults when usage is otherwise correct
  if(!err){
    if(env && env.ELASTICSEARCH_PORT){
      messages.push('Elasticsearch Docker container found at: '+ env.ELASTICSEARCH_PORT+'. Connecting...\n');
    }else{
      if(program.host === 'localhost'){
        messages.push('Connecting to localhost.\n')
      }

      if(program.port === 9200){
        messages.push('Using port 9200.\n');
      }
    }


    if(program.bucket && !program.data){
      if(program.transformer){
        messages.push('Streaming in contents of ' + program.bucket +'. '+ program.transformer + ' will be used for each dataset.\n');
      }else{
        messages.push('Streaming in contents of ' + program.bucket +'. Transformers will be resolved automatically from key names if possible.\n');
      }
    }

    if(!program.bucket && program.profile !== 'default'){
      messages.push('AWS profile is unnecessary without a provided bucket.\n');
    }

    if(program.bucket){
      if(env && env.AWS_ACCESS_KEY_ID){
        messages.push('Preferring AWS environment variables to ~/.aws/credentials.\n');
      }else{
        messages.push('Using ' + program.profile || 'default'+ ' aws profile from ~/.aws/credentials.\n');
      }
    }

    if(program.sourceSrs){
      messages.push('Setting source spatial reference system to ' + program.sourceSrs +'.\n')
    }

    if(program.preformatted){
      messages.push('Data is preformatted. Skipping ogr2ogr.\n')
    }


    messages.push('Loading data into ' + program.index + '/' + program.type + '.\n');


  }


  return {
    messages: messages,
    err: err
  };
}
