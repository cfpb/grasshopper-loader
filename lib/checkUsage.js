function usage(error, err) {
  console.log(error);
  console.log('usage: ./grasshopper-loader -s <shapefile> -h <ElasticSearch host> -p <ElasticSearch port> -t <transformer>');
  return ;
}

module.exports = function(program){
  var errorText = ''
  var err = 0;

  if(!program.shapefile){
    errorText += 'Must provide a shapefile.\n';
    err = 1;
  }

  if(!program.host){
    errorText += 'Must provide an elasticsearch host.\n';
    err = 1;
  }

  if(!program.port || program.port < -1 || program.port > 65536){
    errorText += 'Must provide a port number between 0 and 65535.\n';
    err = 1;
  }

  if(!program.transformer){
    errorText += 'Using default transformer';   
  }


  return usage(errorText, err);
}
