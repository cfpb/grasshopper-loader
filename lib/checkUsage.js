var errorText = ''

function usage(error) {
  console.log(error);
  console.log('usage: ./shp2es -s <shapefile> -h <ElasticSearch host> -p <ElasticSearch port>');
  return false;
}

module.exports = function(program){
  if(!program.shapefile){
    errorText += 'Must provide a shapefile.\n';
  }

  if(!program.host){
    errorText += 'Must provide an elasticsearch host.\n';
  }

  if(!program.port || program.port < -1 || program.port > 65536){
    errorText += 'Must provide a port number between 0 and 65535.\n';
  }


  if (errorText) return usage(errorText);
  return true;
}
