module.exports = function(program){
  var messages = [];
  var err = 0;

  if(!program.shapefile){
    messages.push('Must provide a shapefile.\n');
    err = 1;
  }

  if(program.port < -1 || program.port > 65536){
    messages.push('Must provide a port number between 0 and 65535.\n');
    err = 1;
  }
  
  //Explain defaults when usage is otherwise correct
  if(!err){
    if(program.host === 'localhost'){
      messages.push('Connecting to localhost.\n')
    }
    if(program.port === 9200){
      messages.push('Using port 9200.\n');
    }

    if(program.transformer === './transformers/default'){
      messages.push('Using default transformer.\n');   
    }
  }


  return {
    messages:messages,
    err: err
  };
}
