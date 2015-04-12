'use strict';
var path = require('path');

function resolve(transformer, file){
  if(!transformer){
    return path.resolve('./transformers', path.basename(file, path.extname(file)) + '.js'); 
  }

  if(transformer.match(/.js$/i)){
    return path.resolve(transformer);
  }
  
  return path.resolve('./transformers', transformer + '.js');

}

module.exports = resolve;
