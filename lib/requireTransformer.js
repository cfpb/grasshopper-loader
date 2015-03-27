var path = require('path');
var resolveTransformer = require('./resolveTransformer.js');

var iterCount = 1;

function requireTransformer(transformer, file, iterations) {
  iterations = iterations ? iterations : 0;
  try{
    return require(transformer);
  }catch(err){
    if(iterations < iterCount){
      return requireTransformer(resolveTransformer(null, path.dirname(file)), file, iterations + 1);
    }
    throw err;
  }
}

module.exports = requireTransformer;
