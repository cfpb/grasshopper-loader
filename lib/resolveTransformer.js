'use strict';
var path = require('path');

function resolve(transformer, file){
  return transformer
    ? path.resolve(transformer)
    : path.resolve('./transformers', path.basename(file, path.extname(file)) + '.js');
}

module.exports = resolve;
