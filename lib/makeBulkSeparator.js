'use strict';
//Make bulk metadata
//Index operation, using provided index and type
module.exports = function(index, type){
  if(type && !index) throw new Error('Must provide index with type');
  var sep = {};
  sep.index = {};
  if(index) sep.index._index = index;
  if(type) sep.index._type = type;
  return JSON.stringify(sep) + '\n';
}
