module.exports = function(index, type){
  var sep = {};
  sep.index = {};
  if(index) sep.index._index = index;
  if(type) sep.index._type = type;
  return JSON.stringify(sep) + '\n';
}
