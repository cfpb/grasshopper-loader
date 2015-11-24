'use strict';
//drop falsy values
function filter(v){
  if (v === 'null') v = null;
  return v;
}


module.exports = function(nmbr, street, city, st, zip){
  if(!street) return null;
  return Array.prototype.filter.call(arguments, filter).join(' ');
}
