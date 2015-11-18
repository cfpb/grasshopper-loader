'use strict';
//drop falsy values
function filter(v){
  return v;
}

//space args properly
function spacer(v, i){
  return i > 0 ? ' ' + v : v;
}


module.exports = function(nmbr, street, city, st, zip){
  if(!nmbr || !street) return null;
  return Array.prototype.filter.call(arguments, filter).map(spacer).join('');
}
