'use strict';
//drop falsy values
function filter(v){
  return v;
}

//covert to string and/or trim and space
function spacer(v, i){
  var trimmed = (v + '').trim();
  return i > 0 ? ' ' + trimmed : trimmed;
}


module.exports = function(addr, city, st, zip){
  if(!addr) return null;
  return Array.prototype.filter.call(arguments, filter).map(spacer).join('');
}
