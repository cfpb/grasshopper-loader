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


module.exports = function(addr, city, st, zip, name){
  if(!addr) throw new Error('No valid address for ' + name||'');
  return Array.prototype.filter.call(arguments, filter).map(spacer).join('');
}
