//covert to string and/or trim
function trimmer(v){
  return (v + '').trim();
}
module.exports = function(addr, city, st, zip){
  var t = [].map.call(arguments, trimmer);
  return t[0] + ' ' + t[1] + ', ' + t[2] + ', ' + t[3];
}
