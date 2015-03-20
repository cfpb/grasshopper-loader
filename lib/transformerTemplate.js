var through = require('through2');
var formatAddress = require('../lib/formatAddress');


module.exports = function(addr, city, state, zip){
  if(!addr||!city||!state||!zip) throw new Error('Must provide address, city, state, and zip.');
  var prefix = '';
  var suffix = '';

  //Chunk is a GeoJSON feature
  function transform(chunk, enc, cb){
    chunk = JSON.parse(chunk);
    var payload = {};
    var props = chunk.properties;

    payload.address = formatAddress(
      props[addr],
      props[city],
      props[state],
      props[zip]
    );

    payload.coordinates = chunk.geometry.coordinates

    //Elaticsearch bulk wants newline separated values
    this.push(prefix + JSON.stringify(payload) + suffix);
    cb();
  }

  return function(pre, suf){
    if(pre) prefix = pre;
    if(suf) suffix = suf;
    return through(transform);
  }
};
