var through = require('through2');
var makeAddress = require('../lib/formatAddress');

var prefix = '';
var suffix = '';

function transform(chunk, enc, cb){
  chunk = JSON.parse(chunk);
  var trimmed = {};
  var props = chunk.properties;

  trimmed.address = makeAddress(props.ADR_LABEL,
                                props.ADR_CITY,
                                props.ADR_STATE,
                                props.ADR_ZIP5
                                );
  trimmed.coordinates = chunk.geometry.coordinates

  //Elaticsearch bulk wants newline separated values
  this.push(prefix + JSON.stringify(trimmed) + suffix);
  cb();
}

module.exports = function(pre, suf){
  if(pre) prefix = pre;
  if(suf) suffix = suf;
  return through(transform);
};
