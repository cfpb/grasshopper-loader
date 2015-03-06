var through = require('through2');
var makeAddress = require('../lib/makeAddress');

function transform(chunk, enc, cb){
  chunk = JSON.parse(chunk);
  var trimmed = {};
  var props = chunk.properties;

  trimmed.address = makeAddress(props.ADR_LABEL,
                                props.ADR_CITY,
                                props.ADR_ST,
                                props.ADR_ZIP5
                                );
  trimmed.coordinates = chunk.geometry.coordinates

  this.push(JSON.stringify(trimmed));
  cb();
}

module.exports = function(){
  return through(transform);
};
