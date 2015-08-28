var through = require('through2');

module.exports = function(){
  var stream = through(function(chunk, enc, cb){
    var obj = JSON.parse(chunk.toString());
    var coords = obj.geometry.coordinates;
    var props = obj.properties;

    return cb(null, [coords[0], coords[1], props.address, props.alt_address].join(',') + '\n');
  });

  //write headers
  stream.write(JSON.stringify({
    type: "Feature",
    geometry: {
      type: "Point",
      coordinates: ["x", "y"]
    },
    properties: {
      "address": "address",
      "alt_address": "alt_address"
    }
  }));

  return stream;
};
