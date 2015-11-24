var through = require('through2');

module.exports = function(){
  var stream = through(function(chunk, enc, cb){

    try{
      var obj = JSON.parse(chunk.toString());
      var coords = obj.geometry.coordinates;
      var props = obj.properties;
      var x = coords[0];
      var y = coords[1];
      var addr = props.address;
      var alt = props.alt_address;
    }catch(e){
      return process.nextTick(function(){
        return cb(new Error('Couldn\'t backup invalid GeoJSON feature: ' + chunk.toString()));
      });
    }

    return cb(null, [x, y, addr, alt].join(',') + '\n');
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
