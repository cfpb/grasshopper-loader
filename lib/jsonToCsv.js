//NOTE:
//
//Flubs on commas in data (doesn't differentiate them in any way from field seperators). Brittle.
//FIXME
//
var through = require('through2');

module.exports = function(){
  var stream = through(function(chunk, enc, cb){

    try{
      var obj = JSON.parse(chunk.toString());
      var coords = obj.geometry.coordinates;
      var props = obj.properties;
      var x = coords[0];
      var y = coords[1];
    }catch(e){
      return process.nextTick(function(){
        return cb(new Error('Couldn\'t backup invalid GeoJSON feature: ' + chunk.toString()));
      });
    }

    return cb(null, [x, y, props.address, props.number, props.street, props.city, props.state, props.zip].join(',') + '\n');
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
      "number": "number",
      "street": "street",
      "city": "city",
      "state": "state",
      "zip": "zip"
    }
  }));

  return stream;
};
