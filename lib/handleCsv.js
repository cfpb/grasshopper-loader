var fs = require('fs-extra');
var path = require('path');
var pump = require('pump');
var csvToVrt = require('csv-to-vrt');

module.exports = function(csv, record, scratchSpace, callback, errback){
  if(typeof csv === 'string'){
    makeVrt();
  }else{
    var stream = csv;
    csv = path.join(scratchSpace, record.file);
    var csvStream = fs.createOutputStream(csv);
    pump(stream, csvStream, function(err){
      if(err) return errback.call(stream, record, err);
      makeVrt();
    });
  }

  function makeVrt(){
    csvToVrt(csv, record.spatialReference, function(err, vrt){
      if(err) return errback(record, err);
      callback(record, vrt);
    });
  }
}
