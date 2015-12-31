var fs = require('fs-extra');
var path = require('path');
var crypto = require('crypto');
var pump = require('pump');
var csvToVrt = require('csv-to-vrt');

module.exports = function(csv, record, scratchSpace, callback, errback){
  if(typeof csv === 'string'){
    makeVrt();
  }else{
    var stream = csv;
    try{
      var ownScratch = path.join(scratchSpace, crypto.pseudoRandomBytes(10).toString('hex'));
      csv = path.join(ownScratch, record.file);
      var csvStream = fs.createOutputStream(csv);
    }catch(e){
      return errback(stream, e);
    }
    pump(stream, csvStream, function(err){
      if(err) return errback(stream, err);
      makeVrt();
    });
  }

  function makeVrt(){
    csvToVrt(csv, record.spatialReference, function(err, vrt){
      if(err) return errback(null, err);
      callback(vrt);
    });
  }
}
