var fs = require('fs-extra');
var path = require('path');
var crypto = require('crypto');
var pump = require('pump');
var handleCsv = require('./handleCsv');
var unzipFile = require('./unzipFile');

var csvReg = /(?:txt|csv)$/i;

module.exports = function(stream, record, scratchSpace, callback, errback){

  var ownScratch = path.join(scratchSpace, crypto.pseudoRandomBytes(10).toString('hex'));
  var zipdir = path.join(ownScratch, record.name);
  var zipfile = zipdir + '.zip';

  pump(stream, fs.createOutputStream(zipfile), function(err){
    if(err) return stream.emit('error', err);
    unzipFile(zipfile, record, ownScratch, function(unzipped){
      if(csvReg.test(record.file)){
        handleCsv(unzipped, record, ownScratch, callback, errback);
      }else{
        callback(unzipped);
      }
    }, errback);
  });
};
