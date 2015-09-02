var fs = require('fs-extra');
var path = require('path');
var pump = require('pump');
var handleCsv = require('./handleCsv');
var unzipFile = require('./unzipFile');

var csvReg = /(?:txt|csv)$/i;

module.exports = function(stream, record, scratchSpace, callback, errback){

  var zipdir = path.join(scratchSpace, record.name);
  var zipfile = zipdir + '.zip';

  pump(stream, fs.createOutputStream(zipfile), function(err){
    if(err) return stream.emit('error', err);
    unzipFile(record, scratchSpace, function(unzipped){
      if(csvReg.test(record.file)){
        handleCsv(unzipped, record, scratchSpace, callback, errback);
      }else{
        callback(record, unzipped);
      }
    }, errback);
  });
};
