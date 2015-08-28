var fs = require('fs-extra');
var path = require('path');
var yauzl = require('yauzl');
var pump = require('pump');
var retrieverPipeline = require('./retriever-pipeline');
var handleCsv = require('./handleCsv');

var csvReg = /(?:txt|csv)$/i;

module.exports = function(stream, record, scratchSpace, callback, errback){
  var zipdir = path.join(scratchSpace, record.name);
  var zipfile = zipdir + '.zip';

  pump(stream, fs.createOutputStream(zipfile), function(err){
    if(err) return stream.emit('error', err);

    yauzl.open(zipfile, function(err, zip){
      if(err) return errback(err, record);
      var entriesFinished = 0;
      var count = 0;

      zip.on('end', function(){
        entriesFinished = 1;
      });

      zip.on('entry', function(entry){
        if(/\/$/.test(entry.fileName)) return;

        zip.openReadStream(entry, function(err, readStream) {
          if(err) return errback.call(readStream, record, err);
          count++;
          var output = fs.createOutputStream(path.join(zipdir, entry.fileName));

          pump(readStream, output, function(err){
            if(err) return errback.call(readStream, record, err);
            count--;
            if(entriesFinished && !count){
              var unzipped = path.join(zipdir, record.file);

              if(csvReg.test(record.file)){
                handleCsv(unzipped, record, scratchSpace, callback, errback);
              }else{
                callback(retrieverPipeline(record, unzipped), record);
              }
            }
          });
        });
      });
    });
  });
};
