var fs = require('fs-extra');
var path = require('path');
var pump = require('pump');
var yauzl = require('yauzl');

module.exports = function(file, record, outputDir, callback, errback){
  if(!record.file) return errback(record, new Error('Must provide a file in the record.'));

  var zipdir = path.join(outputDir, record.name);

    yauzl.open(file, function(err, zip){
      if(err) return errback(record, err);
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
              callback(path.join(zipdir, record.file));
            }
          })
        })
      })
    })
}
