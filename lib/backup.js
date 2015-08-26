var fs = require('fs');
var path = require('path');
var pump = require('pump');
var zlib = require('zlib');
var UploadStream = require('./UploadStream');

module.exports = function(options, stream, record, callback){
  if(options.backupBucket) var uploadStream = new UploadStream(options.backupBucket, options.profile);

  if(options.backupBucket && !options.backupDirectory){
    options.backupDirectory = '.';
  }

  var endfile = path.join(options.backupDirectory, record.name + '.csv.gz');
  var zipStream = zlib.createGzip();
  var destStream;

  if(options.backupBucket){
    destStream = uploadStream.stream(endfile);
    record._retrieverOutput = destStream;
  }else{
    destStream = fs.createWriteStream(endfile);
    record._retrieverOutput = endfile;
  }

  //don't close stream on backup failure
  stream.pipe(zipStream);

  pump(zipStream, destStream, callback);
};
