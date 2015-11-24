var ftp = require('ftp');
var eos = require('end-of-stream');

module.exports = function(urlObj, record, callback, errback){
  if(!urlObj || !urlObj.path || !urlObj.hostname) return errback(new Error('Must provide parsed url object as first argument to connectToFtp.'), record);

  var ftpClient = new ftp();

  ftpClient.on('ready', function(){
    ftpClient.get(urlObj.path, function(err, stream){

      if(err){
        ftpClient.end();
        return errback(err, record);
      }

      eos(stream, function(){
        ftpClient.end();
      });

      callback(stream, record);
    });
  });

  ftpClient.connect({host: urlObj.hostname});

  ftpClient.on('error', function(err){
    return errback(err, record);
  });
};
