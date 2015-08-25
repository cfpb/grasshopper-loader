var ftp = require('ftp');
var eos = require('end-of-stream');

module.exports = function(urlObj, record, callback, errback){
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
};
