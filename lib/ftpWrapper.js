var ftp = require('ftp');

var clients = {};

function connect(urlObj, callback, errback){

  if(clients[urlObj.hostname]) return process.nextTick(function(){
    return callback(null, clients[urlObj.hostname]);
  });

  var client = new ftp();
  var errBeforeCallback;
  clients[urlObj.hostname] = client;

  client.on('ready', function(){
    if(errBeforeCallback) return callback(errBeforeCallback);
    return callback(null, client);
  });

  client.on('error', function(err){
    errBeforeCallback = err;
    return errback(err);
  });

  client.connect({host: urlObj.hostname});
}


function request(urlObj, record, callback, errback){
  var client = clients[urlObj.hostname];
  if(!client) return errback(new Error('Ftp client not connected'), record);

  client.get(urlObj.path, function(err, stream){
    if(err) return errback(err, record);
    callback(stream, record);
  });
}


module.exports = {
  connect: connect,
  request: request/*,
  list: list,
  close: close*/
}
