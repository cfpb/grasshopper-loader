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


function request(urlObj, callback){
  var client = clients[urlObj.hostname];
  if(!client) return callback(new Error('Ftp client not connected'));

  client.get(urlObj.path, function(err, stream){
    if(err) return callback(err);
    callback(null, stream);
  });
}


function list(urlObj, callback){
  var client = clients[urlObj.hostname];
  if(!client) return callback(new Error('Ftp client not connected'));

  client.list(urlObj.path, function(err, endpoints){
    if(err) return callback(err);
    callback(null, endpoints);
  });
}


function closeClients(){
  Object.keys(clients).forEach(function(key){
    clients[key].end();
  });
}


module.exports = {
  connect: connect,
  request: request,
  list: list,
  closeClients: closeClients
}
