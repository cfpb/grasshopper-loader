var ftp = require('ftp');

var clients = {};

function connect(urlObj, callback, errback){

  if(clients[urlObj.hostname]) return process.nextTick(function(){
    return callback(null, clients[urlObj.hostname]);
  });

  var client = new ftp();
  var beforeCallback = 1;
  clients[urlObj.hostname] = client;

  client.on('ready', function(){
    beforeCallback = 0;
    return callback(null, client);
  });

  client.on('error', function(err){
    delete clients[urlObj.hostname];
    if(errback) errback(err);
    if(beforeCallback) callback(err);
  });

  client.connect({host: urlObj.hostname, port: urlObj.port || '21'});
}


function request(urlObj, callback){

  var client = clients[urlObj.hostname];
  if(!client) return callback(new Error('Ftp client not connected with hostname: ' + urlObj.hostname));

  client.get(urlObj.path, function(err, stream){
    if(err) return callback(err);
    callback(null, stream);
  });
}


function list(urlObj, callback){
  var client = clients[urlObj.hostname];
  if(!client) return callback(new Error('Ftp client not connected with hostname: ' + urlObj.hostname));

  client.list(urlObj.path, function(err, endpoints){
    if(err) return callback(err);
    callback(null, endpoints);
  });
}


function closeClients(){
  Object.keys(clients).forEach(function(key){
    clients[key].end();
    delete clients[key];
  });
}


module.exports = {
  connect: connect,
  request: request,
  list: list,
  closeClients: closeClients
}
