var pump = require('pump');
var lump = require('lump-stream');
var bulkPrefixer = require('./bulkPrefixer');
var esLoader = require('./esLoader');

function pipeline(options, stream, record, cb){

  try{
    var client = esLoader.connect(options.host, options.port, options.log);
    var loader = esLoader.load(client, record.name, options.alias, options.type);
  }catch(e){
    return cb(e);
  }

  pump(
    stream,
    bulkPrefixer(),
    lump(Math.pow(2, 20)),
    loader,
    function(err){
      if(err) return cb(err);
    }
  );

  return loader;
}

module.exports = pipeline;
