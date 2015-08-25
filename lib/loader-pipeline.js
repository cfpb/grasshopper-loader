var pump = require('pump');
var lump = require('lump-stream');
var bulkPrefixer = require('./bulkPrefixer');
var esLoader = require('./esLoader');

function pipeline(program, stream, client, cb){
  var loader;

  try{
    loader = esLoader.load(client, program.name, program.alias, program.type);
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
