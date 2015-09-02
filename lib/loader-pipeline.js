var pump = require('pump');
var lump = require('lump-stream');
var bulkPrefixer = require('./bulkPrefixer');
var esLoader = require('./esLoader');

function pipeline(options, stream, record, cb){

  try{
    var client = esLoader.connect(options.host, options.port, options.log);
  }catch(e){
    return cb(e);
  }

  esLoader.load(options, client, record.name, function(err, loader){
    if(err) return cb(err);

    pump(
      stream,
      bulkPrefixer(),
      lump(Math.pow(2, 20)),
      loader,
      function(err){
        if(err) return loader.emit(err);
      }
    );

    cb(null, loader);
  });
}

module.exports = pipeline;
