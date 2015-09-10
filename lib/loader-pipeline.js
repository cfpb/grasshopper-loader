var pump = require('pump');
var lump = require('lump-stream');
var bulkPrefixer = require('./bulkPrefixer');
var esLoader = require('./esLoader');

function pipeline(options, stream, record, cb){

  if(options.forcedIndex){
    esLoader.loadIntoIndex(options, pumpIntoLoader);
  }else{
    esLoader.load(options, record.name, pumpIntoLoader);
  }


  function pumpIntoLoader(err, loader){
    if(err) return cb(err);

    pump(
      stream,
      bulkPrefixer(),
      lump(Math.pow(2, 20)),
      loader,
      function(err){
        if(err) return loader.emit('error', err);
      }
    );

    cb(null, loader);
  }
}

module.exports = pipeline;
