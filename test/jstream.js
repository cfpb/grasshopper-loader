var jsonstream = require("JSONstream");
var through = require("through2");
var fs= require('fs');

fs.createReadStream('t.json')
  .pipe(jsonstream.parse('features'))
  .pipe(through(function(chunk, enc, cb){
    console.log("Got chunk: %s", chunk.toString()); 
  }))
