var jsonstream = require("JSONstream");
var through = require("through2");
var fs= require('fs');

var jStream = jsonstream.parse('features..type');
jStream.on('root',function(){console.log(arguments)});
jStream.on('error',function(e){console.log(e)});

fs.createReadStream('t.json')
  .pipe(jStream)
  .pipe(through(function(chunk, enc, cb){
    console.log(typeof chunk);
    console.log(chunk);
    this.push(chunk); 
  })).pipe(process.stdout);
