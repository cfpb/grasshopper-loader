var fs = require('fs');
var path = require('path');
var nodedir = require('node-dir');

var callback;

function process(input){
  var ext = path.extname(input);

  if(ext) return callback(null, input);


  
}

module.exports = {
  init: init,
  process: process
}


