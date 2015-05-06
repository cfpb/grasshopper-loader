'use strict';
var through = require('through2');

var loadDate = Date.now();

module.exports = function(){
  var prefix = '';
  var suffix = '';
  
  //Chunk is a GeoJSON feature
  function transform(chunk, enc, cb){
    var json = JSON.parse(chunk);
    var props = json.properties;

    Object.keys(props).forEach(function(v){
      if(props[v] === null){
        props[v] = "";
      }
    });
    
    props.load_date = loadDate;

    //Elaticsearch bulk wants newline separated values
    this.push(prefix + JSON.stringify(json) + suffix);
    cb();
  }

  return function(pre, suf){
    if(pre) prefix = pre;
    if(suf) suffix = suf;
    return through(transform);
  }
};
