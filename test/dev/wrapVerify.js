'use strict';
var verify = require('../../lib/verify');

function log(err){
  if(err) throw err.error;
  console.log('GREAT');
}

verify('../data/t.jsn', 10, log);
