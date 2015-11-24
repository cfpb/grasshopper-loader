'use strict';

var us = require('us');

var fipsToAbbr = us.mapping('fips', 'abbr');
var fipsReg = /tl_\d{4}_(\d{2})/;

function getState(file){
  var match = file.match(fipsReg);
  if(!match) return void 0;
  return fipsToAbbr[match[1]];
}

module.exports = getState;
