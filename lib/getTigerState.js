'use strict';

var us = require('us');

var fipsToAbbr = us.mapping('fips', 'abbr');
var fipsReg = /tl_\d{4}_(\d{2})/;

function getState(file){
  return fipsToAbbr[file.match(fipsReg)[1]];
}

module.exports = getState;
