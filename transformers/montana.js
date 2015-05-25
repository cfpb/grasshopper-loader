'use strict';
var template = require('../lib/transformerTemplate');
module.exports = template(function(props){
  return props.Number + ' ' + props.Address
}, 'City', 'State', 'ZIP');
