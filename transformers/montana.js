'use strict';
var template = require('../lib/pointTransformer');
module.exports = template(function(props){
  return props.Number + ' ' + props.Address
}, 'City', 'State', 'ZIP');
