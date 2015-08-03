'use strict';
var template = require('../lib/transformerTemplate');
module.exports = template('PrimaryAdd', 'ZN',
  function(props){
    return props.State || 'RI'
  }, 'Zip');
