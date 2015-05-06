'use strict';
var template = require('../lib/pointTransformer');
module.exports = template('FULLADDR', 'PO_NAME', 'STATE', function(props){
  return +props.ZIP_5.replace(/,/,'');
});
