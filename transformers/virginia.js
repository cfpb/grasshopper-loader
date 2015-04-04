var template = require('../lib/transformerTemplate');
module.exports = template('FULLADDR', 'PO_NAME', 'STATE', function(props){
  return +props.ZIP_5.replace(/,/,'');
});
