var verify = require('../../lib/verify');

function log(err){
  if(err) throw new Error('Expected: ' + err.expected + '\nActual: ' + err.actual + '\n');
  console.log('GREAT');
}

verify('../data/t.json', 10, log);
