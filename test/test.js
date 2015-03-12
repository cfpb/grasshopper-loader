var test = require('tape');

var checkUsage = require('../lib/checkUsage');
var makeBulkSeparator = require('../lib/makeBulkSeparator');
var makeAddress = require('../lib/makeAddress');

var ogr = require('../lib/ogr');
var splitOGRJSON = require('../lib/splitOGRJSON');

var esLoader = require('../lib/esLoader');


test('Check Usage', function(t){
  t.plan(5);
  t.equal(2+2, 4);
});
