var test = require('tape');
var streamStats = require('stream-stats');
var isStream = require('isstream');

var checkUsage = require('../lib/checkUsage');
var makeBulkSeparator = require('../lib/makeBulkSeparator');
var makeAddress = require('../lib/makeAddress');

var ogr = require('../lib/ogr');
var splitOGRJSON = require('../lib/splitOGRJSON');

var esLoader = require('../lib/esLoader');


test('Check Usage', function(t){
  t.plan(6);

  var program = {
    shapefile: 'someshape',
    host: 'localhost',
    port: 9200,
    transformer: './transformers/default'
  }

  var first = checkUsage(program)
  t.equal(first.messages.length, 3);
  t.equal(first.err, 0);
  
  var second = checkUsage({port:9201});
  t.equal(second.messages.length, 1);
  t.equal(second.err, 1);

  var third = checkUsage({port:NaN})
  t.equal(third.messages.length, 2);
  t.equal(third.err, 1);
});

test('ogr module', function(t){
  t.plan(3);
  
  var shp = './data/t.shp';

});
