var fs = require('fs');

var test = require('tape');
var streamStats = require('stream-stats');
var isStream = require('isstream');

var checkUsage = require('../lib/checkUsage');
var makeBulkSeparator = require('../lib/makeBulkSeparator');
var makeAddress = require('../lib/makeAddress');

var ogrChild = require('../lib/ogrChild');
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
  
  var shp = 'test/data/t.shp';
  var child = ogrChild(shp); 
  var errInChild = 0;

  t.ok(child, 'ogrChild process is created');
  t.ok(isStream(child.stdout), 'the child process has stdout');

  child.stderr.once('data',function(){
    errInChild = 1;
  });

  child.stderr.once('end',function(){
    t.notOk(errInChild, 'ogr2ogr doesn\'t emit an error');
  });
});

test('splitOGRJSON module', function(t){
  t.plan(1); 

  var json = 'test/data/t.json';
  var stats = streamStats('splitOGR',{store:1});
  
  fs.createReadStream(json)
    .pipe(splitOGRJSON())
    .pipe(stats)
    .sink();

  stats.on('end', function(){
    var result = stats.getResult(); 
    var validJSON = 1;
    console.log(result);
    result.chunks.forEach(function(v){
      try{
        JSON.parse(v.chunk.toString())   
      }catch(e){
        validJSON = 0;
      }
    });
    t.ok(validJSON, 'splitOGRJSON yields valid JSON chunks');
  })
});


