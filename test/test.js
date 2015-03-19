var fs = require('fs');
var path = require('path');

var test = require('tape');
var streamStats = require('stream-stats');
var isStream = require('isstream');
var ignore = require('ignore');

var checkUsage = require('../lib/checkUsage');
var ogrChild = require('../lib/ogrChild');
var splitOGRJSON = require('../lib/splitOGRJSON');
var makeBulkSeparator = require('../lib/makeBulkSeparator');
var formatAddress = require('../lib/formatAddress');
var esLoader = require('../lib/esLoader');
var transformerTemplate = require('../lib/transformerTemplate');


test('Check Usage', function(t){
  t.plan(6);

  var program = {
    data: 'someshape',
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


test('ogriChild module', function(t){
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


test('makeBulkSeparator module', function(t){
  t.plan(3);
  var sep = makeBulkSeparator('ind','typ'); 
  var sepObj = JSON.parse(sep.slice(0,sep.length-1));
  var expectedSep = {index:{_index:'ind',_type:'typ'}}
  t.deepEqual(sepObj, expectedSep, 'Bulk separator on standard input');

  var mtSep = makeBulkSeparator();
  var mtObj = JSON.parse(mtSep.slice(0,mtSep.length-1));
  var expectedMT = {index:{}};
  t.deepEqual(mtObj, expectedMT, 'Bulk separator on empty input');
  
  try{
    makeBulkSeparator(null, 'failing'); 
  }catch(e){
    t.pass('Bulk separator with a type and no index fails');
  }
});


test('formatAddress module', function(t){
  t.plan(2);

  var add1 = '123 fake st. San Francisco, CA, 12345';
  var add2 = '221B Baker St. Arg, AZ, 67876';

  t.equal(formatAddress('123 fake st.', 'San Francisco', 'CA', 12345), add1, 'Standard format')
  t.equal(formatAddress('  221B Baker St.', 'Arg  ', 'AZ', '67876'), add2, 'Trim strings');
});


test('esLoader module', function(t){
  t.plan(2);
  try{
    esLoader.connect();
  }catch(e){
    t.pass('Connect fails without host/port')
  }
  t.ok(isStream.isWritable(esLoader.load()),'esLoader.load returns a write stream');
});


test('transformerTemplate module', function(t){
  t.plan(5);
  var trans = transformerTemplate('addr','cty','st', 'zip');

  t.equal(typeof trans, 'function', 'template returns a function');
  t.ok(isStream.isDuplex(trans()), 'The produced transformer generates a transform stream'); 

  var stats = streamStats('transTemplate',{store:1});

  var preSufTest = trans('start','finish');

  preSufTest.pipe(stats).sink();
  stats.on('end',function(){
    var result = stats.getResult();
    var output = result.store.toString();
    t.ok(/^start/.test(output), 'prefix applied properly'); 
    t.ok(/finish$/.test(output), 'suffix applied properly');
  });
  preSufTest.end('{"properties":{"addr":"123 a st","cty":"sunny","st":"ca","zip":54321},"geometry":{"coordinates":[]}}')

  try{
    transformerTemplate();
  }catch(e){
    t.pass('Calling the template without all arguments throws an error');
  }
});

test('Transformers', function(t){
  
  fs.readdir('transformers/',function(err,transformers){

    var sample = 'test/data/sample.json';
    var bulkMatch = {index:{_index:'address',_type:'point'}}
    var dataMatch = {"address":"123 a st sunny, ca, 54321", "coordinates":[-129.1,38.2]};

    var bulkMetadata =  makeBulkSeparator('address', 'point');
    var filtered = ignore().addIgnoreFile('.gitignore').filter(transformers);

    t.plan(filtered.length*2);

    filtered.forEach(function(transFile){
      var transformer = require(path.join('../transformers', transFile));
      var stats = streamStats(transFile, {store:1});

      fs.createReadStream(sample)
        .pipe(splitOGRJSON())
        .pipe(transformer(bulkMetadata, '\n'))
        .pipe(stats)
        .sink();

      stats.once('end', function(){
        var result = stats.getResult(); 
        var output = result.store.toString().split('\n')  

        var bulkMeta = JSON.parse(output[0]);
        var data = JSON.parse(output[1]);

        t.deepEqual(bulkMatch, bulkMeta, "Bulk metadata created properly");
        t.deepEqual(dataMatch, data, "Data to insert transformed correctly for " + transFile);

      });
    });


  }); 

  
  

});
