var fs = require('fs');
var path = require('path');
var spawn = require('child_process').spawn;

var test = require('tape');
var streamStats = require('stream-stats');
var isStream = require('isstream');
var ignore = require('ignore');

var checkUsage = require('../lib/checkUsage');
var getGeoFiles = require('../lib/getGeoFiles');
var ogrChild = require('../lib/ogrChild');
var splitOGRJSON = require('../lib/splitOGRJSON');
var makeBulkSeparator = require('../lib/makeBulkSeparator');
var formatAddress = require('../lib/formatAddress');
var esLoader = require('../lib/esLoader');
var verify = require('../lib/verify');
var resolveTransformer = require('../lib/resolveTransformer');
var requireTransformer = require('../lib/requireTransformer');
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

test('getGeoFiles module', function(t){
  t.plan(11); 
  
  ['shp', 'gdb', 'json'].forEach(function(v){
    var input = 'test/data/t.'+ v;
    getGeoFiles(input, function(err, file, cb) {
      t.equal(input, file, v + ' passed through to processData'); 
    });
  });

  getGeoFiles('test/data/t.zip', function(err, file, cb) {
    t.ok(cb, 'cb passed to remove intermediate directory'); 
    try{
      fs.readdirSync(path.dirname(file))
        t.pass('Intermediate directory for zip created');
    }catch(e){
      t.fail('Didn\'t make an appropriate intermediate directory');
    }
    cb(null, function(){
      t.pass('Intermediate dir removed after processing');
    });
  });

  getGeoFiles('test/data/threefiles', function(err, file, cb){
    t.ok(file, file + ' read from directory');
  }); 

  getGeoFiles('some.txt', function(err, file, cb){
    t.ok(err, 'Error produced on bad file type.');
  });

  try{
    getGeoFiles('fakepath', function(err, file, cb){
    });
  }catch(e){
    t.pass('Throws error on bad path.');
  }

});


test('ogrChild module', function(t){
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
  t.plan(4);

  var add1 = '123 fake st. San Francisco CA 12345';
  var add2 = '221B Baker St. Arg AZ 67876';
  var add3 = '123 Unique Way';

  t.equal(formatAddress('123 fake st.', 'San Francisco', 'CA', 12345), add1, 'Standard format')
  t.equal(formatAddress('  221B Baker St.', 'Arg  ', 'AZ', '67876'), add2, 'Trim strings');
  t.equal(formatAddress('123 Unique Way', '', null), add3, 'Gracefully handles lack of city, state, zip');
  try{
    formatAddress('', 'Yreka', 'CA')
  }catch(e){
    t.pass('Throws on no street number/name');
  }
});


test('esLoader module', function(t){
  t.plan(3);
  try{
    esLoader.connect();
  }catch(e){
    t.pass('Connect fails without host/port')
  }
  t.ok(esLoader.connect('localhost', 9200, []), 'Proper connect returns an elasticsearch client');
  t.ok(isStream.isWritable(esLoader.load()),'esLoader.load returns a write stream');
});

test('verify module', function(t){
  t.plan(5);
  
  verify('test/data/t.json', 20, function(err){
    t.notOk(err, 'No error when featureCount equals passed value.'); 
  }); 

  verify('test/data/t.json', 10, function(err){
    t.ok(err.error, 'Produces an error when compared against the wrong number.'); 
    t.equal(err.actual, 20, 'Actual value propagated.');
    t.equal(err.expected, 10, 'Expected value propagated.');
  });
  
  verify('test/data/t.jsn', 10, function(err){
    t.ok(err.error, 'Produces an error when the file doesn\'t exist'); 
  }); 
  
});

test('resolveTransformer module', function(t){
  t.plan(3);
  var arkTrans = path.resolve('./transformers/arkansas.js');
  t.equal(arkTrans, resolveTransformer(null, 'arkansas.gdb'), 'Resolves transformer using filename');
  t.equal(arkTrans, resolveTransformer('./transformers/arkansas.js'), 'Resolves transformer using passed transformer');
  t.equal(arkTrans, resolveTransformer('./transformers/arkansas.js', 'sometext'), 'Resolver prefers passed transformer');
});

test('requireTransformer module', function(t){
  t.plan(3); 

  var arkFile = path.resolve('./transformers/arkansas.js')
  var arkTrans = require(arkFile);
  
  t.equal(arkTrans, requireTransformer(arkFile, 'test/data/arkansas.json'), 'Requires transformer using filename');
  t.equal(arkTrans, requireTransformer(arkFile,'test/data/arkansas/t.shp'), 'Requires transformer after walking to directory name');
  try{
    requireTransformer('dkomqwdnqd/dnqwdqiow','fwerwef'); 
  }catch(e){
    t.pass('Throws on bad require');
  }
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

    var fieldtest = 'test/data/fieldtest.json';
    var bulkMatch = {index:{_index:'address',_type:'point'}}
    var dataMatch = {
      "type": "Feature",
      "properties": {
        "address": "123 a st sunny ca 54321",
        "alt_address": "",
        "load_date": 1234567890123
       },
       "geometry": {
         "type": "Point",
         "coordinates": [-129.1,38.2]
      }
    };
    var validAddresses = ["123 a st sunny ca 54321",
                          "123 a st",
                          "123 a st sunny",
                          "123 a st ca",
                          "123 a st 54321",
                          "123 a st sunny ca",
                          "123 a st sunny 54321",
                          "123 a st ca 54321"
                          ]

    var bulkMetadata =  makeBulkSeparator('address', 'point');
    var filtered = ignore().addIgnoreFile('.gitignore').filter(transformers);

    t.plan(filtered.length*3);

    filtered.forEach(function(transFile){
      var transformer = require(path.join('../transformers', transFile));
      var stats = streamStats(transFile, {store:1});

      fs.createReadStream(fieldtest)
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
        t.ok(validAddresses.indexOf(data.properties.address) !== -1, "Address formed correctly for " + transFile);
        t.deepEqual(dataMatch.geometry, data.geometry, "Data to insert transformed correctly for " + transFile);

      });
    });

  }); 

});

test('Entire loader', function(t){
  t.plan(3);
  var count = 0;
  var loader = spawn('node', ['./grasshopper-loader', '-d', './test/data/arkansas.json', '--index', 'ind', '--type', 'typ'])
  loader.on('exit', function(code){
    t.equal(code, 0, 'Ran without errors, exit code 0, on elasticsearch at localhost:9200')
    if(++count === 3) wipe();
  });

  var l2 = spawn('node', ['./grasshopper-loader', '-d', './test/data/ark.json']);
  l2.on('exit', function(code){
    t.notEqual(code, 0, 'Bails when given an invalid file');
    if(++count === 3) wipe();
  });
  
  var l3 = spawn('node', ['./grasshopper-loader', '-d', './test/data/t.prj', '-t', 'transformers/arkansas.js']);
  l3.on('exit', function(code){
    t.notEqual(code, 0, 'Bails on bad file type');
    if(++count === 3) wipe();
  });

  function wipe(){ 
    spawn('curl', ['-XDELETE','localhost:9200/ind/typ']);
  }
});
