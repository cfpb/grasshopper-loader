'use strict';
var fs = require('fs');
var path = require('path');
var spawn = require('child_process').spawn;
var program = require('commander');

var test = require('tape');
var streamStats = require('stream-stats');
var isStream = require('isstream');
var ignore = require('ignore');
var concat = require('concat-stream');

var checkUsage = require('../lib/checkUsage');
var Counter = require('../lib/counter');
var getS3Files = require('../lib/getS3Files');
var getGeoUrl = require('../lib/getGeoUrl');
var getGeoFiles = require('../lib/getGeoFiles');
var ogrChild = require('../lib/ogrChild');
var splitOGRJSON = require('../lib/splitOGRJSON');
var makeBulkSeparator = require('../lib/makeBulkSeparator');
var formatAddress = require('../lib/formatAddress');
var esLoader = require('../lib/esLoader');
var verify = require('../lib/verify');
var resolveTransformer = require('../lib/resolveTransformer');
var requireTransformer = require('../lib/requireTransformer');
var pointTransformer = require('../lib/pointTransformer');

program
  .version('0.0.1')
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost', 'localhost')
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200', Number, 9200)
  .option('--index <index>', 'Elasticsearch index. Defaults to testind', 'testindex')
  .option('--type <type>', 'Elasticsearch type within the provided or default index. Defaults to testtype', 'testtype')
  .option('--profile <profile>', 'The aws credentials profile in ~/.aws/credentials. Will also respect AWS keys as environment variables.', 'default')
  .parse(process.argv);

test('Check Usage', function(t){

  var instances = [{
    args:{
      data: 'someshape',
      host: 'localhost',
      port: 9200
    },
    expected:{
      messages:3,
      err:0
    },
    label:'data, host, port'
  },
  {
    args:{
      data:'http://www.google.com/fake.txt',
      host: 'localhost',
      port: 9200
    },
    expected:{
      messages:1,
      err:1
    },
    label:'Url test with bad filetype'
  },
  {
    args:{
      data:'http://www.google.com/fake.zip',
      host: 'localhost',
      port: 9200
    },
    expected:{
      messages:3,
      err:0
    },
    label:'Url with good filetype'
  },
  {
    args:{
      port: 9201
    },
    expected:{
      messages:1,
      err:1
    },
    label:'Non-default port, no data'
  },{
    args:{
      port: NaN 
    },
    expected:{
      messages:2,
      err:1
    },
    label:'No data, bad port'
  },{
    args:{
      bucket: 'abuck',
      data: 'data',
      profile: 'www',
      host: 'localhost',
      port: 9200
    },
    expected:{
      messages:3,
      err:0
    },
    label:'Bucket, data, profile'
  },{
    args:{
      bucket: 'anotherb',
      host: 'localhost',
      port: 9200
    },
    expected:{
      messages:5,
      err:0
    },
    label:'Bucket only'
  },{
    args:{
      data: 'da',
      profile: 'unneeded',
      host: 'localhost',
      port: 9200
    },
    expected:{
      messages:4,
      err:0
    },
    label:'Data, unnecessary profile'
  },{
    args:{
      bucket: 'buck3',
      transformer:'arkansas',
      host: 'localhost',
      port: 9200
    },
    expected:{
      messages:5,
      err:0
    },
    label:'Bucket and transformer'
  }
  ];

  instances.forEach(function(v){
    var usage = checkUsage(v.args);
    t.equal(usage.messages.length, v.expected.messages, v.label + ' messages.');
    t.equal(usage.err, v.expected.err, v.label + ' err.');
  });

  t.end();

});

test('counter', function(t){
  t.plan(1);
  var counter = new Counter();
  counter.incr();
  counter.incr();
  t.equal(counter.decr(), 1, 'Counter works');
});

test('getS3Files module', function(t){
  t.plan(15);

  var simpleKeys = [
    {"bucket":"wyatt-test", "data":"new_york.json"},
    {"bucket":"wyatt-test", "data":"new_york.json", "profile":"wyatt-test"},
    {"bucket":"wyatt-test", "data":"test/arkansas.json"}
  ];

  var zip = {'bucket':'wyatt-test', 'data':'arkansas.zip'};
  var folder = {'bucket':'wyatt-test', 'data':'test'};
  var bucket = {'bucket':'wyatt-test'};

  simpleKeys.forEach(function(v){
    getS3Files(v, new Counter(), function(err, file, stream, cb){
      t.notOk(err, 'No error with '+ JSON.stringify(v));
      t.ok(isStream(stream), 'Stream exists');
      t.equal(v.data, file, 'Carries key into file');
    });
  });

  getS3Files(zip, new Counter(), function(err, file, stream, cb){
    if(typeof stream === 'function') cb = stream;
    t.notOk(err, 'No error getting zip');
    t.equal(path.join(path.basename(path.dirname(file)), path.basename(file)), 'arkansas/t.shp', 'Shapefile extracted and passed from S3.');
    if(cb) cb();
  });

  getS3Files(folder, new Counter(), function(err, file, stream, cb){
    t.notOk(err, 'No error on folder');
    t.ok(isStream(stream), 'Generates stream');
    t.equal(file,'test/arkansas.json', 'Operate on only actual file in folder.'); 
  });

  var count = 0;
  getS3Files(bucket, new Counter(), function(err, file, stream, cb){
    if(typeof stream === 'function') cb = stream;
    if(++count === 4) t.pass('Gets all the files from the bucket.'); 
    if(cb) cb();
  })
     
});


test('getGeoUrl module', function(t){
  t.plan(3);

  var zip = "http://cfpb.github.io/grasshopper-loader/arkansas.zip"
  var json = "http://cfpb.github.io/grasshopper-loader/arkansas.json"

  getGeoUrl(zip, new Counter(), function(err, file, stream, cb){
    if(typeof stream === 'function') cb = stream;
    t.equal(path.join(path.basename(path.dirname(file)), path.basename(file)), 'arkansas/t.shp', 'Shapefile extracted and passed from remote zip.');
    if(cb)cb();
  });

  getGeoUrl(json, new Counter(), function(err, file, stream, cb){
    t.equal(file, 'arkansas.json', 'GeoJson file pulled remotely.');
    t.ok(isStream(stream), 'GeoJson file streamed in');
  })
});

test('getGeoFiles module', function(t){
  t.plan(8); 
  
  ['shp', 'gdb', 'json'].forEach(function(v){
    var input = 'test/data/t.'+ v;
    getGeoFiles(input, new Counter(), function(err, file, cb) {
      t.equal(input, file, v + ' passed through to processData'); 
    });
  });

  getGeoFiles('test/data/threefiles', new Counter(), function(err, file, cb){
    t.ok(file, file + ' read from directory');
  }); 

  getGeoFiles('some.txt', new Counter(), function(err, file, cb){
    t.ok(err, 'Error produced on bad file type.');
  });

  try{
    getGeoFiles('fakepath', new Counter(), function(err, file, cb){
    });
  }catch(e){
    t.pass('Throws error on bad path.');
  }

});


test('ogrChild module', function(t){
  t.plan(6);
  
  var shp= 'test/data/t.shp';
  var shpChild = ogrChild(shp); 
  var json = 'test/data/new_york.json';
  var jsonChild = ogrChild(json, fs.createReadStream(json));
  var errInShp = 0;
  var errInJson = 0;
  var shpStats = streamStats('shp');
  var jsonStats = streamStats('json');
  
  t.ok(shpChild, 'ogrChild process is created');
  t.ok(isStream(shpChild.stdout), 'the child process has stdout');

  t.ok(jsonChild, 'ogrChild created for stream');
  t.ok(isStream(jsonChild.stdout), 'jsonChild produces an output stream');

  jsonChild.stderr.on('data',function(d){
    console.log(d.toString());
    errInJson = 1;
  });

  jsonChild.stderr.once('end',function(){
    t.notOk(errInJson, 'ogr2ogr doesn\'t emit an error from streamed GeoJson');
  });

  shpChild.stderr.on('data',function(d){
    console.log(d.toString());
    errInShp = 1;
  });

  shpChild.stderr.once('end',function(){
    t.notOk(errInShp, 'ogr2ogr doesn\'t emit an error from shapefile.');
  });

  shpChild.stdout.pipe(shpStats).sink();
  jsonChild.stdout.pipe(jsonStats).sink();

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
  var sep = makeBulkSeparator(program.index, program.type); 
  var sepObj = JSON.parse(sep.slice(0,sep.length-1));
  var expectedSep = {index:{_index:program.index,_type:program.type}}
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
  
  verify('test/data/t.json')(20, function(err){
    t.notOk(err, 'No error when featureCount equals passed value.'); 
  }); 

  verify('test/data/t.json')(10, function(err){
    t.ok(err.error, 'Produces an error when compared against the wrong number.'); 
    t.equal(err.actual, 20, 'Actual value propagated.');
    t.equal(err.expected, 10, 'Expected value propagated.');
  });
  
  verify('test/data/t.jsn')(10, function(err){
    t.ok(err.error, 'Produces an error when the file doesn\'t exist'); 
  }); 
  
});

test('resolveTransformer module', function(t){
  t.plan(4);
  var arkTrans = path.resolve('./transformers/arkansas.js');
  t.equal(arkTrans, resolveTransformer(null, 'arkansas.gdb'), 'Resolves transformer using filename');
  t.equal(arkTrans, resolveTransformer('./transformers/arkansas.js'), 'Resolves transformer using passed transformer');
  t.equal(arkTrans, resolveTransformer('./transformers/arkansas.js', 'sometext'), 'Resolver prefers passed transformer');
  t.equal(arkTrans, resolveTransformer('arkansas'), 'Resolves with state name only.');
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


test('pointTemplate module', function(t){
  t.plan(5);
  var trans = pointTransformer('addr','cty','st', 'zip');

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
    pointTransformer();
  }catch(e){
    t.pass('Calling the template without all arguments throws an error');
  }
});

test('Transformers', function(t){
  
  fs.readdir('transformers/',function(err, transformers){

    var pointFields = 'test/data/pointFields.json';
    var lineFields = 'test/data/lineFields.json';
    var bulkMatch = {index:{_index:'address',_type:'point'}}

    var pointMatch = {
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

    var lineMatch = {
      "type": "Feature",
      "properties":{
        "RFROMHn":"123",
        "RTOHN":"101",
        "FULLNAME":"a st",
        "CITY":"sunny",
        "ZIPL":"54321",
        "ZIPR":"54321",
        "load_date": 1234567890123
      },
      "geometry":{
        "type":"LineString",
        "coordinates":[
          [-129.1234,38.2],
          [-129.1235,38.2],
          [-129.1236,38.2]
        ]
      }
    }

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

    t.plan(filtered.length*4);

    filtered.forEach(function(transFile){
      var transformer = require(path.join('../transformers', transFile));
      var stats = streamStats(transFile, {store:1});
      var fields = transFile === 'tiger.js' ? lineFields : pointFields; 
      var match = transFile === 'tiger.js' ? lineMatch : pointMatch;

      fs.createReadStream(fields)
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
        t.ok(data.properties.load_date, "load_date added to output");
        t.ok(validAddresses.indexOf(data.properties.address) !== -1, "Address formed correctly for " + transFile);
        t.deepEqual(match.geometry, data.geometry, "Data to insert transformed correctly for " + transFile);

      });
    });

  }); 

});

test('Entire loader', function(t){
  t.plan(5);
  var args = [
    {ok:1, message: 'Ran without errors, exit code 0, on elasticsearch at ' + program.host + ':' + program.port, arr: ['./grasshopper-loader', '-d', './test/data/arkansas.json', '--host', program.host, '--port', program.port, '--index', program.index, '--type', program.type]},
    {ok:0, message: 'Bails when given an invalid file', arr: ['./grasshopper-loader', '-d', './test/data/ark.json', '--host', program.host, '--port', program.port, '--index', program.index, '--type', program.type]},
    {ok:0, message: 'Bails on bad file type', arr: ['./grasshopper-loader', '-d', './test/data/t.prj', '-t', 'transformers/arkansas.js', '--host', program.host, '--port', program.port, '--index', program.index, '--type', program.type]},
    {ok:1, message: 'Loads GeoJson from an S3 bucket.', arr: ['./grasshopper-loader', '-b', 'wyatt-test', '-d', 'new_york.json', '--profile','wyatt-test', '--host', program.host, '--port', program.port, '--index', program.index, '--type', program.type]},
    {ok:1, message: 'Loads a zipped shape from an S3 bucket.', arr: ['./grasshopper-loader', '-b', 'wyatt-test', '-d', 'arkansas.zip', '--host', program.host, '--port', program.port, '--index', program.index, '--type', program.type]},
  ];

  args.forEach(function(v,i){
    var loader = spawn('node', v.arr);
    var stats;

    if(v.ok){
      stats = streamStats('loader'+i,{store:1});
      loader.stderr.pipe(stats);
    }

    loader.on('exit', function(code){
      if(v.ok){
        t.equal(code, 0, v.message);
        if(stats.stats.store.length){
          console.log(stats.stats.store.toString());
        }
      }else{
        t.notEqual(code, 0, v.message);
      }
    });
  })
  
});
