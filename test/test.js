var test = require('tape');
var fs = require('fs-extra');
var crypto = require('crypto');
var path = require('path');
var url = require('url');
var util = require('util');
var pump = require('pump');
var spawn = require('child_process').spawn;
var winston = require('winston');
var options = require('commander');
var isStream = require('isstream');
var streamStats = require('stream-stats');

var retriever = require('../lib/retriever');
var loader = require('../lib/loader');
var retrieverPipeline = require('../lib/retriever-pipeline');
var loaderPipeline = require('../lib/loader-pipeline');
var resolveOverrides = require('../lib/resolveOverrides');
var resolveFields = require('../lib/resolveFields');
var fieldFilter = require('../lib/fieldFilter');
var formatAddress = require('../lib/formatAddress');
var createIndex = require('../lib/createIndex');
var esLoader = require('../lib/esLoader');
var ogrChild = require('../lib/ogrChild');
var handleCsv = require('../lib/handleCsv');
var unzipFile = require('../lib/unzipFile');
var handleZip = require('../lib/handleZip');
var bulkPrefixer = require('../lib/bulkPrefixer');
var assureRecordCount = require('../lib/assureRecordCount');
var ftp = require('../lib/ftpWrapper');
var makeLogger = require('../lib/makeLogger');
var getTigerState = require('../lib/getTigerState');



//If linked to an elasticsearch Docker container
var esVar = process.env.ELASTICSEARCH_PORT;
var esHost = 'localhost';
var esPort = 9200;

if(esVar){
  esVar = esVar.split('//')[1].split(':');
  esHost = esVar[0];
  esPort = +esVar[1];
}




options
  .version('0.0.1')
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost', esHost)
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200', Number, esPort)
  .option('-a, --alias <alias>', 'Elasticsearch index alias. Defaults to testindex', 'testindex')
  .option('-t, --type <type>', 'Elasticsearch type within the provided or default index. Defaults to testtype', 'testtype')
  .option('-c, --concurrency', 'Concurrency of loader operations, defaults to 2', 2)
  .option('-b, --bucket <bucket>', 'An S3 bucket where the data overrides may be found.', 'grasshopper-loader-test')
  .option('-d, --directory <directory>', 'A directory where data overrides may be found, either relative to the current folder or the passed S3 bucket.', 'test/overrides')
  .option('-P, --profile <profile>', 'The aws credentials profile in ~/.aws/credentials. Will also respect AWS keys as environment variables.', 'default')
  .parse(process.argv);




var maine = 'test/data/metadata/maine.json';

var scratchSpace = 'scratch/' + crypto.pseudoRandomBytes(10).toString('hex');
fs.mkdirsSync(scratchSpace);


var logger = new winston.Logger({
    transports: [
      new (winston.transports.Console)()
    ]
  });

logger.remove(winston.transports.Console);
options.logger = logger;

var client = esLoader.connect(options.host, options.port, []);



test('resolveOverrides module', function(t){
 t.plan(15);

 resolveOverrides(options, function(err, overrides){
   t.notOk(err, 'No error on good override location in bucket');
   t.ok(overrides.list, 'Returns instance of overrides');

   var resolvedMaine = overrides.resolve('maine');
   var overrideStream = overrides.get('maine');

   t.equal(resolvedMaine, 'test/overrides/maine.zip');
   t.ok(isStream(overrideStream), 'Override streams from bucket');
 });

 var op2 = JSON.parse(JSON.stringify(options));
 delete op2.bucket;
 op2.directory = 'falsedirdoesntexist';

 resolveOverrides(op2, function(err, overrides){
   t.ok(err, 'Can\'t resolve overrides from invalid directory');
   t.ok(overrides.list, 'Returns instance of overrides even when an error exists');

   var resolvedMaine = overrides.resolve('maine');

  t.notOk(resolvedMaine, 'Can\'t resolve overrides from invalid directory');
 });

 var op3 = JSON.parse(JSON.stringify(options));

 resolveOverrides(op3, function(err, overrides){
   t.notOk(err, 'No error on good override local location');
   t.ok(overrides.list, 'Returns instance of overrides');

   var resolvedMaine = overrides.resolve('maine');
   var overrideStream = overrides.get('maine');

   t.equal(resolvedMaine, 'test/overrides/maine.zip');
   t.ok(isStream(overrideStream), 'Override streams locally');
 });

 var op4 = JSON.parse(JSON.stringify(options));
 delete op4.directory;

 resolveOverrides(op4, function(err, overrides){
   t.notOk(err, 'No error on good override with bucket but no directory');
   t.ok(overrides.list, 'Returns instance of overrides');

   var resolvedMaine = overrides.resolve('maine');
   var overrideStream = overrides.get('maine');

   t.equal(resolvedMaine, 'maine.zip');
   t.ok(isStream(overrideStream), 'Override streams locally');
 });
});



test('formatAddress module', function(t){
  t.plan(5);

  var add1 = '123 fake st. San Francisco CA 12345';
  var add2 = '221B Baker St. Arg AZ 67876';
  var add3 = '123 Unique Way';

  t.equal(formatAddress('123', 'fake st.', 'San Francisco', 'CA', '12345'), add1, 'Standard format')
  t.equal(formatAddress(123, 'fake st.', 'San Francisco', 'CA', 12345), add1, 'Numbers instead of strings')
  t.equal(formatAddress('221B', 'Baker St.', 'Arg', 'AZ', '67876'), add2, 'Non-number number');
  t.equal(formatAddress('123', 'Unique Way', '', null), add3, 'Gracefully handles lack of city, state, zip');
  t.equal(formatAddress('123', '', 'Yreka', 'CA'), null, 'Returns null on no street name');
});




test('makeLogger module', function(t){
  t.plan(2);
  var l1 = makeLogger({});
  var l2 = makeLogger({quiet: 1});
  t.ok(l1.transports.info, 'Logs at info level if not set to quiet.');
  t.ok(l2.transports.error, 'Logs at error level if set to quiet.');
});



test('esLoader module', function(t){
  t.plan(8);

  try{
    esLoader.connect();
  }catch(e){
    t.pass('Connect fails without host/port')
  }

  var op2 = JSON.parse(JSON.stringify(options));

  options.client = client;
  op2.client = client;
  op2.forcedIndex = 'testforcedindex';

  t.ok(client, 'Proper connect returns an elasticsearch client');

  esLoader.load({client: ''}, '', function(err){
    t.ok(err, 'Loader errors without proper arguments');
  });

  esLoader.load(options, 'somename', function(err, loader){
    t.notOk(err, 'Proper arguments to loader doesn\'t error');
    t.ok(isStream.isWritable(loader), 'esLoader.load returns a write stream');
  });

  esLoader.loadIntoIndex({client: ''}, function(err){
    t.ok(err, 'Loader errors without proper arguments');
  });


  esLoader.loadIntoIndex(op2, function(err, loader){
    t.notOk(err, 'Proper arguments to loader doesn\'t error');
    t.ok(isStream.isWritable(loader), 'esLoader.load returns a write stream');
  });

});




test('createIndex module', function(t){
  t.plan(3);

  var client = options.client;

  createIndex({alias: 'testalias', client: client}, 'testname', function(err, index){
    if(err) t.fail();
    t.ok(index, 'Assigns index for arbitrary alias.');
  });

  createIndex({alias: 'census', client: client}, 'testname', function(err, index){
    if(err) t.fail();
    t.ok(index, 'Creates index for census.');
    client.indices.delete({index: index}, function(err){
      t.notOk(err, 'Deletes test index');
    });
  });
});




test('bulk Prefixer module', function(t){
  t.plan(3);

  t.ok(isStream.isDuplex(bulkPrefixer()), 'bulkPrefixer returns a stream');

  var pref = bulkPrefixer();
  var pref2 = bulkPrefixer();
  var prefixed = '{"index":{}}\nsomedata\n';

  pref.end('somedata\n');
  pref2.end('somedata');

  pref.on('data', function(data){
    t.equal(data.toString(), prefixed, 'Properly prefixes newline-delimited data');
  });

  pref2.on('data', function(data){
    t.equal(data.toString(), prefixed, 'Properly prefixes data with missing newline');
  });
});




test('handleCsv module', function(t){
  t.plan(6);
  var csvFile = 'test/data/virginia.csv';
  var txtFile = 'test/data/virginia.txt';
  var csvStream = fs.createReadStream(csvFile);
  var txtStream = fs.createReadStream(txtFile);
  var txtRecord = {name: 'virginia', file: 'virginia.txt', spatialReference: 'NAD83'}
  var csvRecord = {name: 'virginia', file: 'virginia.csv', spatialReference: 'NAD83'}
  var badTxtRecord = {name: 'virginia', file: 'virginia.txt'};

  handleCsv(csvFile, csvRecord, scratchSpace, function(vrt){
    t.ok(vrt, 'Creates a valid vrt file from a csv file and good record.');
  },
  function(stream, err){
   if(err) t.fail(err);
  });

  handleCsv(csvStream, csvRecord, scratchSpace, function(vrt){
    t.ok(vrt, 'Creates a valid vrt file from a csv stream and good record.');
  },
  function(stream, err){
   if(err) t.fail(err);
  });

  handleCsv(txtFile, txtRecord, scratchSpace, function(vrt){
    t.ok(vrt, 'Creates a valid vrt file from a text file and good record.');
    fs.copySync('test/data/virginia.csv', 'test/data/virginia.txt');
  },
  function(stream, err){
   if(err) t.fail(err);
  });

  handleCsv(txtStream, txtRecord, scratchSpace, function(vrt){
    t.ok(vrt, 'Creates a valid vrt file from a text stream and good record.');
  },
  function(stream, err){
   if(err) t.fail(err);
  });

  handleCsv(txtFile, badTxtRecord, scratchSpace, function(){
  },
  function(stream, err){
    t.ok(err, 'Errors without a spatial reference.');
  });

  handleCsv('fake', csvRecord, scratchSpace, function(){
  },
  function(stream, err){
    t.ok(err, 'Fails with bad filename.');
  });
});




test('unzipFile module', function(t){
 t.plan(3);
 var zipfile = 'test/data/arkansas.zip';
 var badfile = 'test/data/nofilehere.notzip';
 var record = {name: 'arkansas', file: 'arkansas.zip'};
 var badRecord = {name: 'arkansas'};

 var dir1 = path.join(scratchSpace, crypto.pseudoRandomBytes(10).toString('hex'));
 var dir2 = path.join(scratchSpace, crypto.pseudoRandomBytes(10).toString('hex'));
 var dir3 = path.join(scratchSpace, crypto.pseudoRandomBytes(10).toString('hex'));

 unzipFile(zipfile, record, dir1, function(file){
   t.ok(file, 'Successfully unzips file.');
 },
 function(record, err){
   t.notOk(err);
 });

 unzipFile(badfile, record, dir2, function(file){
   t.notOk(file);
 },
 function(record, err){
   t.ok(err, 'Error on bad file.');
 });

 unzipFile(zipfile, badRecord, dir3, function(file){
   t.notOk(file);
 },
 function(record, err){
   t.ok(err, 'Error without complete record.');
 });
});




test('handleZip module', function(t){
  t.plan(3);

  var arkRecord = {name: 'arkansas', file: 'arkansas.zip'};
  var virgRecord = {name: 'virg', file: 'virg.csv', spatialReference: 'NAD83'};

  handleZip(fs.createReadStream('test/data/arkansas.zip'), arkRecord, scratchSpace,
    function(file){
      t.ok(file, 'handleZip works on zipped shapefile.');
    }, function(stream, err){t.notOk(err)});

  handleZip(fs.createReadStream('test/data/virg.zip'), virgRecord, scratchSpace,
    function(file){
      t.ok(file, 'handleZip works on zipped csv.');
    }, function(stream, err){t.notOk(err)})

  handleZip(fs.createReadStream('test/data/virginia.csv'), virgRecord, scratchSpace,
    function(file){
      t.notOk(file);
    }, function(stream, err){t.ok(err, 'Errors on bad file.')})
});




test('assureRecordCount module', function(t){
  t.plan(5);

  var countOnly = {count: 123};
  var deriveCount = {};

  assureRecordCount(deriveCount, 'test/data/arkansas.json', function(err){
    t.notOk(err, 'Doesn\'t err on good file and args.');
    t.equal(deriveCount.count, 19, 'Gets the appropriate count.');
  });

  assureRecordCount(countOnly, 'doesn\'t matter', function(err){
    t.notOk(err, 'Doesn\'t err on good args and count present.');
    t.equal(countOnly.count, 123, 'Count remains the same.');
  });

  assureRecordCount({}, 'test/data/fakebadfile', function(err){
    t.ok(err, 'Errs on bad file if no count provided.');
  });
});




//These tests will pass even if the ftp server isn't reachable
test('ftpWrapper module', function(t){
  t.plan(4);
/*
  var globalClient;
  ftp.connect(url.parse('ftp://ftp2.census.gov/geo/tiger/TIGER2015/ADDRFEAT/'), function(err, client){
    if(err){
      console.warn('Could not connect to ftp test server');
      t.pass();
    }else{
      t.ok(client.get, 'Returns a valid client');
      globalClient = client;
    }

    ftp.connect(url.parse('ftp://ftp2.census.gov/geo/tiger/TIGER2015/ADDRFEAT/'), function(err, client){
      if(err){
        console.warn('Could not connect to ftp test server when trying to connect multiple times');
        t.pass();
      }else{
        t.deepEqual(client, globalClient, 'Reuses client if it exists');
      }
    },
    function(err){
      t.fail(err);
    });

    ftp.list(url.parse('ftp://ftp2.census.gov/geo/tiger/TIGER2015/ADDRFEAT/'), function(err, endpoints){
      if(err){
        console.warn('Could not connect to ftp test server when listing');
        t.pass();
      }else{
        t.ok(endpoints.length > 3000, 'Lists all files at endpoint');
      }
    });
*/
    ftp.list(url.parse('ftp://ftp2.fakeaddr.gov/geo/tiger/TIGER2015/ADDRFEAT/'), function(err){
      t.ok(err, 'Error when listing from invalid client');
    });
/*
    ftp.request(url.parse('ftp://ftp2.census.gov/geo/tiger/TIGER2015/ADDRFEAT/tl_2015_01011_addrfeat.zip'), function(err, stream){
      if(err){
        console.warn('Could not connect to ftp test server on specific file request');
        t.pass();
      }else{
        t.ok(isStream(stream), 'Returns file stream');
      }
    });
*/
    ftp.request(url.parse('ftp://ftp2.fake.gov/geo/tiger/TIGER2015/ADDRFEAT/'), function(err){
      t.ok(err, 'Error when requesting from invalid client');
    });
/*
  },
  function(err){
    t.fail(err);
  });
*/
  ftp.connect(url.parse('ftp://ftp2.alsofake.gov/geo/tiger/TIGER2015/ADDRFEAT/'), function(err){
      t.ok(err, 'Passes through client error to callback');
    },
    function(err){
      t.ok(err, 'Client error on invalid ftp');
  });

});




test('ftpWrapper addendum', function(t){
  t.plan(1);
  try{
    ftp.closeClients();
    t.pass('Closes clients without error');
  }catch(e){
    t.fail('Error closing clients');
  }
});




test('resolveFields module', function(t){
  t.plan(6);

  var ncmeta = fs.readJsonSync('test/data/metadata/ncmeta.json');
  var ncsep = fs.readJsonSync('test/data/metadata/ncmetaSep.json');
  var nc = fs.readJsonSync('test/data/fields/north_carolina.json');

  var resolved = resolveFields(nc.properties, ncmeta.fields);
  var resolvedWithSep = resolveFields(nc.properties, ncsep.fields);

  t.equal(resolved.Street, 'CENTER STAGE COURT', 'Resolves and joins street');
  t.equal(resolved.State, 'NC', 'Resolves state');
  t.equal(resolved.Zip, '27127', 'Resolves zip');

  t.equal(resolvedWithSep.Street, 'CENTER STAGESEPARATORCOURT', 'Resolves street with separator');

  try{
    resolveFields(nc.properties, {});
  }catch(e){
    t.ok(e, 'Throws with bad fields.');
  }

  try{
    resolveFields(nc.properties, {'Address': {type: 'dynamic', value: 'garbage string'}});
  }catch(e){
    t.ok(e, 'Throws on bad dynamic value');
  }
});




test('fieldFilter module', function(t){
  t.plan(6);

  fieldFilter.setLogger(logger);

  var ncmeta = fs.readJsonSync('test/data/metadata/ncmeta.json');
  var count = 0;

  var cases = {
    "no_fields": {
      stream: fieldFilter(ncmeta),
      collection: [],
      count: 0
    },
    "empty_fields": {
      stream: fieldFilter(ncmeta),
      collection: [],
      count: 0
    },
    "spotty_fields": {
      stream: fieldFilter(ncmeta),
      collection: [],
      count: 3
    }
  }

  var noFields = fs.readJsonSync('test/data/fields/test_fields/no_fields.json');
  var emptyFields = fs.readJsonSync('test/data/fields/test_fields/empty_fields.json');
  var nc = fs.readJsonSync('test/data/fields/north_carolina.json');

  Object.keys(cases).forEach(function(v){
    var currCase = cases[v];
    currCase.stream.on('data', function(data){
      currCase.collection.push(data);
    });

    currCase.stream.on('end', function(){
      t.equal(currCase.collection.length, currCase.count, 'Got expected number of records.');
      after(++count);
    });

    currCase.stream.on('error', function(err){
      t.ok(err, 'Error on bad fields');
    })
  });

  cases.no_fields.stream.end(noFields);
  cases.empty_fields.stream.end(emptyFields);
  cases.spotty_fields.stream.write(nc);
  cases.spotty_fields.stream.write(emptyFields);
  cases.spotty_fields.stream.write(nc);
  cases.spotty_fields.stream.write(nc);
  cases.spotty_fields.stream.end(noFields);

  function after(count){
    if(count===3){
      t.pass('Processed all files without error.');
    }
  }

});




test('getTigerState module', function(t){
  t.plan(3);
  var CA = getTigerState('tl_2014_06071_addrfeat.zip');
  var nomatch = getTigerState('somefile');
  var badmatch = getTigerState('tl_2014_99071_addrfeat.zip');

  t.equal(CA, 'CA', 'Gets state for a valid TIGER file');
  t.equal(nomatch, undefined, 'Returns undefined when FIPS isn\'t matched');
  t.equal(badmatch, undefined, 'Returns undefined when FIPS isn\'t valid');
});



test('ogrChild module', function(t){
  t.plan(6);

  var shp= 'test/data/arkansas/arkansas.shp';
  var shpChild = ogrChild(shp);
  var json = 'test/data/arkansas.json';
  var jsonChild = ogrChild(json, fs.createReadStream(json));
  var errInShp = 0;
  var errInJson = 0;
  var shpStats = streamStats('shp');
  var jsonStats = streamStats('json');

  t.ok(shpChild, 'ogrChild process is created');
  t.ok(isStream(shpChild.stdout), 'the child process has stdout');

  t.ok(jsonChild, 'ogrChild created for stream');
  t.ok(isStream(jsonChild.stdout), 'jsonChild produces an output stream');

  jsonChild.stderr.on('data', function(d){
    logger.error(d.toString());
    errInJson = 1;
  });

  jsonChild.stderr.once('end', function(){
    t.notOk(errInJson, 'ogr2ogr doesn\'t emit an error from streamed GeoJson');
  });

  shpChild.stderr.on('data', function(d){
    logger.error(d.toString());
    errInShp = 1;
  });

  shpChild.stderr.once('end', function(){
    t.notOk(errInShp, 'ogr2ogr doesn\'t emit an error from shapefile.');
  });

  pump(shpChild.stdout, shpStats, shpStats.sink());
  pump(jsonChild.stdout, jsonStats, jsonStats.sink());

});




test('retriever-pipeline module', function(t){
  t.plan(8);
  var record = fs.readJsonSync('test/data/metadata/ncmeta.json');
  var ncjson = '{"type":"Feature","geometry":{"type":"Point","coordinates":[-80.23539,36.07191]},"properties":{"address":"191 CENTER STAGE COURT WINSTON SALEM NC 27127","city":"WINSTON SALEM","number":"191","state":"NC","street":"CENTER STAGE COURT","zip":"27127"}}'

  var pipeline = retrieverPipeline(record, 'test/data/fields/north_carolina.json');
  var pipeStats = streamStats('pipeline', {store: 1});
  var pipe2= retrieverPipeline(record, null, fs.createReadStream('test/data/fields/north_carolina.json'));
  var pipeStats2 = streamStats('pipe2', {store: 1});
  var badPipeline = retrieverPipeline(record, 'qweqgjwegvhqxhgbjqkwq');

  t.ok(isStream(pipeline), 'Returns a stream');
  t.ok(isStream(pipe2), 'Returns a stream when handed a stream');
  t.ok(isStream(badPipeline), 'Bad pipeline still returns stream');

  function trunc(n, digits){
    var shift = Math.pow(10, digits);
    var func = n < 0 ? Math.ceil : Math.floor;
    return func(n * shift)/shift;
  }

  function checkPipes(pipeline, stats){
    pump(pipeline, stats, stats.sink(), function(err){
      t.notOk(err, 'Doesn\'t err in a correct pipeline');

      var store = JSON.parse(stats.stats.store.toString());
      store.geometry.coordinates[0] = trunc(store.geometry.coordinates[0], 5)
      store.geometry.coordinates[1] = trunc(store.geometry.coordinates[1], 5)
      store = JSON.stringify(store);

      t.deepEqual(JSON.parse(store), JSON.parse(ncjson), 'Transforms properly in retriever pipeline when file is streamed');
    });
  }

  checkPipes(pipeline, pipeStats);
  checkPipes(pipe2, pipeStats2);

  badPipeline.once('error', function(err){
    t.ok(err, 'Passes through error when encountered in the pipeline');
  });

});




test('loader-pipeline module', function(t){
  t.plan(5);

  var record = fs.readJsonSync('test/data/metadata/ncmeta.json');

  var client = options.client;
  var op1 = options;
  op1.client = null;
  var op2 = JSON.parse(JSON.stringify(options));
  op1.client = client;
  op2.client = client;
  op2.forcedIndex = 'testforcedindex';

  var str1 = retrieverPipeline(record, 'test/data/fields/north_carolina.json');
  var str2 = retrieverPipeline(record, 'test/data/fields/north_carolina.json');
  var str3 = retrieverPipeline(record, 'test/data/fields/north_carolina.json');

  loaderPipeline(op1, str1, record, function(err, loader){
    t.notOk(err, 'No error on valid loader pipeline with derived index.');
    t.ok(loader, 'Returns loader stream');
    loader.on('error', function(err){t.fail(err)});
  });

  loaderPipeline(op2, str2, record, function(err, loader){
    t.notOk(err, 'No error on valid loader pipeline with forced index.');
    t.ok(loader, 'Returns loader stream');
    loader.on('error', function(err){t.fail(err)});
  });

  loaderPipeline({}, str3, record, function(err){
    t.ok(err, 'Error propagated on bad options.');
  });

});




test('loader', function(t){
  t.plan(4);

  var record = fs.readJsonSync('test/data/metadata/ncmeta.json');

  var client = options.client;
  var op1 = options;
  op1.client = null;
  var op2 = JSON.parse(JSON.stringify(options));
  op1.client = client;
  op2.client = client;
  op2.logger = logger;
  op2.forcedIndex = 'testforcedindex';

  var str1 = retrieverPipeline(record, 'test/data/fields/north_carolina.json');
  var str2 = retrieverPipeline(record, 'test/data/fields/north_carolina.json');
  var str3 = retrieverPipeline(record, 'test/data/fields/north_carolina.json');
  var str4 = retrieverPipeline(record, 'test/data/fields/north_carolina.json');

  loader(op1, str1, record, function(err){
    t.notOk(err, 'Pipeline 1 loads without error');
  });

  loader(op2, str2, record, function(err){
    t.notOk(err, 'Pipeline 2 loads without error');
  });

  loader({logger: logger}, str3, record, function(err){
    t.ok(err, 'Pipeline 3 errors on bad options');
  });

  loader(op1, str4, {}, function(err){
    t.ok(err, 'Pipeline 4 errors on bad record');
  });

});




test('retriever', function(t){
  t.plan(31);

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, profile: options.profile, directory: options.directory, file: 'nofile'}, function(output){
    if(output.errors.length !== 1) console.log(output.errors);
    t.equal(output.errors.length, 1, 'Errors on bad file and no bucket.');
  });

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, profile: 'noprofilepresentfakeprofile', bucket: options.bucket, file: maine}, function(output){
    t.equal(output.errors.length, 0, 'No hard errors on bad profile.');
  });

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, profile: options.profile, directory: options.directory, file: ''}, function(output){
    if(output.errors.length !== 1) console.log(output.errors);
    t.equal(output.errors.length, 1, 'Errors with no file passed.');
  });

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, bucket: options.bucket, profile: options.profile, directory: options.directory, file: 'nofile'}, function(output){
    if(output.errors.length !== 1) console.log(output.errors);
    t.equal(output.errors.length, 1, 'Errors on bad file and good bucket.');
  });

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, bucket: options.bucket, profile: options.profile, directory: options.directory, file: 'test/data/metadata/parent_dir.json'}, function(output){
    if(output.errors.length !== 1) console.log(output.errors);
    t.equal(output.errors.length, 1, 'Errors on parent dir in record name.');
  });

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, bucket: options.bucket, profile: options.profile, directory: options.directory, file: 'test/data/metadata/slash.json'}, function(output){
    if(output.errors.length !== 1) console.log(output.errors);
    t.equal(output.errors.length, 1, 'Errors on forward slash in record name.');
  });

  retriever({concurrency: 2, client: client, host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, bucket: 'fakebucketskjhqblwjdqwobdjabmznmbxbcbcnnbmcioqwOws', profile: options.profile, directory: options.directory, file: maine}, function(output){
    if(output.errors.length) console.log(output.errors);
    t.equal(output.errors.length, 0, 'No hard error on bad bucket.');
  });

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, bucket: options.bucket, profile: options.profile, directory: options.directory, file: maine}, function(output){
    if(output.errors.length !== 0) console.log(output.errors);
    t.equal(output.errors.length, 0, 'No error on good file and bucket.');
    t.equal(output.loaded.length, 1, 'Loads data with backups.');
  });

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, profile: options.profile, directory: options.directory, file: maine}, function(output){
    if(output.errors.length !== 0) console.log(output.errors);
    t.equal(output.errors.length, 0, 'No error on good file.');
    t.equal(output.loaded.length, 1, 'Loads data from test data with local backups.');
  });

  retriever({concurrency: 1, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, profile: options.profile, file: maine}, function(output){
    if(output.errors.length !== 0) console.log(output.errors);
    t.equal(output.errors.length, 0, 'No error on limited concurrency.');
    t.equal(output.loaded.length, 1, 'Loads data with limited concurrency.');
  });

  retriever({concurrency: 20, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, profile: options.profile, file: maine}, function(output){
    if(output.errors.length !== 0) console.log(output.errors);
    t.equal(output.errors.length, 0, 'No error on large concurrency.');
    t.equal(output.loaded.length, 1, 'Loads data when concurrency is large.');
  });

  retriever({concurrency: 'qwe', client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, profile: options.profile, file: maine}, function(output){
    if(output.errors.length !== 1) console.log(output.errors);
    t.equal(output.errors.length, 1, 'Error with bad concurrency.');
  });

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, profile: options.profile, directory: options.directory, file: 'test/data/metadata/private_maine.json'}, function(output){
    if(output.errors.length !== 0) console.log(output.errors);
    t.equal(output.errors.length, 0, 'No error when no source is provided with a proper override.');
    t.equal(output.processed.length, 1, 'Data correctly processed.');
  });

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, bucket: options.bucket, profile: options.profile, directory: options.directory, file: maine, match: 'maine'}, function(output){
    if(output.errors.length !== 0) console.log(output.errors);
    t.equal(output.errors.length, 0, 'No error with match.');
    t.equal(output.loaded.length, 1, 'Loads matched data.');
  });

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, bucket: options.bucket, profile: options.profile, directory: options.directory, file: maine, match: 'nomatch'}, function(output){
    if(output.errors.length !== 0) console.log(output.errors);
    t.equal(output.errors.length, 0, 'No error with no match.');
    t.equal(output.loaded.length, 0, 'Loads nothing when no data matched.');
  });

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, profile: options.profile, directory: options.directory, file: 'test/data/metadata/mainejson.json'}, function(output){
    if(output.errors.length !== 0) console.log(output.errors);
    t.equal(output.errors.length, 0, 'No error on good json file.');
    t.equal(output.loaded.length, 1, 'Loads data from json file.');
  });

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, profile: options.profile, file: 'test/data/metadata/mainecsv.json'}, function(output){
    if(output.errors.length !== 0) console.log(output.errors);
    t.equal(output.errors.length, 0, 'No error on csv.');
    t.equal(output.loaded.length, 1, 'Loads data from csv.');
  });

  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, profile: options.profile, file: 'test/data/metadata/mainezipcsv.json'}, function(output){
    if(output.errors.length !== 0) console.log(output.errors);
    t.equal(output.errors.length, 0, 'No error on zipped csv.');
    t.equal(output.loaded.length, 1, 'Loads data from zipped csv.');
  });
/*Travis isn't playing nicely
  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, profile: options.profile, file: 'test/data/metadata/maineandarkanderr.json'}, function(output){
    if(output.errors.length !== 1) console.log(output.errors);
    t.equal(output.errors.length, 1, 'Schema error from file with schema error.')
    t.equal(output.processed.length, 3, 'Processes errors and successes alike.');
    t.equal(output.loaded.length, 2, 'Loads data after schema error.');
  });
*/
  retriever({concurrency: 2, client: client, log: 'error', host: options.host, port: options.port, alias: options.alias, type: options.type, quiet: true, logger: logger, profile: options.profile, file: 'test/data/metadata/maineandarkandparenterr.json'}, function(output){
    if(output.errors.length !== 1) console.log(output.errors);
    t.equal(output.errors.length, 1, 'Parent dir error');
    t.equal(output.processed.length, 3, 'Processes errors and successes alike.');
    t.equal(output.loaded.length, 2, 'Loads data after parent dir error.');
  });
});




test('Cli tests', function(t){
  t.plan(4);

  spawn('./index.js', ['-l', 'error', '-h', options.host, '-p', options.port, '-a', options.alias, '-t', options.type, '-b', options.bucket, '--profile', options.profile, '-d', options.directory, '-f', maine])
    .on('exit', function(code){
      t.equal(code, 0, 'Loads via cli');
    })
    .stderr.once('data', function(data){
      console.log(data.toString());
    });
/*Travis isn't playing nicely
  spawn('./index.js', ['-l', 'error', '-h', options.host, '-p', options.port, '-a', options.alias, '-t', options.type, '-b', options.bucket, '--profile', options.profile, '-d', options.directory, '-f', 'test/data/metadata/maineandarkanderr.json'])
    .on('exit', function(code){
      t.equal(code, 0, 'Errors are captured in cli');
    })
*/
  spawn('./test/no-cb.js', ['-l', 'debug', '-h', options.host, '-p', options.port, '-a', options.alias, '-t', options.type, '-b', options.bucket, '--profile', options.profile, '-d', options.directory, '-f', maine])
    .on('exit', function(code){
      t.equal(code, 0, 'Works without a callback.');
    })
    .stderr.once('data', function(data){
      console.log(data.toString());
    });

  spawn('./tiger.js', ['-l', 'error', '-h', options.host, '-p', options.port, '-a', options.alias, '-t', options.type, '-d', './test/data/tiger'])
    .on('exit', function(code){
      t.equal(code, 0, 'Tiger works via cli');
    })
    .stderr.once('data', function(data){
      console.log(data.toString());
    });

  spawn('./tiger.js', ['-c', '1', '-l', 'error', '-h', options.host, '-p', options.port, '-a', options.alias, '-t', options.type, '-d', './test/data/tiger'])
    .on('exit', function(code){
      t.equal(code, 0, 'Tiger works with bounded concurrency');
    })
    .stderr.once('data', function(data){
      console.log(data.toString());
    });

});




test('Field tests', function(t){
  var data = fs.readJsonSync('data.json');
  var fieldFiles = {};

  fs.readdirSync('test/data/fields')
    .filter(function(v){return v[0] !== '.' && v.indexOf('.') !== -1})
    .forEach(function(v){fieldFiles[path.basename(v, '.json')] = fs.readJsonSync(path.join('test/data/fields', v))});

  data.forEach(function(source){

    //Non-public
    if(!url.parse(source.url).hostname) return;

    var fieldStream = fieldFilter(source);

    var rawField = fieldFiles[source.name];

    t.ok(rawField, util.format('A test record exists in test/data/fields for %s', source.name));

    fieldStream.on('data', function(data){
      var props = data.properties;
      t.ok(props.address, util.format('%s generates address', source.name));
    });

    fieldStream.on('error', function(err){
      t.fail(util.format('%s failed with %s', source.name, err));
    })

    fieldStream.end(fieldFiles[source.name]);
  });

  t.end();
});




test('Synonyms check', function(t){
  t.plan(4);

  var expectedSettings = fs.readJsonSync('./synonyms.json');

  client.indices.getSettings({index: options.index}, function(err, settings){
    if(err) t.fail('Error retrieving index settings. Something is wrong.');
    var settingsArr = Object.keys(settings);
    var addrpointSettings;
    var tigerSettings;

    for(var i=0; i<settingsArr.length; i++){
      var name = settingsArr[i];
      var analysis = settings[name].settings.index.analysis

      if(!analysis) continue;

      if(name.match('tiger')){
        tigerSettings = analysis.filter;
      }else{
        addrpointSettings = analysis.filter;
      }
    }

    t.deepEqual(addrpointSettings.state_synonyms.synonyms, expectedSettings.state_synonyms, 'Addrpoint state synonyms applied correctly');
    t.deepEqual(addrpointSettings.address_synonyms.synonyms, expectedSettings.address_synonyms, 'Addrpoint address synonyms applied correctly');
    t.deepEqual(tigerSettings.state_synonyms.synonyms, expectedSettings.state_synonyms, 'Tiger state synonyms applied correctly');
    t.deepEqual(tigerSettings.address_synonyms.synonyms, expectedSettings.address_synonyms, 'Tiger address synonyms applied correctly');
  });
});




test('Cleanup', function(t){
  t.plan(3);
  client.close();
  t.pass('Elasticsearch client closed');
  fs.removeSync(scratchSpace);
  t.pass('scratchSpace removed');
  fs.removeSync('test/data/virginia.vrt');
  t.pass('Remove generated vrt');
})
