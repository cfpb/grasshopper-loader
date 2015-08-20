var test = require('tape');
var fs = require('fs-extra');
var path = require('path');
var util = require('util');
var pump = require('pump');
var spawn = require('child_process').spawn;
var winston = require('winston');
var retriever = require('../lib/retriever');
var checkHash = require('../lib/checkHash');
var UploadStream = require('../lib/UploadStream');
var fieldFilter = require('../lib/fieldFilter');

var maine = 'test/data/retriever/maine.json';

var logger = new winston.Logger({
    transports: [
      new (winston.transports.Console)()
    ]
  });

logger.remove(winston.transports.Console);


test('checkHash module', function(t){
  t.plan(3);
  var stream = fs.createReadStream(maine);
  var hash = 'c7f2afb44fc9c2fbedd4ee32e67a8d0f31335461a29d44e67e537cece0120c18';

  checkHash(stream, hash, function(hashIsEqual, computedHash){
    t.ok(hashIsEqual, 'Computes proper hash');
    t.equal(computedHash, hash, 'Precomputed hash equals computed hash');
  });

  checkHash(stream, 'wronghash', function(hashIsEqual){
    t.notOk(hashIsEqual, 'Returns falsy if the hashes aren\'t equal.');
  });
});



test('uploadStream module', function(t){
  t.plan(7);

  var uploadStream = new UploadStream('wyatt-test', 'default');
  t.ok(uploadStream.S3, 'Creates and returns an S3 instance.');
  t.ok(uploadStream.credentials, 'Creates credentials object.');
  t.equal(uploadStream.bucket, 'wyatt-test', 'Saves reference to bucket.');

  try{
    new UploadStream();
  }catch(e){
    t.pass('Errors without a bucket passed in.');
  }

  var upload = uploadStream.stream( 'test/output/upload.json');

  pump(fs.createReadStream(maine), upload, function(err){
    t.notOk(err, 'No error on okay upload.');
  })
  .on('uploaded', function(details){
    t.ok(details, 'Returns upload details.');
  });

  var up = new UploadStream('fakebucketqwkMljeqhwegqw');
  var errStream = up.stream('qwdqqqqs/up.csv.gz');

  pump(fs.createReadStream(maine), errStream, function(){
    t.pass('Errors on uploading to bad bucket.');
  });

});



test('fieldFilter module', function(t){
  t.plan(4);

  var ncmeta = fs.readJsonSync('test/data/retriever/ncmeta.json');
  var count = 0;

  var cases = {
    "no_fields": {
      stream: fieldFilter(ncmeta.fields, logger),
      collection: [],
      count: 0
    },
    "empty_fields": {
      stream: fieldFilter(ncmeta.fields, logger),
      collection: [],
      count: 0
    },
    "spotty_fields": {
      stream: fieldFilter(ncmeta.fields, logger),
      collection: [],
      count: 3
    }
  }

  var noFields = fs.readJsonSync('test/data/retriever/no_fields.json');
  var emptyFields = fs.readJsonSync('test/data/retriever/empty_fields.json');
  var nc = fs.readJsonSync('test/data/fields/north_carolina.json');

  Object.keys(cases).forEach(function(v){
    var currCase = cases[v];
    currCase.stream.on('data', function(data){
      currCase.collection.push(data);
    });

    currCase.stream.on('end', function(){
      t.equal(currCase.collection.length, currCase.count, 'Got expected number of records.');
      after(++count);
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



test('retriever', function(t){

  t.plan(34);

  retriever({quiet: true, profile: 'default', directory: '.', file: 'nofile'}, function(output){
    t.equal(output.errors.length, 1, 'Errors on bad file and no bucket.');
  });

  retriever({quiet: true, profile: 'noprofilepresentfakeprofile', 'bucket': 'wyatt-test', file: maine}, function(output){
    var errLen = 1;
    if(process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY) errLen = 0;
    t.equal(output.errors.length, errLen, 'Errors on bad profile, only without environment variables set.');
  });

  retriever({quiet: true, profile: 'default', directory: '.', file: ''}, function(output){
    t.equal(output.errors.length, 1, 'Errors with no file passed.');
  });

  retriever({quiet: true, bucket: 'wyatt-test', profile: 'default', directory: '.', file: 'nofile'}, function(output){
    t.equal(output.errors.length, 1, 'Errors on bad file and good bucket.');
  });

  retriever({quiet: true, bucket: 'wyatt-test', profile: 'default', directory: '.', file: 'test/data/retriever/parent_dir.json'}, function(output){
    t.equal(output.errors.length, 1, 'Errors on parent dir in record name.');
  });

  retriever({quiet: true, bucket: 'wyatt-test', profile: 'default', directory: '.', file: 'test/data/retriever/slash.json'}, function(output){
    t.equal(output.errors.length, 1, 'Errors on forward slash in record name.');
  });

  retriever({quiet: true, bucket: 'fakebucketskjhqblwjdqwobdjabmznmbxbcbcnnbmcioqwOws', profile: 'default', directory: '.', file: maine}, function(output){
    t.equal(output.errors.length, 1, 'Error on bad bucket.');
  });

  retriever({quiet: true, bucket: 'wyatt-test', profile: 'default', directory: '.', file: maine}, function(output){
    t.equal(output.errors.length, 0, 'No error on good file and bucket.');
    t.equal(output.processed.length, 1, 'Loads data from the test dataset to bucket.');
    t.equal(output.location, 'wyatt-test/.', 'Keeps track of location, including bucket.');
  });

  retriever({quiet: true, profile: 'default', directory: 'test/output', file: maine}, function(output){
    t.equal(output.errors.length, 0, 'No error on good file.');
    t.equal(output.processed.length, 1, 'Loads data from test data locally.');
    t.equal(output.location, 'test/output', 'Keeps track of location, including bucket.');
  });

  retriever({quiet: true, bucket: 'wyatt-test', profile: 'default', directory: '.', file: maine, match: 'maine'}, function(output){
    t.equal(output.errors.length, 0, 'No error with match.');
    t.equal(output.processed.length, 1, 'Loads matched data.');
  });

  retriever({quiet: true, bucket: 'wyatt-test', profile: 'default', directory: '.', file: maine, match: 'nomatch'}, function(output){
    t.equal(output.errors.length, 0, 'No error with no match.');
    t.equal(output.processed.length, 0, 'Loads nothing when no data matched.');
  });

  retriever({quiet: true, profile: 'default', directory: 'test/output', file: 'test/data/retriever/mainejson.json'}, function(output){
    t.equal(output.errors.length, 0, 'No error on good json file.');
    t.equal(output.processed.length, 1, 'Loads data from json file.');
  });

  retriever({quiet: true, profile: 'default', directory: 'test/output', file: 'test/data/retriever/mainecsv.json'}, function(output){
    t.equal(output.errors.length, 0, 'No error on csv.');
    t.equal(output.processed.length, 1, 'Loads data from csv.');
  });

  retriever({quiet: true, profile: 'default', directory: 'test/output', file: 'test/data/retriever/mainezipcsv.json'}, function(output){
    t.equal(output.errors.length, 0, 'No error on zipped csv.');
    t.equal(output.processed.length, 1, 'Loads data from zipped csv.');
  });

  retriever({quiet: true, profile: 'default', directory: 'test/output', file: 'test/data/retriever/maineandarkanderr.json'}, function(output){
    t.equal(output.errors.length, 1, 'Hash error from file with hash error.')
    t.equal(output.processed.length, 3, 'Processes errors and successes alike.');
    t.equal(output.retrieved.length, 2, 'Loads data after hash error.');
    t.equal(output.stale.length, 1, 'Singles out stale data');
    t.equal(output.fresh.length, 2, 'Gets fresh data');
  });

  retriever({quiet: true, profile: 'default', directory: 'test/output', file: 'test/data/retriever/maineandarkandparenterr.json'}, function(output){
    t.equal(output.errors.length, 1, 'Parent dir error');
    t.equal(output.processed.length, 3, 'Processes errors and successes alike.');
    t.equal(output.retrieved.length, 2, 'Loads data after parent dir error.');
    t.equal(output.fresh.length, 2, 'Gets fresh data');
  });

  spawn('./retriever.js', ['-b', 'wyatt-test', '-p', 'default', '-d', '.', '-f', maine])
    .on('exit', function(code){
      t.equal(code, 0, 'Loads via cli');
    });

  spawn('./test/no-cb.js', ['-b', 'wyatt-test', '-p', 'default', '-d', '.', '-f', maine])
    .on('exit', function(code){
      t.equal(code, 0, 'Works without a callback.');
    });

});



test('Ensure output', function(t){
  t.plan(10);
  var count = 0;

 retriever({quiet: true, profile: 'default', directory: 'test/output', file: 'test/data/retriever/parcelsjson.json'}, function(output){
    t.equal(output.errors.length, 0, 'No error on converted parcels.')
    t.equal(output.processed.length, 1, 'Loads data from parcels');
    ensure(++count);
  });

  retriever({match: 'maine, arkansas', quiet: true, profile: 'default', directory: 'test/output', file: 'test/data/retriever/maineandarkanderr.json'}, function(output){
    t.equal(output.errors.length, 0, 'No error on filtered file.')
    t.equal(output.processed.length, 2, 'Loads data after filter.');
    ensure(++count);
  });

  function ensure(count){
    if(count < 2) return;

    var outfiles = [
      {file: 'test/output/arkansas.csv.gz', hash: '38af547a0147a0934f63bda7a4b6614e4b0bc4defca1ecd7eed9e4303fa7af59'},
      {file: 'test/output/maine.csv.gz', hash: '1af6790085e15625392157c2187a6e6624eaa3c1d856ee8531fe1873fe7548e7'},
      {file: 'test/output/sacramento.csv.gz', hash: '7f1be41d92041b0d5714fcb1f65a58d87efa3bb46681aa0c5160e7ff7701ae85'}
    ];

    outfiles.forEach(function(obj){
      var stream = fs.createReadStream(obj.file);

      checkHash(stream, obj.hash, function(hashIsEqual, computedHash){
        t.ok(hashIsEqual, 'Computes proper hash');
        t.equal(computedHash, obj.hash, 'Precomputed hash equals computed hash');
      });

    });
  }
});



test('Field tests', function(t){
  var data = fs.readJsonSync('data.json');
  var fieldFiles = {};

  t.plan(data.length*5);

  fs.readdirSync('test/data/fields')
    .filter(function(v){return v[0] !== '.'})
    .forEach(function(v){fieldFiles[path.basename(v, '.json')] = fs.readJsonSync(path.join('test/data/fields', v))});

  data.forEach(function(source){
    var fieldStream = fieldFilter(source.fields);

    var rawField = fieldFiles[source.name];

    t.ok(rawField, util.format('A test record exists in test/data/fields for %s', source.name));

    fieldStream.on('data', function(data){
      var props = data.properties;
      t.ok(props.Address, util.format('%s generates address', source.name));
      t.ok(props.City, util.format('%s generates city', source.name));
      t.ok(props.State, util.format('%s generates state', source.name));
      t.ok(props.Zip, util.format('%s generates zip', source.name));
    });

    fieldStream.end(fieldFiles[source.name]);
  });

});
