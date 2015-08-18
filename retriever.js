var fs = require('fs-extra');
var util = require('util');
var path = require('path');
var crypto = require('crypto');
var url = require('url');
var spawn = require('child_process').spawn;
var winston = require('winston');
var pump = require('pump');
var eos = require('end-of-stream');
var request = require('request');
var OgrJsonStream = require('ogr-json-stream');
var ftp = require('ftp');
var zlib = require('zlib');
var yauzl = require('yauzl');
var csvToVrt = require('csv-to-vrt');
var centroidStream = require('centroid-stream');
var UploadStream = require('./lib/UploadStream');
var checkHash = require('./lib/checkHash');
var fieldFilter = require('./lib/fieldFilter');

var zipReg = /.zip$/i;
var csvReg = /(?:txt|csv)$/i;
var restrictedReg = /\.\.|\//;
var comma = new Buffer(',');


function retrieve(program, callback){

  var output = {
    errors: [],
    fresh: [],
    stale: [],
    processed: [],
    retrieved: [],
    startTime: Date.now(),
    endTime: null,
    location: ''
  };

  var scratchSpace = 'scratch/' + crypto.pseudoRandomBytes(10).toString('hex');
  fs.mkdirsSync(scratchSpace);


  var logger = new winston.Logger({
    transports: [
      new (winston.transports.Console)()
    ]
  });

  if(program.quiet){
    logger.remove(winston.transports.Console);
  }


  function wrappedCb(err){
    if(err) output.errors.push(err);

    try{
      fs.removeSync(scratchSpace);
    }catch(e){
      if(e) output.errors.push(e);
    }

    output.endTime = Date.now();

    if(output.errors.length && !callback) throw output.errors.join('\n');

    if(callback) callback(output);
  }


  if(!program.file) return wrappedCb(new Error('Must provide a metadata file with the -f option.'));

  var monitoringMode = !program.bucket && !program.directory;
  var stringMatch = typeof program.match === 'string';
  var regMatch = typeof program.match === 'object';

  var uploadStream;
  var data;

  try{
    data = JSON.parse(fs.readFileSync(program.file));
  }catch(err){
    return wrappedCb(err);
  }

  var recordCount = data.length;

  function recordCallback(err, record){
    output.processed.push(record.name);

    if(err){
      logger.error(err);
      output.errors.push(err);
    }else{
      output.retrieved.push(record.name);
    }
    if(output.processed.length === recordCount) wrappedCb(null);
  }


  if(program.bucket) uploadStream = new UploadStream(program.bucket, program.profile);

  data.forEach(function(record){

    //Don't allow to traverse to other folders via data.json
    if(restrictedReg.test(record.name)){
      return recordCallback(new Error(util.format('Invalid record name %s. Must not contain ".." or "/".', record.name)), record);
    }

    //If the record is filtered, remove it from the count
    if(stringMatch && program.match.indexOf(record.name) === -1 ||
      regMatch && !program.match.test(record.name)
    ){
      if(--recordCount === output.processed.length){
        return wrappedCb(null);
      }
      return recordCount;
    }

    var urlObj = url.parse(record.url);

    if(urlObj.protocol === 'ftp:'){

      var ftpClient = new ftp();

      ftpClient.on('ready', function(){
        ftpClient.get(urlObj.path, function(err, stream){

          if(err){
            ftpClient.end();
            return recordCallback(err, record);
          }

          eos(stream, function(){
            ftpClient.end();
          });

          processRequest(stream, record);
        });
      });

      ftpClient.connect({host: urlObj.hostname});

    }else{
      processRequest(request(record.url), record);
    }
  });


  function processRequest(stream, record){
    checkHash(stream, record.hash, function(hashIsEqual, remoteHash){
      if(hashIsEqual){
        logger.info('Remote file for %s verified.', record.name);
        record._retrieverVerified = 1;
        output.fresh.push(record.name);

        //if no output or stream has already completed
        if(monitoringMode || record._retrieverProcessed) return recordCallback(null, record);
        return;
      }
      output.stale.push(record.name);
      stream.emit('error', new Error('The hash from ' + record.name + ' did not match the downloaded file\'s hash.\nRecord hash: ' + record.hash +'\nRemote hash: ' + remoteHash +'\n'));
    });

    stream.on('error', handleStreamError.bind(stream, record));

    if(monitoringMode) return;

    if(zipReg.test(record.url)){
      var zipdir = path.join(scratchSpace, record.name);
      var zipfile = zipdir + '.zip';

      pump(stream, fs.createWriteStream(zipfile), function(err){
        if(err) return stream.emit('error', err);

        yauzl.open(zipfile, function(err, zip){
          if(err) return recordCallback(err, record);
          var entriesFinished = 0;
          var count = 0;

          zip.on('end', function(){
            entriesFinished = 1;
          });

          zip.on('entry', function(entry){
            if(/\/$/.test(entry.fileName)) return;

            zip.openReadStream(entry, function(err, readStream) {
              if(err) return handleStreamError.call(this, record, err);
              count++;
              var output = fs.createOutputStream(path.join(zipdir, entry.fileName));

              pump(readStream, output, function(err){
                if(err) return recordCallback(err, record);
                count--;
                if(entriesFinished && !count){
                  var unzipped = path.join(zipdir, record.file);

                  if(csvReg.test(record.file)){

                    csvToVrt(unzipped, record.sourceSrs, function(err, vrt){
                      if(err) return recordCallback(err, record);
                      handleStream(spawnOgr(vrt), record);
                    });

                  }else{
                    handleStream(spawnOgr(unzipped), record);
                  }
                }
              });
            });
          });
        });
      });
    }else{
      if(csvReg.test(record.file)){
        var csv = path.join(scratchSpace, record.file);
        var csvStream = fs.createWriteStream(csv);

        pump(stream, csvStream, function(err){
          if(err) return recordCallback(err, record);

          csvToVrt(csv, record.sourceSrs, function(err, vrt){
            if(err) return recordCallback(err, record);
            handleStream(spawnOgr(record, vrt), record);
          });
        });

      }else{
        handleStream(spawnOgr(record, null, stream), record);
      }
    }
  }


  function handleStreamError(record, err){
    if(this.unpipe) this.unpipe();
    if(this.destroy) this.destroy();
    if(this.kill) this.kill();
    if(record._retrieverOutput){
      if(program.bucket){
        record._retrieverOutput.abortUpload(function(err){
          if(err) logger.error(err);
        });
      }else{
        fs.removeSync(record._retrieverOutput);
      }
    }
    recordCallback(err, record);
  }


  function spawnOgr(record, file, stream){
    var jsonChild;

    if(stream){
      jsonChild = spawn('ogr2ogr', ['-f', 'GeoJSON', '-t_srs', 'WGS84', '/vsistdout/', '/vsistdin/']);
      pump(stream, jsonChild.stdin);
    }else{
      jsonChild = spawn('ogr2ogr', ['-f', 'GeoJSON', '-t_srs', 'WGS84', '/vsistdout/', file]);
    }

    var csvChild = spawn('ogr2ogr', ['-f', 'CSV', '-lco', 'GEOMETRY=AS_XY', '/vsistdout/', '/vsistdin/']);
    var centroids = centroidStream.stringify();

    jsonChild.stderr.on('data', function(errorText){
      jsonChild.stdout.emit('error', errorText);
    });

    csvChild.stderr.on('data', function(errorText){
      csvChild.stdout.emit('error', errorText);
    });

    pump(jsonChild.stdout, OgrJsonStream(), fieldFilter(record.fields), centroids, function(){
      csvChild.stdin.end(']}');
    });

    csvChild.stdin.write('{"type":"FeatureCollection","features":[');

    csvChild.stderr.once('data', function(data){
      handleStreamError.call(csvChild, record, new Error('Error converting to csv. ' + data.toString()))
    });

    centroids.on('data', function(data){
      csvChild.stdin.write(Buffer.concat([data, comma]));
    });

    return csvChild.stdout;
  }


  function handleStream(stream, record){

    if(program.bucket && !program.directory){
      program.directory = '.';
    }

    var endfile = path.join(program.directory, record.name + '.csv.gz');
    var zipStream = zlib.createGzip();
    var destStream;

    if(program.bucket){
      destStream = uploadStream.stream(endfile);
      record._retrieverOutput = destStream;
      output.location = program.bucket + '/' + program.directory;
    }else{
      destStream = fs.createWriteStream(endfile);
      record._retrieverOutput = endfile;
      output.location= program.directory;
    }

    pump(stream, zipStream, destStream, function(err){
      if(!err) record._retrieverProcessed = 1;
      if(err||record._retrieverVerified) return recordCallback(err, record);
    });
  }

}

module.exports = retrieve;
