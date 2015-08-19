var program = require('commander');
var loader = require('./lib/loader');

//Favor source GDAL installations for ogr transformations
process.env.PATH = '/usr/local/bin:' + process.env.PATH

var esVar = process.env.ELASTICSEARCH_PORT;
var esHost;
var esPort;

if(esVar){
  esVar = esVar.split('//')[1].split(':');
  esHost = esVar[0];
  esPort = +esVar[1];
}


program
  .version('0.0.1')
  .option('-b, --bucket <bucket>', 'An S3 bucket where data resides. If no -d option is passed, will attempt to load all data in the bucket.')
  .option('-d, --data <data>', 'Point data as a .zip, .shp, .gdb, GeoJson, or directory. Will also accept a .csv if preformatted by ogr2ogr. Provide a local file or an S3 key. Zipped, gzipped, and GeoJson data can also be accessed via url. Required if no bucket is passed.')
  .option('-t, --transformer <transformer>', 'Data transformer. Defaults to ./transformers/[[file basename]].js.')
  .option('-h, --host <host>', 'ElasticSearch host. Defaults to localhost unless linked to a Docker container aliased to Elasticsearch', esHost || 'localhost')
  .option('-p, --port <port>', 'ElasticSearch port. Defaults to 9200 unless linked to a Docker container aliased to Elasticsearch.', Number, esPort || 9200)
  .option('-l, --log <log>', 'ElasticSearch log level. Defaults to debug.', 'debug')
  .option('--alias <alias>', 'Elasticsearch index alias. Defaults to address.', 'address')
  .option('--type <type>', 'Elasticsearch type within the provided or default alias. Defaults to point.', 'point')
  .option('--profile <profile>', 'The aws credentials profile in ~/.aws/credentials. Will also respect AWS keys as environment variables.', 'default')
  .option('--source-srs <sourceSrs>', 'Source Spatial Reference System, passed to ogr2ogr as -s_srs. Auto-detects by default.')
  .option('--preformatted', 'Input has been preformatted to GeoJson or csv and transformed to WGS84 by ogr2ogr. Results in the loader skipping ogr2ogr and immediately splitting the input into records.')
  .parse(process.argv);

loader(program);
