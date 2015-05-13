# Grasshopper-Loader
[![Build Status](https://travis-ci.org/cfpb/grasshopper-loader.svg?branch=master)](https://travis-ci.org/cfpb/grasshopper-loader)

**Description**: This is the data loader for [Grasshopper](https://github.com/cfpb/grasshopper), cfpb's work-in-progress geocoder.
Data is assumed to conform to the relevant Input Data Standard for Grasshopper (*q.v.* the [Input Point Data Standard](https://github.com/cfpb/grasshopper/blob/master/docs/data_format.md).
Data is transformed from these standard formats and loaded into Elasticsearch (the schema of which may change).

## Usage
  - [Install node](https://nodejs.org/)
  - [Install GDAL](http://trac.osgeo.org/gdal/wiki/DownloadingGdalBinaries)
    - On OSX, instead of using a binary or building from source, you can [download homebrew](http://brew.sh/) and `brew install gdal`.
  - [Install elasticsearch](https://www.elastic.co/downloads/elasticsearch)
    - You can also point the loader to elasticsearch running on another machine.
  - Run `npm install` from the project root
  - The loader is a command-line application, run by invoking **grasshopper-loader.js** with the following options:
    - **-b, --bucket** An AWS S3 bucket where data resides. If no -d option is passed, will attempt to load all data in the bucket.
    - **-d, --data** *Required if no bucket is passed* The data source to load. This can be a gdb, shp, GeoJSON, zip, or a directory of any of these which gets loaded recursively. Zips and GeoJSON can also be loaded from remote sources via URL.
    - **-t, --transformer** *Default: transformers/[[data file basename]].js* The transformer to use. This converts state-specific data formats to our [Point Data Schema](https://github.com/cfpb/grasshopper/blob/master/docs/point_data_spec.md)
    - **-h, --host** *Default: localhost* The elasticsearch host
    - **-p, --port** *Default: 9200* The elasticsearch port
    - **-l, --log** *Default: debug* The elasticsearch log level
    - **--index** *Default: address* The elasticsearch index
    - **--type** *Default: point* The elasticsearch type
    - **--profile** *Default: default* The aws credentials profile in ~/.aws/credentials. AWS keys as environment variables will override this setting.')
    - **--source-srs** *Auto-detects if possible* Source spatial reference system. Needed for untransformed .csv files. Passes the value to ogr2ogr's -s_srs parameter.'
    - **--preformatted** Indicates whether input has already been converted to GeoJson and transformed to WGS84 by ogr2ogr. Will skip the ogr2ogr step if so.
  - Test the loader by starting elasticsearch on localhost:9200 and running `npm test`
    - This will run the tests in test/test.js from the root of the project
    - The host, port, index, type, and profile arguments to the loader can all be passed to `npm test` as well, with respective defaults of localhost, 9200, testindex, testtype, and default.
  - To manually test the loader, run `./grasshopper-loader.js -d test/data/new_york.json` 
    - This will load 100 addresses from New York into the address index and the point type
    - Check this with `curl -XGET "localhost:9200/address/point/_count?q=properties.address:NY"`

## Info
  - **Technology stack**: Due to a high volume of IO, the loader uses [node.js](http://nodejs.org/) for high throughput.
  - **Dependencies**: node.js, GDAL ~1.11
  - **Status**: Alpha
  - **Notes on change**: As both the Elasticsearch schema and data standards may (and likely will) change, expect to see active, breaking changes in this repo until tagged otherwise. Also, many new transformations will be added as more state-specific data is acquired.

----

## Open source licensing info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)
3. [CFPB Source Code Policy](https://github.com/cfpb/source-code-policy/)
