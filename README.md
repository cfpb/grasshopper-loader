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
    - **-d, --data** *Required* The data source to load. This can be a gdb, shp, GeoJSON, zip, or a directory of any of these which gets loaded recursively.
    - **-t, --transformer** *Default: transformers/[[data file basename]].js* The transformer to use. This converts state-specific data formats to our [Point Data Schema](https://github.com/cfpb/grasshopper/blob/master/docs/point_data_spec.md)
    - **-h, --host** *Default: localhost* The elasticsearch host
    - **-p, --port** *Default: 9200* The elasticsearch port
    - **--index** *Default: address* The elasticsearch index
    - **--type** *Default: point* The elasticsearch type
  - Test the loader by starting elasticsearch on localhost:9200 and running `npm test`
    - This will run the tests in test/test.js from the root of the project
  - To manually test the loader, run `./grasshopper-loader.js -d test/data/new_york.json` 
    - This will load 100 addresses from New York into the address index and the point type
    - Check this with `curl -XGET "localhost:9200/address/point/_count?q=properties.address:NY"`

## Info
  - **Technology stack**: Due to a high volume of IO, the loader uses [node.js](http://nodejs.org/) for high throughput.
  - **Dependencies**: node.js, GDAL
  - **Status**: Alpha
  - **Notes on change**: As both the Elasticsearch schema and data standards may (and likely will) change, expect to see active, breaking changes in this repo until tagged otherwise. Also, many new transformations will be added as more state-specific data is acquired.

----

## Open source licensing info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)
3. [CFPB Source Code Policy](https://github.com/cfpb/source-code-policy/)
