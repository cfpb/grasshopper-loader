# Grasshopper-Loader
[![Build Status](https://travis-ci.org/cfpb/grasshopper-loader.svg?branch=master)](https://travis-ci.org/cfpb/grasshopper-loader)

**Description**: This is the data loader for [Grasshopper](https://github.com/cfpb/grasshopper), cfpb's work-in-progress geocoder.
Data is gathered from state sources, verified, transformed into GeoJSON, loaded into Elasticsearch.

## Usage
  - **Local method**
    - [Install node v0.10.40](https://nodejs.org/)
    - [Install GDAL v1.11.2](http://trac.osgeo.org/gdal/wiki/DownloadingGdalBinaries)
      - On OSX, instead of using a binary or building from source, you can [download homebrew](http://brew.sh/) and `brew install gdal`.
    - [Install elasticsearch v1.7.3](https://www.elastic.co/downloads/elasticsearch)
      - You can also point the loader to elasticsearch running on another machine.
    - Run `npm install` from the project root
    - Test with `npm test -- <CLI Options>`
    - Run with `./index.js <CLI Options>`

  - **Docker method**
    - [Install Docker](https://docs.docker.com/installation/#installation)
    - Build an image:
      `docker build --rm --tag=<image-name>:<tag-name> .`
    - Test the image:
      `./docker-test.sh <image-name>:<tag-name> <CLI Options>`
    - Run the image:
      `./docker-run.sh <image-name>:<tag-name> <CLI file, index.js or tiger.js> <CLI Options>`
      - These scripts assume there is an aws credentials file at `~/.aws/credentials` if using S3 to provide data overrides.
      - When using boot2docker, elasticsearch running on the host machine (eg, your Mac) can be accessed at 10.0.2.2 and elasticsearch running in a container with port 9200 shared can be accessed at the ip given by `boot2docker ip`.

## CLI Options

The loader is a command-line application, run by invoking either `./index.js` for state data or `tiger.js` for Census data.

*Run any of the CLIs with the `--help` flag to get a summary of all the available flags.*

#### State data
State data is loaded by invoking `./index.js` with the following options:

- **-f, --file** *Required* A json metadata file that contains the collected data endpoints and field mappings of state data. `./data.json` should be used to load all known state data.
- **-m, --match** A string or regular expression that the names from the <file> must contain or match. Can be used to load just a few items from a large metadata file.
- **-c, --concurrency** *Default: 2* The number of loading tasks that will run at once.
- **-h, --host** *Default: localhost* The elasticsearch host. If no argument is provided and a linked elasticsearch Docker container exists, will use its IP.
- **-p, --port** *Default: 9200* The elasticsearch port. If no argument is provided and a linked elasticsearch Docker container exists, will use its lowest exposed port.
- **-a, --alias** *Default: address* The elasticsearch alias to an internally created index. This what queries should be run against once data is loaded.
- **-t, --type** *Default: point* The elasticsearch type (or mapping) within the alias.
- **-l, --log** *Default: error* The elasticsearch log level.
- **-q, --quiet** Suppress application-level logging.
- **-b, --bucket** An AWS S3 bucket where data resides that will override the source url in the metadata file. Metadata entry names are matched against file basenames to determine overrides.
- **-d, --directory** A directory where data sources reside, either relative to the current folder or the passed S3 bucket. Also used to override source urls in a similar fashion.
- **-P, --profile** *Default: default* The aws credentials profile in `~/.aws/credentials`. Needed if using data overrides from a private bucket. AWS keys as environment variables will override this setting.

#### Census data
To load TIGER data use the `tiger.js` CLI with the following options:

- **-d, --directory** *Required* A directory where TIGER files live, which will be concurrently loaded into Elasticsearch.
- **-c, --concurrency** *Default: 4* The number of loading tasks that will run at once.
- **-h, --host** *Default: localhost* The elasticsearch host. If no argument is provided and a linked elasticsearch Docker container exists, will use its IP.
- **-p, --port** *Default: 9200* The elasticsearch port. If no argument is provided and a linked elasticsearch Docker container exists, will use its lowest exposed port.
- **-a, --alias** *Default: census* The elasticsearch alias to an internally created index. This what queries should be run against once data is loaded.
- **-t, --type** *Default: addrfeat* The elasticsearch type (or mapping) within the alias.
- **-l, --log** *Default: error* The elasticsearch log level.
- **-q, --quiet**, Suppress application-level logging.



## The Metadata file
The metadata file that the state data loader uses tracks data sources and information about each source. The fields are defined as follows:
 - **name** *Required, must be lowercase* The name of the state in lowercase, with underscores replacing spaces (*eg* north_carolina). Lowercase is required because this name is passed transparently to elasticsearch which uses it to create an index.
 - **url** *Required if not providing an override directory* The URL where the data can be accessed.
 - **file** If **url** is a zip archive, a reference to the data file relative to the archive, *eg* `folder/file.shp` if a containing folder is zipped or simply `file.shp`. 
 - **spatialReference** *Required if value isn't WGS84 and dataset lacks projection information* Spatial reference information that will be input to ogr2ogr as the -s_srs parameter. 
 - **count** Expected feature count. Useful to suppress errors caused by "incomplete" loading if some rows are known to be missing required data.
 - **fields** Mappings for dataset columns to values we're interested in, namely . The format is as follows:
   - An object with `Number`, `Street`, `City`, `State`, and `Zip` as keys.
   - The value of each key is another object with two keys:
     - `type` which has a value of either `static`, `multi`, or `dynamic` as strings
     - `value` which provides the mapping to the top level key we're interested in based on the type as follows:
       - `static` means a column maps 1:1 to our top level key, just provide the column name as a string
       - `multi` expects an array of strings that refer to column names that, when concatenated with spaces, form the full mapping
       - `dynamic` the body of a javascript function as a string that is passed the variable "props" which is a reference to the complete row of original data, which can be arbitrarily transformed as needed (usually to correct bad formatting)
   - For each row of data, these mappings are used to filter and transform the original data format to the schema expected by Elasticsearch.

## Info
  - **Technology stack**: Due to a high volume of IO, the loader uses [node.js](http://nodejs.org/) for high throughput.
  - **Dependencies**: node.js v0.10.40, GDAL v1.11.2, ElasticSearch v1.7.3
  - **Status**: Beta
  - **Notes on change**: Expect to see active, breaking changes in this repo until tagged otherwise via semver.

----

## Open source licensing info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)
3. [CFPB Source Code Policy](https://github.com/cfpb/source-code-policy/)
