# Grasshopper-Loader
[![Build Status](https://travis-ci.org/cfpb/grasshopper-loader.svg?branch=master)](https://travis-ci.org/cfpb/grasshopper-loader)

**Description**: This is the data loader for [Grasshopper](https://github.com/cfpb/grasshopper), cfpb's work-in-progress geocoder.
Data is gathered from state sources, verified, transformed into GeoJSON, loaded into Elasticsearch.

## Usage
  - **Local method**
    - [Install node](https://nodejs.org/)
    - [Install GDAL](http://trac.osgeo.org/gdal/wiki/DownloadingGdalBinaries)
      - On OSX, instead of using a binary or building from source, you can [download homebrew](http://brew.sh/) and `brew install gdal`.
    - [Install elasticsearch](https://www.elastic.co/downloads/elasticsearch)
      - You can also point the loader to elasticsearch running on another machine.
    - Run `npm install` from the project root
    - Test with `npm test -- <CLI Options>`
    - Run with `./index.js <CLI Options>`

  - **Docker method**
    - [Install Docker](https://docs.docker.com/installation/#installation)
    - Build an image:
      `docker build --rm --tag=<image-name>:<tag-name> .`
    - Test the image:
      `./docker-test  <image-name>:<tag-name> <CLI Options>`
    - Run the image:
      `./docker-run <image-name>:<tag-name> <CLI Options>`
      - These scripts assume there is an aws credentials file at `~/.aws/credentials` if using S3 to backup data.
      - When using boot2docker, elasticsearch running on the host machine (eg, your Mac) can be accessed at 10.0.2.2 and elasticsearch running in a container with port 9200 shared can be accessed at the ip given by `boot2docker ip`.

## CLI Options

The loader is a command-line application, run by invoking either `./index.js` for state data or `tiger.js` for Census data.

*Run any of the CLIs with the `--help` flag to get a summary of all the available flags.*

#### State data
State data is loaded by invoking `./index.js` with the following options:

- **-f, --file** *Required* A json data file that contains the collected data endpoints and field mappings of state data. `./data.json` should be used to load all known state data.
- **-m, --match** A string or regular expression that the names from the <file> must contain or match. Can be used to load just a few items from a large metadata file.
- **-h, --host** *Default: localhost* The elasticsearch host. If no argument is provided and a linked elasticsearch Docker container exists, will use its IP.
- **-p, --port** *Default: 9200* The elasticsearch port. If no argument is provided and a linked elasticsearch Docker container exists, will use its lowest exposed port.
- **-a, --alias** *Default: address* The elasticsearch alias to an internally created index. This what queries should be run against once data is loaded.
- **-t, --type** *Default: point* The elasticsearch type (or mapping) within the alias
- **-l, --log** *Default: error* The elasticsearch log level
- **-q, --quiet**, Suppress application-level logging.
- **-b, --backup-bucket** An AWS S3 bucket where data should be backed up.
- **-d, --backup-directory** A directory where the data should be loaded, either relative to the current folder or the passed S3 bucket.
- **--profile** *Default: default* The aws credentials profile in `~/.aws/credentials`. AWS keys as environment variables will override this setting.
- **--monitor** Run the retriever in monitoring mode which only checks data source freshness and doesn't load or backup data.

#### Census data
To load TIGER data use the `tiger.js` CLI. The host, port, alias, type, log, profile, and quiet flags remain unchanged from the state data CLI. However, instead of a `--file` flag the `tiger.js` CLI takes the following option:

- **-d, --directory** *Required* A directory where TIGER files live, which will be concurrently loaded into Elasticsearch.



## Info
  - **Technology stack**: Due to a high volume of IO, the loader uses [node.js](http://nodejs.org/) for high throughput.
  - **Dependencies**: node.js, GDAL 1.11.2
  - **Status**: Alpha
  - **Notes on change**: Expect to see active, breaking changes in this repo until tagged otherwise via semver.

----

## Open source licensing info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)
3. [CFPB Source Code Policy](https://github.com/cfpb/source-code-policy/)
