# Grasshopper-Loader

**Description**: This is the data loader for [Grasshopper](https://github.com/cfpb/grasshopper), cfpb's work-in-progress geocoder.
Data is assumed to conform to the relevant Input Data Standard for Grasshopper (*q.v.* the [Input Point Data Standard](https://github.com/cfpb/grasshopper/blob/master/docs/data_format.md).
Data is transformed from these standard formats and loaded into Elasticsearch (the schema of which may change).

## Info
  - **Technology stack**: Due to a high volume of IO, the loader uses [node.js](http://nodejs.org/) for high throughput.
  - **Status**: Alpha
  - **Notes on change**: As both the Elasticsearch schema and data standards may (and likely will) change, expect to see active, breaking changes in this repo until tagged otherwise. Also, many new transformations will be added as more state-specific data is acquired.
----

## Open source licensing info
1. [TERMS](TERMS.md)
2. [LICENSE](LICENSE)
3. [CFPB Source Code Policy](https://github.com/cfpb/source-code-policy/)
