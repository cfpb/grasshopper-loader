var fs = require('fs-extra');
var crypto = require('crypto');

var synonyms = fs.readJSONSync('../synonyms.json');


module.exports = function(options, client, basename, cb){
  basename = basename.toLowerCase();
  var index = basename.toLowerCase() + '-' + Date.now() + '-' + crypto.pseudoRandomBytes(6).toString('hex');

  if(options.alias !== 'census'){
    return cb(null, index);
  }

  var settings = JSON.parse(JSON.stringify(censusSettings));
  settings.mappings[options.type] = censusType;

  client.indices.create({index: index, body: settings}, function(err){
    if(err) return cb(err);
    cb(null, index);
  })
};


var censusSettings = {
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "stateSynonyms": {
            "tokenizer": "whitespace",
            "filter": [
              "stateSynonyms"
            ]
          },
          "addressSynonyms": {
            "tokenizer": "whitespace",
            "filter": [
              "addressSynonyms"
            ]
          }
        },
        "filter": {
          "stateSynonyms": {
            "type": "synonym",
            "ignore_case": true,
            "synonyms": synonyms.stateSynonyms
          },
          "addressSynonyms": {
            "type": "synonym",
            "ignore_case": true,
            "synonyms": synonyms.addressSynonyms
          }
        }
      }
    }
  },
  "mappings": {}
};

var censusType = {
  "properties": {
    "geometry": {
      "properties": {
        "coordinates": {
          "type": "double"
        },
        "type": {
          "type": "string"
        }
      }
    },
    "properties": {
      "properties": {
        "ARIDL": {
          "type": "string"
        },
        "ARIDR": {
          "type": "string"
        },
        "EDGE_MTFCC": {
          "type": "string"
        },
        "FULLNAME": {
          "type": "string",
          "analyzer": "addressSynonyms"
        },
        "LFROMHN": {
          "type": "string"
        },
        "LFROMTYP": {
          "type": "string"
        },
        "LINEARID": {
          "type": "string"
        },
        "LTOHN": {
          "type": "string"
        },
        "LTOTYP": {
          "type": "string"
        },
        "OFFSETL": {
          "type": "string"
        },
        "OFFSETR": {
          "type": "string"
        },
        "PARITYL": {
          "type": "string"
        },
        "PARITYR": {
          "type": "string"
        },
        "RFROMHN": {
          "type": "string"
        },
        "RFROMTYP": {
          "type": "string"
        },
        "ROAD_MTFCC": {
          "type": "string"
        },
        "RTOHN": {
          "type": "string"
        },
        "RTOTYP": {
          "type": "string"
        },
        "STATE": {
          "type": "string",
          "analyzer": "stateSynonyms"
        },
        "TFIDL": {
          "type": "long"
        },
        "TFIDR": {
          "type": "long"
        },
        "TLID": {
          "type": "long"
        },
        "ZIPL": {
          "type": "string"
        },
        "ZIPR": {
          "type": "string"
        },
        "load_date": {
          "type": "long"
        }
      }
    },
    "type": {
      "type": "string"
    }
  }
}


