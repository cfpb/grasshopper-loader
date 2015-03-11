var elasticsearch = require('elasticsearch');
var obj = {_index: 'wyatt', _type: 'pearsall'};
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'debug'
});

client.bulk({
  body:[
  {index:obj},
  {title:'yo'},
  {index:obj},
  {title:'hark'},
  {index:obj},
  {title:'glorp'}  
 ]
})
