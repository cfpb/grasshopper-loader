'use strict';

var shapefile = require('shapefile-stream');
var through = require('through2');
var elasticsearch = require('elasticsearch');

var shp = {
  read: function (path, h, p) {
	  console.log("Loading " + path + " into " + h + ":" + p);
		var client = new elasticsearch.Client({
		  host: h + ":" + p,
			log: 'debug'
		});
		shapefile.createReadStream(path)
			.pipe(through.obj(function(data, enc, next){
			  var props = data.properties
				load(client, 'address', 'point', data);
        next();
			}));
		return;
	}

};

function load(client, index, type, data) {
  	client.create({
	  index: String(index),
		type: String(type),
		body: data
	});
}

module.exports = shp;
