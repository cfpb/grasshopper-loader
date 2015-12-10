var fs = require('fs-extra');
var path = require('path');
var aws = require('aws-sdk');
var autoAuth = require('aws-auto-auth');

function Overrides(options){
  this.options = options;
  this.overrides = {};
  if(options.bucket){
    var credentials = autoAuth(aws, options.profile);
    this.S3 = new aws.S3({credentials: credentials});
  }
}

Overrides.prototype.resolve = function(name){
  return this.overrides[name];
}

Overrides.prototype.get = function(name){
  if(this.options.bucket){
    //request from S3
  }else{
   //read locally 
  }
}

Overrides.prototype.list = function(callback){
  var self = this;

  if(!this.options.bucket && !this.options.directory) return callback(null, this);

  if(this.options.bucket){
    this.S3.listObjects({Bucket: this.options.bucket,
                         Prefix: this.options.directory || ''},
      function(err, res){
        if(err) return callback(err, self);
        res.Contents.forEach(function(v){
          var key = v.Key;
          assignOverride(self, key);
        });
        return callback(null, self);
      }
    );
  }else{
    fs.readdir(this.options.directory, function(err, files){
      if(err) return callback(err, self);

      files.forEach(function(file){
        assignOverride(self, file);
      });
      return callback(null, self);
    });
  }
}


function resolveOverrides(options, callback){
  var overrides = new Overrides(options);
  overrides.list(callback);
}


function assignOverride(obj, file){
  return obj.overrides[getName(file)] = file;
}


function getName(file){
  return path.basename(file, path.extname(file));
}


module.exports = resolveOverrides;
