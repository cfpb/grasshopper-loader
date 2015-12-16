var fs = require('fs-extra');
var path = require('path');
var aws = require('aws-sdk');
var autoAuth = require('aws-auto-auth');


function Overrides(options){
  this.options = options;
  this.overrides = {};
  if(options.bucket){
    var credentials = autoAuth(aws, options.profile);
    this.credentials = credentials;
    this.S3 = new aws.S3({credentials: credentials});
  }
}


Overrides.prototype.resolve = function(name){
  return this.overrides[name];
}


Overrides.prototype.get = function(name){
  if(this.options.bucket){
    var request = this.S3.getObject({'Bucket': this.options.bucket, 'Key': this.overrides[name]});
    return request.createReadStream();
  }else{
    return fs.createReadStream(this.overrides[name]);
  }
}


Overrides.prototype.list = function(callback){
  var self = this;

  if(!self.options.bucket && !self.options.directory) return callback(null, self);

  if(self.options.bucket){
    if(!self.credentials.accessKeyId) return callback(new Error('Couldn\'t resolve S3 credentials.'), self);

    self.S3.listObjects({Bucket: self.options.bucket,
                         Prefix: self.options.directory || ''},
      function(err, res){
        if(err) return callback(err, self);
        var dir = self.options.directory || '.';
        res.Contents.forEach(function(v){
          var key = v.Key;
          //directory check
          if(key[key.length - 1] === '/') return;
          if(path.dirname(key) === dir){
            assignOverride(self, key);
          }
        });
        return callback(null, self);
      }
    );
  }else{
    fs.readdir(self.options.directory, function(err, files){
      if(err) return callback(err, self);

      files.forEach(function(file){
        file = path.join(self.options.directory, file);
        //directory check
        if(fs.lstatSync(file).isFile()){
          assignOverride(self, file);
        }
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
