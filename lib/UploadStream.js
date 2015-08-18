'use strict';
var aws = require('aws-sdk');
var autoAuth = require('aws-auto-auth');
var s3Stream = require('s3-upload-stream');


function UploadStream(bucket, profile){
  if(!bucket) throw new Error('Need to initialize uploadStream with an S3 bucket.');

  if(!(this instanceof UploadStream)) return new UploadStream(bucket, profile);

  this.credentials = autoAuth(aws, profile);
  this.bucket = bucket;
  this.S3 = new aws.S3({credentials: this.credentials});
  this.s3Stream = s3Stream(this.S3);
}


function getStream(key){
  var stream = this.s3Stream.upload({Bucket: this.bucket, Key: key});
  stream.abortUpload = makeAbort(this.bucket, key);
  return stream;
}


function makeAbort(bucket, key){
  return function(cb){
    return this.S3.deleteObject({Bucket: bucket, Key: key}, cb);
  }
}


UploadStream.prototype.stream = getStream;


module.exports = UploadStream;
