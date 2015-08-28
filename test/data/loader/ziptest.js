var zlib = require('zlib');
var fs = require('fs');
fs.createReadStream('ark.json.gz')
  .pipe(zlib.createGunzip())
  .pipe(process.stdout);
