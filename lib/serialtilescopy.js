var fs = require('fs');
var util = require('util');
var tilelive = require('@mapbox/tilelive');
var zlib = require('zlib');
var url = require('url');
var progress = require('progress-stream');
var MigrationStream = require('./migration-stream');
var S3 = require('@mapbox/tilelive-s3');

function serialtiles(srcUri, s3urlTemplate, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }

  var max_tilesize = options.limits && options.limits.max_tilesize ?
    options.limits.max_tilesize : 500 * 1024;
  var once;

  var source = fs.createReadStream(url.parse(srcUri).pathname)
    .on('error', done);

  var prog = progress({
    objectMode: true,
    time: 100
  });

  var stats = {
    skipped: 0,
    done: 0
  };

  new S3({
    data: { tiles: [ s3urlTemplate ] },
    sse: 'AES256'
  }, function(err, dst) {
    if (err) return callback(err);
    copy(dst);
  });

  function copy(dst) {
    var gunzip = zlib.createGunzip()
      .on('error', done);

    var s3 = tilelive.createWriteStream(dst, {retry:options.retry})
      .on('error', done)
      .on('stop', done);

    var migrate = MigrationStream()
        .on('error', done);

    var deserialize = tilelive.deserialize(options)
      .on('error', done)
      .on('tile', function(tile) {
        if (!tile.buffer) return;
        stats.done++;
        if (tile.buffer.length <= max_tilesize) return;
        var err = new Error(util.format('Tile exceeds maximum size of %sk at z %s. Reduce the detail of data at this zoom level or omit it by adjusting your minzoom.', Math.round(max_tilesize / 1024), tile.z));
        err.code = 'EINVALID';
        done(err);
      });

    if (options.progress) prog.on('progress', function(p) { options.progress(stats, p); });

    source
      .pipe(gunzip)
      .pipe(deserialize)
      .pipe(migrate)
      .pipe(prog)
      .pipe(s3);
  }

  function done(err) {
    if (once) return;
    once = true;

    if (err) source.unpipe();
    callback(err);
  }
}

module.exports = serialtiles;
