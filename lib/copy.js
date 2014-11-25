var url = require('url');
var zlib = require('zlib');
var fs = require('fs');
var invalid = require('./invalid');
var S3 = require('tilelive-s3');
var tilelive = require('tilelive');

function serialtiles(srcUri, s3urlTemplate, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }

  var max_tilesize = options.limits && options.limits.max_tilesize ?
    options.limits.max_tilesize : 500 * 1024;
  var once, foundInfo;

  var source = fs.createReadStream(url.parse(srcUri).pathname)
    .on('error', done);

  new S3({
    data: { tiles: [ s3urlTemplate ] }
  }, function(err, dst) {
    if (err) return callback(err);
    copy(dst);
  });

  function copy(dst) {
    var gunzip = zlib.createGunzip()
      .on('error', done);

    var s3 = tilelive.createWriteStream(dst)
      .on('error', done)
      .on('stop', done);

    var deserialize = tilelive.deserialize(options)
      .on('error', done)
      .on('tile', function(tile) {
        if (!tile.buffer) return;
        if (tile.buffer.length <= max_tilesize) return;
        done(invalid('Tile exceeds maximum size of %sk at z %s. Reduce the detail of data at this zoom level or omit it by adjusting your minzoom.', Math.round(max_tilesize/1024), tile.z));
      });

    source.pipe(gunzip).pipe(deserialize).pipe(s3);
  }

  function done(err) {
    if (once) return;
    once = true;

    if (err) source.unpipe();
    callback(err);
  }
}

function tilelivecopy(srcUri, s3url, options, callback) {
  var scheme = url.parse(srcUri).protocol === 'mbtiles:' ? 'list' : 'pyramid';

  tilelive.info(srcUri, function(err, info) {
    options.minzoom = info.minzoom;
    options.maxzoom = info.maxzoom;
    options.bounds = info.bounds;
    options.type = scheme;

    if (scheme === 'list') return tilelive.load(srcUri, function(err, src) {
      if (err) return callback(err);
      options.listStream = src.createZXYStream();
      tilelive.copy(src, s3url, options, callback);
    });

    tilelive.copy(srcUri, s3url, options, callback);
  });
}

module.exports = {
  tilelivecopy: tilelivecopy,
  serialtiles: serialtiles
};
