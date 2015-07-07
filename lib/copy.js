var url = require('url');
var zlib = require('zlib');
var fs = require('fs');
var util = require('util');
var S3 = require('tilelive-s3');
var tilelive = require('tilelive');
var progress = require('progress-stream');
var queue = require('queue-async');
var ValidationStream = require('mapbox-upload-validate/lib/validators/serialtiles').ValidationStream;

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
    data: { tiles: [ s3urlTemplate ] }
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

    var validate = new ValidationStream({ validateVectorTiles: true })
      .on('error', function(err) {
        err.code = 'EINVALID';
        done(err);
      });

    if (options.progress) prog.on('progress', function(p) { options.progress(stats, p); });

    source
      .pipe(gunzip)
      .pipe(deserialize)
      .pipe(validate)
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

function tilelivecopy(srcUri, s3url, options, callback) {
  var scheme = 'scanline';
  var protocol = url.parse(srcUri).protocol;
  if (protocol === 'mbtiles:') scheme = 'list';
  if (protocol === 'omnivore:') scheme = 'pyramid';
  options.type = scheme;

  queue()
    .defer(tilelive.load, srcUri)
    .defer(tilelive.load, s3url)
    .await(function(err, src, dst) {
      function close(err) {
        var srcClose = src && typeof src.close === 'function' ?
          src.close.bind(src) : function(cb) { cb(); };
        var dstClose = dst && typeof dst.close === 'function' ?
          dst.close.bind(dst) : function(cb) { cb(); };

        queue()
          .defer(srcClose)
          .defer(dstClose)
          .await(function(closeErr) {
            callback(err || closeErr);
          });
      }

      if (err) {
        err.code = 'EINVALID';
        return close(err);
      }

      src.getInfo(function(err, info) {
        if (err) {
          err.code = 'EINVALID';
          return close(err);
        }

        options.minzoom = options.minzoom || info.minzoom;
        options.maxzoom = options.maxzoom || info.maxzoom;
        options.bounds = options.bounds || info.bounds;
        if (scheme === 'list') options.listStream = src.createZXYStream();

        tilelive.copy(src, dst, options, close);
      });
    });
}

module.exports = {
  tilelive: tilelivecopy,
  serialtiles: serialtiles
};
