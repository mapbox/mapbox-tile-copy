var url = require('url');
var tilelive = require('tilelive');
var TileStatStream = require('tile-stat-stream');
var queue = require('queue-async');

function tilelivecopy(srcUri, s3url, options, callback) {
  var scheme = 'scanline';
  var protocol = url.parse(srcUri).protocol;
  if (protocol === 'mbtiles:') scheme = 'list';
  if (protocol === 'omnivore:') scheme = 'pyramid';
  options.type = scheme;

  var stats = new TileStatStream();

  queue()
    .defer(tilelive.load, srcUri)
    .defer(tilelive.load, s3url)
    .await(function(err, src, dst) {
      function done(err) {
        if (options.stats) {
          callback(err, stats.getStatistics());
        } else {
          callback(err);
        }
      }

      if (err) {
        err.code = 'EINVALID';
        return done(err);
      }

      src.getInfo(function(err, info) {
        if (err) {
          err.code = 'EINVALID';
          return done(err);
        }

        options.minzoom = options.minzoom || info.minzoom;
        options.maxzoom = options.maxzoom || info.maxzoom;
        options.bounds = options.bounds || info.bounds;
        options.close = true;
        if (scheme === 'list') options.listStream = src.createZXYStream();

        if (options.stats) {
          options.transform = stats;
        }

        tilelive.copy(src, dst, options, done);
      });
    });
}

module.exports = tilelivecopy;
