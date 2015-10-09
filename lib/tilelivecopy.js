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
      function close(err) {
        // If an error occurred, pass to callback but continue (no return)
        // as there's cleanup/closing to do.
        if (err) callback(err);

        var srcClose = src && typeof src.close === 'function' ?
          src.close.bind(src) : function(cb) { cb(); };
        var dstClose = dst && typeof dst.close === 'function' ?
          dst.close.bind(dst) : function(cb) { cb(); };

        queue()
          .defer(srcClose)
          .defer(dstClose)
          .await(function(closeErr) {
            // callback was already called, closing is just for cleanup
            if (err) {
              return;
            // error occurred closing src/dst
            } else if (closeErr) {
              callback(closeErr);
            // success with stats
            } else if (options.stats) {
              callback(null, stats.getStatistics());
            // success without stats
            } else {
              callback();
            }
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

        if (options.stats) {
          options.transform = stats;
        }

        tilelive.copy(src, dst, options, close);
      });
    });
}

module.exports = tilelivecopy;
