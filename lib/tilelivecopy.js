var url = require('url');
var tilelive = require('@mapbox/tilelive');
var tileliveOmivore = require('@mapbox/tilelive-omnivore');
var TileStatStream = require('tile-stat-stream');
var queue = require('queue-async');
var combiner = require('stream-combiner');
var MigrationStream = require('./migration-stream');

function tilelivecopy(srcUri, s3url, options, callback) {
  var scheme = 'scanline';
  var protocol = url.parse(srcUri).protocol;
  var experimentalBypass = options.experiment || false; // bypass the MBTiles migration stream with "false"
  if (protocol === 'mbtiles:') scheme = 'list';
  if (protocol === 'omnivore:') scheme = 'scanline';
  options.type = scheme;

  var stats = new TileStatStream();

  queue()
    .defer(tilelive.load, srcUri)
    .defer(tilelive.load, s3url)
    .await(function(err, src, dst) {
      function done(err) {
        // invalid geojson
        if (err && err.message.indexOf('Failed to parse geojson feature') != -1) {
          err.code = 'EINVALID';
        }

        // web mercator out of range
        if (err && err.message.indexOf('required parameter y is out of range') != -1 || err && err.message.indexOf('required parameter x is out of range') != -1) {
          err.code = 'EINVALID';
          err.message = 'Coordinates beyond web mercator range. Please check projection and lat/lon values.'
        }

        // reprojection error from mapnik-vector-tile
        if (err && err.message.indexOf('vector_tile_processor:') != -1) {
          err.code = 'EINVALID';
          err.message = 'Unable to reproject data. Please reproject to Web Mercator (EPSG:3857) and try again.'
        }

        // tile size limits from tilelive-bridge
        if (err && err.message.indexOf('Tile >= max allowed size') != -1 && process.env.BRIDGE_MAX_VTILE_BYTES_COMPRESSED) {
          err.code = 'EINVALID';
          err.message = `Tile size exceeds limit. At least one vector tile is larger than ${process.env.BRIDGE_MAX_VTILE_BYTES_COMPRESSED/1000000}MB.`;
        }

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

        var transforms = [];

        if (options.stats) transforms.push(stats);
        if ((protocol === 'mbtiles:' || protocol === 'tilejson:') && !experimentalBypass) transforms.push(MigrationStream());
        if (transforms.length > 0) options.transform = combiner(transforms);

        if (!dst.sse) dst.sse = 'AES256';
        tilelive.copy(src, dst, options, done);
      });
    });
}

module.exports = tilelivecopy;
