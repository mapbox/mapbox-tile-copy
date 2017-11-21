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
  if (protocol === 'mbtiles:') scheme = 'list';
  if (protocol === 'omnivore:') scheme = 'scanline';
  options.type = scheme;

  var stats = new TileStatStream();

  queue()
  // loading source and sink/destination (where you're dumping things)
    .defer(tilelive.load, srcUri)
    .defer(tilelive.load, s3url)
    .await(function(err, src, dst) {
      function done(err) {
        // invalid geojson
        if (err && err.message.indexOf('Failed to parse geojson feature') != -1) {
          err.code = 'EINVALID';
        }

        // web mercator out of range
        if (err && err.message.indexOf('required parameter y is out of range') != -1) {
          err.code = 'EINVALID';
          // einvalid is how we're capturing specific messages so we can create a more proper error for unpacker
          // otherwise return with different code 
          err.message = 'Coordinates beyond web mercator range. Please check projection and lat/lon values.'
        }

        if(err && err.message.indexOf('vector_tile_processor: layer extent did not repoject back to map projection') != -1){
          err.code = 'EINVALID';
          err.message = 'Unable to reproject data. Please reproject to Web Mercator (EPSG:3857) and try again.'
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
        if (protocol === 'mbtiles:' || protocol === 'tilejson:') transforms.push(MigrationStream());
        if (transforms.length > 0) options.transform = combiner(transforms);

        if (!dst.sse) dst.sse = 'AES256';
        tilelive.copy(src, dst, options, done);
      });
    });
}

module.exports = tilelivecopy;
