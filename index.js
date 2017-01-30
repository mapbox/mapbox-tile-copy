var url = require('url');
var getUri = require('./lib/get-uri');
var tilelivecopy = require('./lib/tilelivecopy');
var serialtilescopy = require('./lib/serialtilescopy');
var s3urls = require('s3urls');

var tilelive = require('tilelive');
var Vector = require('tilelive-vector');
var MBTiles = require('mbtiles');
var Omnivore = require('@mapbox/tilelive-omnivore');
var TileJSON = require('tilejson');
var Mapbox = require('./lib/tilelive-mapbox');
var S3 = require('tilelive-s3');
var path = require('path');

// Note: tilelive-vector is needed for `tm2z` protocol (https://github.com/mapbox/tilelive-vector/issues/124)
Vector.registerProtocols(tilelive);
MBTiles.registerProtocols(tilelive);
Omnivore.registerProtocols(tilelive);
TileJSON.registerProtocols(tilelive);
Mapbox.registerProtocols(tilelive);
S3.registerProtocols(tilelive);

var mapnik = require('mapnik');
mapnik.Logger.setSeverity(mapnik.Logger.NONE);

mapnik.register_fonts(path.dirname(require.resolve('mapbox-studio-default-fonts')), { recurse: true });
mapnik.register_fonts(path.dirname(require.resolve('mapbox-studio-pro-fonts')), { recurse: true });
if (process.env.MapboxTileCopyFonts)
  mapnik.register_fonts(process.env.MapboxTileCopyFonts, { recurse: true });

module.exports = function(filepath, s3url, options, callback) {
  // Make sure the s3url is of type s3://bucket/key
  var query = url.parse(s3url).query;
  s3url = s3urls.convert(s3url, 's3');
  if (query) s3url += '?' + query;

  if (s3url.indexOf('{z}') == -1 ||
      s3url.indexOf('{x}') == -1 ||
      s3url.indexOf('{y}') == -1) return callback(new Error('Destination URL does not include a {z}/{x}/{y} template.'));

  if (options.bundle === true) {
    tilelivecopy(filepath, s3url, options, copied);
  } else {
    getUri(filepath, options.layerName, function(err, srcUri) {
      if (err) return callback(err);
      if (url.parse(srcUri).protocol === 'serialtiles:') {
        serialtilescopy(srcUri, s3url, options, copied);
      } else {
        tilelivecopy(srcUri, s3url, options, copied);
      }
    });
  }

  function copied(err, stats) {
    if (!err) return callback(err, stats);
    var fatal = { SQLITE_CORRUPT: true, EINVALIDTILE: true };
    if (fatal[err.code]) err.code = 'EINVALID';
    return callback(err);
  }
};
