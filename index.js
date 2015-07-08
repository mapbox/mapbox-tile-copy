var url = require('url');
var getUri = require('./lib/get-uri');
var tilelivecopy = require('./lib/tilelivecopy');
var serialtilescopy = require('./lib/serialtilescopy');
var s3urls = require('s3urls');

var tilelive = require('tilelive');
var Vector = require('tilelive-vector');
var MBTiles = require('mbtiles');
var Omnivore = require('tilelive-omnivore');
var TileJSON = require('tilejson');
var Mapbox = require('./lib/tilelive-mapbox');
var S3 = require('tilelive-s3');
var path = require('path');

Vector.registerProtocols(tilelive);
MBTiles.registerProtocols(tilelive);
Omnivore.registerProtocols(tilelive);
TileJSON.registerProtocols(tilelive);
Mapbox.registerProtocols(tilelive);
S3.registerProtocols(tilelive);

Vector.mapnik.register_fonts(path.dirname(require.resolve('mapbox-studio-default-fonts')), { recurse: true });
Vector.mapnik.register_fonts(path.dirname(require.resolve('mapbox-studio-pro-fonts')), { recurse: true });
if (process.env.MapboxTileCopyFonts)
  Vector.mapnik.register_fonts(process.env.MapboxTileCopyFonts, { recurse: true });

var mapnik = require('mapnik');
mapnik.Logger.setSeverity(mapnik.Logger.NONE);

module.exports = function(filepath, s3url, options, callback) {
  // Make sure the s3url is of type s3://bucket/key
  s3url = s3urls.convert(s3url, 's3');

  getUri(filepath, function(err, srcUri) {
    if (err) return callback(err);

    var copyTiles = (function(protocol) {
      // customized copy for serialtiles
      if (protocol === 'serialtiles:') return serialtilescopy;

      // otherwise tilelive.copy
      return tilelivecopy;
    })(url.parse(srcUri).protocol);

    copyTiles(srcUri, s3url, options, function(err) {
      if (!err) return callback();

      var fatal = [ 'SQLITE_CORRUPT', 'EINVALIDTILE' ];
      if (fatal.indexOf(err.code) !== -1) err.code = 'EINVALID';
      return callback(err);
    });
  });
};
