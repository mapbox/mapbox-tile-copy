var url = require('url');
var getUri = require('./lib/get-uri');
var copy = require('./lib/copy');
var s3urls = require('s3urls');

var tilelive = require('tilelive');
var Vector = require('tilelive-vector');
var MBTiles = require('mbtiles');
var Omnivore = require('tilelive-omnivore');
var TileJSON = require('tilejson');
var Mapbox = require('../lib/tilelive-mapbox');

Vector.registerProtocols(tilelive);
MBTiles.registerProtocols(tilelive);
Omnivore.registerProtocols(tilelive);
TileJSON.registerProtocols(tilelive);
Mapbox.registerProtocols(tilelive);

Vector.mapnik.register_fonts(path.dirname(require.resolve('mapbox-studio-default-fonts')), { recurse: true });
Vector.mapnik.register_fonts(path.dirname(require.resolve('mapbox-studio-pro-fonts')), { recurse: true });
if (process.env.MapboxUploadValidateFonts)
  Vector.mapnik.register_fonts(process.env.MapboxUploadValidateFonts, { recurse: true });

var mapnik = require('mapnik');
mapnik.Logger.setSeverity(mapnik.Logger.NONE);

module.exports = function(filepath, s3url, jobInfo, callback) {
  // Make sure the s3url is of type s3://bucket/key
  s3url = s3urls.convert(s3url, 's3');

  getUri(filepath, function(err, uri) {
    if (err) return callback(err);

    var copyTiles = (function(protocol) {
      // customized copy for serialtiles
      if (protocol === 'serialtiles:') return copy.serialtiles;

      // no-op for tm2z, tilejson
      if (protocol === 'tm2z:' || protocol === 'tilejson:')
        return function(a, b, c, cb) { cb(); };

      // otherwise tilelive.copy
      return copy.tilelive;
    })(url.parse(uri).protocol);

    copyTiles(srcUri, s3url, jobInfo, callback);
  });
};
