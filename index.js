var url = require('url');
var getUri = require('./lib/get-uri');
var tilelivecopy = require('./lib/tilelivecopy');
var serialtilescopy = require('./lib/serialtilescopy');

var s3urls = require('@mapbox/s3urls');
var tilelive = require('@mapbox/tilelive');
var Vector = require('@mapbox/tilelive-vector');
var MBTiles = require('@mapbox/mbtiles');
var Omnivore = require('@mapbox/tilelive-omnivore');
var TileJSON = require('@mapbox/tilejson');
var Mapbox = require('./lib/tilelive-mapbox');
var S3 = require('@mapbox/tilelive-s3');
var tl_file = require('tilelive-file');
var path = require('path');

// Note: tilelive-vector is needed for `tm2z` protocol (https://github.com/mapbox/tilelive-vector/issues/124)
Vector.registerProtocols(tilelive);
MBTiles.registerProtocols(tilelive);
Omnivore.registerProtocols(tilelive);
TileJSON.registerProtocols(tilelive);
Mapbox.registerProtocols(tilelive);
S3.registerProtocols(tilelive);
tl_file.registerProtocols(tilelive);

var mapnik = require('mapnik');
mapnik.Logger.setSeverity(mapnik.Logger.NONE);

mapnik.register_fonts(path.dirname(require.resolve('mapbox-studio-default-fonts')), { recurse: true });
mapnik.register_fonts(path.dirname(require.resolve('mapbox-studio-pro-fonts')), { recurse: true });
if (process.env.MapboxTileCopyFonts)
  mapnik.register_fonts(process.env.MapboxTileCopyFonts, { recurse: true });

module.exports = function(filepath, dsturi, options, callback) {

  if (s3urls.valid(dsturi)) {
    // Make sure the s3url is of type s3://bucket/key
    var query = url.parse(dsturi).query;
    dsturi = s3urls.convert(dsturi, 's3');
    if (query) dsturi += '?' + query;

    if (dsturi.indexOf('{z}') == -1 ||
        dsturi.indexOf('{x}') == -1 ||
        dsturi.indexOf('{y}') == -1) return callback(new Error('Destination URL does not include a {z}/{x}/{y} template.'));

  } else if (dsturi.indexOf('file://') == -1) {
    return callback(new Error('Invalid output protocol: ' + dsturi));
  }

  if (options.bundle === true) {
    tilelivecopy(filepath, dsturi, options, copied);
  } else {
    getUri(filepath, options.layerName, function(err, srcUri) {
      if (err) return callback(err);
      if (url.parse(srcUri).protocol === 'serialtiles:') {
        serialtilescopy(srcUri, dsturi, options, copied);
      } else {
        tilelivecopy(srcUri, dsturi, options, copied);
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
