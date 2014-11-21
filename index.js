var url = require('url');
var getUri = require('./lib/get-uri');
var copy = require('./lib/copy');

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

module.exports = function(filepath, s3urlTemplate, jobInfo, callback) {
  console.log("in init!!!");
  console.log(filepath);
  console.log(s3urlTemplate);
  console.log(jobInfo);
  // getUri(filepath, function(err, uri) {
  //   if (err) return callback(err);

  //   var copyTiles = url.parse(uri).protocol === 'serialtiles:' ?
  //     copy.serialtiles : copy.tilelive;

  //   copyTiles(srcUri, s3urlTemplate, jobInfo, callback);
  // });
};
