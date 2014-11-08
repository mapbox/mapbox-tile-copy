var tilelive = require('tilelive');
var Vector = require('tilelive-vector');
var MBTiles = require('mbtiles');
var Omnivore = require('tilelive-omnivore');
var TileJSON = require('tilejson');
var Mapbox = require('../lib/tilelive-mapbox');
var invalid = require('./invalid');

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

// Generate a tilelive URI for the file, maybe with mapbox-file-sniff
// then, tilelive-copy, we may want to refactor tilelive.js to expose its copy functionality
//  rather than recreating it here
// In the case of serialtiles, which must walk a different copy path
