var stream = require('stream');
var tiletype = require('@mapbox/tiletype');
var mapnik = require('mapnik');
var fs = require('fs');
var vtvalidate = require('/Users/annmillspaugh/mapbox/vtvalidate/lib/binding/module.node');
process.env.LOG_INVALID_VT = true;
module.exports = MigrationStream;
module.exports.migrate = migrate;

function MigrationStream() {
  var migrationStream = new stream.Transform({ objectMode: true });
  var v1TileDataLogged = false;

  migrationStream._transform = function(tile, enc, callback) {
    if (!tile.buffer) {
      migrationStream.push(tile);
      return callback();
    }

    var format = tiletype.type(tile.buffer);
    if (format !== 'pbf') {
      migrationStream.push(tile);
      return callback();
    }

    var info = mapnik.VectorTile.info(tile.buffer);

    var v2 = info.layers.every(function(layer) {
        return layer.version === 2;
    });

    var newValidatorErrors; 

    if (v2 === true) {

      vtvalidate.isValid(tile.buffer, function(err, result){
        // empty string if it's a valid tile
        // if invalid tile - string that specifies why the tile is invalid
        console.log(result, 'vtvalidate result!');
        if (result){
          newValidatorErrors = result; 
        };

          // Check for errors on the v2 tile
        var data = {};
        var err = checkForErrors(info);
        console.log('check for errors', err);
        console.log('new validator', newValidatorErrors);

        if (newValidatorErrors || err){
          data['vtValidate'] = newValidatorErrors;
          data['mapnikVectorTile'] = err ? err.message : '';
          if (process.env.LOG_INVALID_VT){
            fs.writeFileSync('vt-invalid.json', JSON.stringify(data));
          };
          if (err){
            return callback(err);
          };
        };

        migrationStream.push(tile);
        return callback();
      });     
    }else{
      if (!v1TileDataLogged && process.env.LOG_V1_TILES) {
        fs.writeFileSync('v1-stats.json', JSON.stringify({ 'tileVersion': '1' }));
        v1TileDataLogged = true;
      };
    }

    migrate(tile, function(err, new_tile) {
      if (err) return callback(err);

      // Check for errors on newly-minted v2 tile
      var err = checkForErrors(mapnik.VectorTile.info(new_tile.buffer));
      if (err) return callback(err);

      migrationStream.push(new_tile);
      return callback();
    });
  };

  return migrationStream;
}

function migrate(tile, callback) {
  var vtile = new mapnik.VectorTile(tile.z, tile.x, tile.y);
  vtile.setData(tile.buffer, {upgrade:true}, function(err) {
    if (err) {
        err.code = 'EINVALID';
        return callback(err);
    }
    vtile.getData({compression:'gzip'},function(err, data) {
      if (err) return callback(err);
      tile.buffer = data;
      return callback(null, tile);
    });
  });
}

function checkForErrors(info) {
  var err = null;
  if (info.errors) {
    if (info.tile_errors) {
      err = new Error(info.tile_errors[0].replace(' message', ''));
    } else if (info.layers) {
      for (var i = 0; i < info.layers.length; i++) {
        var layer = info.layers[i];
        if (layer.errors) {
          err = new Error(layer.errors[0].replace(' message', ''));
          break;
        }
      }
    } else {
      err = new Error('Invalid data');
    }
    err.code = 'EINVALID';
  }

  return err;
}
