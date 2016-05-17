var stream = require('stream');
var tiletype = require('tiletype');
var mapnik = require('mapnik');

module.exports = MigrationStream;
module.exports.migrate = migrate;

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

function checkForErrors(tile) {
  var info = mapnik.VectorTile.info(tile.buffer);
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

function MigrationStream() {
  var migrationStream = new stream.Transform({ objectMode: true });
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

    if (v2 === true) {
      // Check for errors on the v2 tile
      var err = checkForErrors(tile);
      if (err) return callback(err);

      migrationStream.push(tile);
      return callback();
    }


    migrate(tile, function(err, tile) {
      if (err) return callback(err);

      // Check for errors on newly-minted v2 tile
      var err = checkForErrors(tile);
      if (err) return callback(err);

      migrationStream.push(tile);
      return callback();
    });
  };

  return migrationStream;
}
