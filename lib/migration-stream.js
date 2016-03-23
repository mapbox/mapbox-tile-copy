var stream = require('stream');
var tiletype = require('tiletype');
var mapnik = require('mapnik');

module.exports = MigrationStream;
module.exports.migrate = migrate;

function migrate(tile, callback) {
  var vtile = new mapnik.VectorTile(tile.z, tile.x, tile.y);
  vtile.setData(tile.buffer, function(err) {
    if (err) {
        err.code = 'EINVALID';
        err.message = 'Invalid data';
        return callback(err);
    }
    vtile.getData({compression:'gzip'},function(err, data) {
      if (err) return callback(err);
      tile.buffer = data;
      return callback(null, tile);
    });
  });
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

    if (info.errors) {
        var err = new Error('Invalid data');
        err.code = 'EINVALID';
        return callback(err);
    }

    var v2 = info.layers.every(function(layer) {
        return layer.version === 2;
    });
    if (v2 === true) {
      migrationStream.push(tile);
      return callback();
    }


    migrate(tile, function(err, tile) {
      if (err) return callback(err);
      migrationStream.push(tile);
      return callback();
    });
  };

  return migrationStream;
}
